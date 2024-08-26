pub use lapin;

mod error;

use lapin::types::FieldTable;
use tokio::task::JoinSet;
use tower::{BoxError, Layer, Service, ServiceExt};

use std::convert::Infallible;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;

use lapin::options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions};
use lapin::protocol::basic::AMQPProperties;
use tokio_stream::StreamExt;

use self::error::{HandlerError, PublishError};

pub trait AMQPTaskResult: Sized + Send + Debug {
    type EncodeError: std::error::Error + Send + Sync + 'static;

    fn encode(self) -> Result<Vec<u8>, Self::EncodeError>;

    fn publish_exchange(&self) -> String;
    fn publish_routing_key(&self) -> String;

    fn publish(
        self,
        channel: &lapin::Channel,
    ) -> impl Future<Output = Result<(), PublishError<Self>>> + Send {
        async {
            let exchange = self.publish_exchange();
            let routing_key = self.publish_routing_key();
            let payload = self.encode().map_err(PublishError::Encode)?;
            channel
                .basic_publish(
                    &exchange,
                    &routing_key,
                    BasicPublishOptions::default(),
                    &payload,
                    AMQPProperties::default(),
                )
                .await?;
            Ok(())
        }
    }
}

impl AMQPTaskResult for () {
    type EncodeError = Infallible;

    fn encode(self) -> Result<Vec<u8>, Self::EncodeError> {
        unreachable!("empty result can't be encoded")
    }

    fn publish_exchange(&self) -> String {
        unreachable!("empty result has no exchange")
    }

    fn publish_routing_key(&self) -> String {
        unreachable!("empty result has no routing key")
    }

    async fn publish(self, _channel: &lapin::Channel) -> Result<(), PublishError<Self>> {
        Ok(())
    }
}

pub trait AMQPTask: Sized + Debug {
    type DecodeError: std::error::Error + Send + Sync + 'static;
    type TaskResult: AMQPTaskResult;

    /// Decode a task from a byte slice, this allows tasks to use different serialization formats
    fn decode(data: Vec<u8>) -> Result<Self, Self::DecodeError>;
    fn queue() -> &'static str;

    /// Debug representation of the task, override this if your task has such a huge payload that
    /// it would clog up the logs
    fn debug(&self) -> String {
        format!("{:?}", self)
    }
}

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// - If true, the worker will ack a message if it fails to decode the task, use this option if you are sure that its a publisher error.
    /// - If false, the worker will nack a message if it fails to decode the task, use this option if you might be attaching the worker to the wrong queue or your decoding logic is faulty.
    pub ack_on_decode_error: bool,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            ack_on_decode_error: true,
        }
    }
}

pub struct AMQPWorker<T, S> {
    service: S,
    inner: Arc<Inner>,
    _phantom: std::marker::PhantomData<T>,
}

struct Inner {
    channel: lapin::Channel,
    config: WorkerConfig,
    consumer_tag: String,
}

impl<T, S> Clone for AMQPWorker<T, S>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            service: self.service.clone(),
            inner: self.inner.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T, S> AMQPWorker<T, S>
where
    T: AMQPTask + Send + 'static,
    S: Service<T, Response = T::TaskResult> + Send + 'static + Clone,
    <S as Service<T>>::Future: Send,
    <S as Service<T>>::Error: Debug + Into<BoxError>,
{
    pub fn new(
        consumer_tag: impl Into<String>,
        service: S,
        channel: lapin::Channel,
        config: WorkerConfig,
    ) -> Self {
        Self {
            service,
            inner: Arc::new(Inner {
                consumer_tag: consumer_tag.into(),
                config,
                channel,
            }),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn consumer_tag(&self) -> &str {
        &self.inner.consumer_tag
    }

    pub fn channel(&self) -> &lapin::Channel {
        &self.inner.channel
    }

    pub fn config(&self) -> &WorkerConfig {
        &self.inner.config
    }

    pub fn add_layer<L>(self, layer: L) -> AMQPWorker<T, L::Service>
    where
        L: Layer<S>,
        <L as Layer<S>>::Service: Service<T, Response = T::TaskResult> + Send + 'static + Clone,
        <<L as Layer<S>>::Service as Service<T>>::Error: Debug + Into<BoxError>,
    {
        let service = layer.layer(self.service);
        AMQPWorker {
            service,
            inner: self.inner,
            _phantom: std::marker::PhantomData,
        }
    }

    #[tracing::instrument(level = "info", skip(self), fields(
        consumer = self.consumer_tag(),
        task = task.debug()
    ))]
    async fn handle_task(&mut self, task: T) -> Result<(), HandlerError<T>> {
        let task_result: T::TaskResult = self.service.call(task).await.map_err(Into::into)?;
        task_result
            .publish(&self.inner.channel)
            .await
            .map_err(|e| HandlerError::Publish(e))?;
        Ok(())
    }

    async fn ready(&mut self) -> Result<Self, BoxError> {
        let svc = self
            .service
            .clone()
            .ready_oneshot()
            .await
            .map_err(Into::into)?;
        Ok(AMQPWorker {
            service: svc,
            inner: self.inner.clone(),
            _phantom: std::marker::PhantomData,
        })
    }

    pub fn consume_spawn(
        self,
        consume_options: BasicConsumeOptions,
        consume_arguments: FieldTable,
        set: &mut JoinSet<Result<(), BoxError>>,
    ) {
        set.spawn(self.consume(consume_options, consume_arguments));
    }

    /// Start consuming tasks from the AMQP queue
    pub async fn consume(
        mut self,
        consume_options: BasicConsumeOptions,
        consume_arguments: FieldTable,
    ) -> Result<(), BoxError> {
        tracing::info!(
            config = ?self.config(),
            consumer_tag = self.consumer_tag(),
            "Starting worker consumer"
        );
        let mut consumer = self
            .inner
            .channel
            .basic_consume(
                T::queue(),
                &self.inner.consumer_tag,
                consume_options,
                consume_arguments,
            )
            .await?;
        loop {
            let mut worker = self.ready().await?;
            tracing::info!(
                consumer = worker.consumer_tag(),
                "Consumer ready, waiting for delivery"
            );
            if let Some(attempted_delivery) = consumer.next().await {
                tokio::spawn(async move {
                    let delivery = match attempted_delivery {
                        Ok(delivery) => delivery,
                        Err(e) => {
                            tracing::error!(
                                name = worker.inner.consumer_tag,
                                ?e,
                                "Delivery of message failed"
                            );
                            return Ok(());
                        }
                    };
                    let delivery_tag = delivery.delivery_tag;
                    let task = match T::decode(delivery.data) {
                        Ok(task) => task,
                        Err(e) => {
                            tracing::error!(
                                consumer = worker.inner.consumer_tag,
                                ?e,
                                ack = worker.inner.config.ack_on_decode_error,
                                "Failed to decode task"
                            );
                            if worker.config().ack_on_decode_error {
                                worker
                                    .channel()
                                    .basic_ack(delivery_tag, BasicAckOptions::default())
                                    .await?;
                            }
                            return Ok(());
                        }
                    };
                    match worker.handle_task(task).await {
                        Ok(_) => {
                            tracing::info!(
                                consumer = worker.consumer_tag(),
                                delivery_tag = delivery.delivery_tag,
                                "Delivery handled successfully"
                            );
                            worker
                                .channel()
                                .basic_ack(delivery_tag, BasicAckOptions::default())
                                .await?;
                            return Ok(());
                        }
                        Err(e) => {
                            tracing::error!(consumer = ?worker.consumer_tag(), ?e, "Task handler returned error");
                            worker
                                .channel()
                                .basic_nack(
                                    delivery_tag,
                                    BasicNackOptions {
                                        multiple: false,
                                        requeue: true,
                                    },
                                )
                                .await?;
                        }
                    }
                    Ok::<_, BoxError>(())
                });
            } else {
                return Ok(());
            }
        }
    }
}

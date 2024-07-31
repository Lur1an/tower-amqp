pub use lapin;

mod error;
mod worker;

use lapin::types::FieldTable;
use tower::{BoxError, Layer, Service, ServiceExt};

use std::convert::Infallible;
use std::fmt::Debug;
use std::future::Future;

use lapin::options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions};
use lapin::protocol::basic::AMQPProperties;
use tokio_stream::StreamExt;

use self::error::{HandlerError, PublishError};

pub trait AMQPTaskResult: Sized + Send + Debug {
    type EncodeError: std::error::Error + Send + Sync + 'static;

    fn encode(self) -> Result<Vec<u8>, Self::EncodeError>;

    fn publish_exchange() -> &'static str;
    fn publish_routing_key() -> &'static str;

    fn publish(
        self,
        channel: &lapin::Channel,
    ) -> impl Future<Output = Result<(), PublishError<Self>>> + Send {
        async {
            let payload = self.encode().map_err(PublishError::Encode)?;
            channel
                .basic_publish(
                    Self::publish_exchange(),
                    Self::publish_routing_key(),
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
        unreachable!()
    }

    fn publish_exchange() -> &'static str {
        unreachable!()
    }

    fn publish_routing_key() -> &'static str {
        unreachable!()
    }

    async fn publish(self, _channel: &lapin::Channel) -> Result<(), PublishError<Self>> {
        Ok(())
    }
}

pub trait AMQPTask: Sized + Debug {
    type DecodeError: std::error::Error + Send + Sync + 'static;
    type TaskResult: AMQPTaskResult;

    /// Decode a task from a byte slice, this allows tasks to use different serialization formats
    fn decode(bytes: &[u8]) -> Result<Self, Self::DecodeError>;
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
    consumer_tag: String,
    service: S,
    channel: lapin::Channel,
    config: WorkerConfig,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, S> Clone for AMQPWorker<T, S>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            consumer_tag: self.consumer_tag.clone(),
            service: self.service.clone(),
            channel: self.channel.clone(),
            config: self.config.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T, S> AMQPWorker<T, S>
where
    T: AMQPTask + Send + 'static,
    S: Service<T, Response = T::TaskResult> + Send + 'static + Clone,
    <S as Service<T>>::Error: Debug + Into<BoxError>,
{
    pub fn new(
        consumer_tag: impl Into<String>,
        service: S,
        channel: lapin::Channel,
        config: WorkerConfig,
    ) -> Self {
        Self {
            consumer_tag: consumer_tag.into(),
            service,
            config,
            channel,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Clone the worker with a different consumer tag
    pub fn with_consumer_tag(self, consumer_tag: impl Into<String>) -> Self {
        Self {
            consumer_tag: consumer_tag.into(),
            ..self
        }
    }

    pub fn add_layer<L>(self, layer: L) -> AMQPWorker<T, L::Service>
    where
        L: Layer<S>,
        <L as Layer<S>>::Service: Service<T, Response = T::TaskResult> + Send + 'static + Clone,
        <<L as Layer<S>>::Service as Service<T>>::Error: Debug + Into<BoxError>,
    {
        let service = layer.layer(self.service);
        AMQPWorker::new(self.consumer_tag, service, self.channel, self.config)
    }

    async fn handle_task(&mut self, task: T) -> Result<(), HandlerError<T>> {
        tracing::info!(consumer = self.consumer_tag, ?task, "Processing task");
        let task_result: T::TaskResult = self.service.call(task).await.map_err(Into::into)?;
        task_result
            .publish(&self.channel)
            .await
            .map_err(|e| HandlerError::Publish(e))?;
        Ok(())
    }

    /// Start consuming tasks from the AMQP queue
    pub async fn consume(
        mut self,
        consume_options: BasicConsumeOptions,
        consume_arguments: FieldTable,
    ) -> Result<(), lapin::Error> {
        tracing::info!(config = ?self.config, consumer_tag = self.consumer_tag, "Starting worker consumer");
        let mut consumer = self
            .channel
            .basic_consume(
                T::queue(),
                &self.consumer_tag,
                consume_options,
                consume_arguments,
            )
            .await?;
        while let Some(attempted_delivery) = consumer.next().await {
            let delivery = match attempted_delivery {
                Ok(delivery) => delivery,
                Err(e) => {
                    tracing::error!(name = self.consumer_tag, ?e, "Delivery of message failed");
                    continue;
                }
            };
            let task = match T::decode(&delivery.data) {
                Ok(task) => task,
                Err(e) => {
                    tracing::error!(
                        consumer = self.consumer_tag,
                        ?e,
                        ack = self.config.ack_on_decode_error,
                        "Failed to decode task"
                    );
                    if self.config.ack_on_decode_error {
                        delivery.ack(BasicAckOptions::default()).await?;
                    }
                    continue;
                }
            };
            match self.handle_task(task).await {
                Ok(_) => {
                    tracing::info!(
                        consumer = self.consumer_tag,
                        delivery_tag = delivery.delivery_tag,
                        "Delivery handled successfully"
                    );
                    delivery.ack(BasicAckOptions::default()).await?;
                    continue;
                }
                Err(e) => {
                    delivery
                        .nack(BasicNackOptions {
                            multiple: false,
                            requeue: true,
                        })
                        .await?;
                    match e {
                        HandlerError::Service(e) => {
                            tracing::error!(self.consumer_tag, ?e, "Task handler failed");
                        }
                        HandlerError::Publish(e) => {
                            tracing::error!(
                                self.consumer_tag,
                                ?e,
                                "Publish failed for task result"
                            );
                        }
                    }
                }
            }
        }
        unreachable!()
    }
}

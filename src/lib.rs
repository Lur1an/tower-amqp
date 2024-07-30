pub use lapin;

use lapin::types::FieldTable;
use tower::{BoxError, Layer, Service};

mod error;
mod worker;

use std::convert::Infallible;
use std::fmt::Debug;
use std::future::Future;

use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions};
use lapin::protocol::basic::AMQPProperties;
use tokio_stream::StreamExt;

use self::error::{HandlerError, PublishError};

pub trait AMQPTaskResult: Sized + Send + Debug {
    type EncodeError: std::error::Error + Send + Sync + 'static + Debug;

    /// Encode the task result into a byte slice, this allows tasks to use different serialization formats
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
}

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// - If true, the worker will ack a message if it fails to decode the task, use this option
    ///  if you are sure that its a publisher error.
    /// - If false, the worker will nack a message if it fails to decode the task, use this option
    ///  if you might be attaching the worker to the wrong queue or your decoding logic is faulty.
    pub ack_on_decode_error: bool,
    pub terminate_on_delivery_error: bool,
}

#[derive(Debug, Clone)]
pub struct AMQPWorker<T, S> {
    consumer_tag: String,
    service: S,
    channel: lapin::Channel,
    config: WorkerConfig,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, S> AMQPWorker<T, S>
where
    T: AMQPTask + Send + 'static,
    S: Service<T> + Send + 'static + Clone,
    <S as Service<T>>::Response: AMQPTaskResult,
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

    pub fn with_consumer_tag(self, consumer_tag: impl Into<String>) -> Self {
        Self {
            consumer_tag: consumer_tag.into(),
            ..self
        }
    }

    pub fn add_layer<L>(self, layer: L) -> AMQPWorker<T, L::Service>
    where
        L: Layer<S>,
        <L as Layer<S>>::Service: Service<T> + Send + 'static + Clone,
        <<L as Layer<S>>::Service as Service<T>>::Error: Debug + Into<BoxError>,
        <<L as Layer<S>>::Service as Service<T>>::Response: AMQPTaskResult,
    {
        let service = layer.layer(self.service);
        AMQPWorker::new(self.consumer_tag, service, self.channel, self.config)
    }

    /// Additional function to add an asynchronous tracing span on top for handling of a delivery
    #[tracing::instrument(
        skip(self, delivery),
        fields(
            consumer_tag = %self.consumer_tag,
            delivery_tag = delivery.delivery_tag,
            exchange = %delivery.exchange,
            routing_key = %delivery.routing_key
        )
    )]
    async fn handle_delivery(&mut self, delivery: &Delivery) -> Result<(), HandlerError<T>> {
        let task = match T::decode(&delivery.data) {
            Ok(task) => task,
            Err(e) => {
                tracing::error!(consumer = self.consumer_tag, ?e, "Failed to decode task");
                delivery.ack(BasicAckOptions::default()).await?;
                return Ok(());
            }
        };
        tracing::info!(consumer = self.consumer_tag, ?task, "Received task");
        let task_result = self.service.call(task).await.map_err(Into::into)?;
        //let task_result = match handler_result {
        //    Ok(task_result) => task_result,
        //    Err(e) => {
        //        tracing::error!(self.consumer_tag, ?e, "Task handler failed");
        //        delivery
        //            .nack(BasicNackOptions {
        //                multiple: false,
        //                requeue: true,
        //            })
        //            .await?;
        //        *failures += 1;
        //        if *failures >= self.max_failures {
        //            return Err(Error::WorkerFailure);
        //        } else {
        //            return Ok(());
        //        }
        //    }
        //};
        match task_result.publish(&self.channel).await {
            Ok(_) => {
                delivery.ack(BasicAckOptions::default()).await?;
            }
            Err(e) => {
                tracing::error!(self.consumer_tag, ?e, "Publish failed for task result");
                delivery
                    .nack(BasicNackOptions {
                        multiple: false,
                        requeue: true,
                    })
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn consume(
        mut self,
        consume_options: BasicConsumeOptions,
        consume_arguments: FieldTable,
    ) -> Result<(), lapin::Error> {
        tracing::info!(name = self.consumer_tag, "Starting worker consumer");
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
                    if self.config.terminate_on_delivery_error {
                        return Ok(());
                    }
                    continue;
                }
            };
            let task = match T::decode(&delivery.data) {
                Ok(task) => task,
                Err(e) => {
                    tracing::error!(consumer = self.consumer_tag, ?e, "Failed to decode task");
                    if self.config.ack_on_decode_error {
                        delivery.ack(BasicAckOptions::default()).await?;
                    }
                    continue;
                }
            };
            match self.handle_delivery(&delivery).await {
                Ok(_) => {
                    tracing::info!(
                        consumer = self.consumer_tag,
                        delivery_tag = delivery.delivery_tag,
                        "Delivery handled successfully"
                    );
                    continue;
                }
                Err(HandlerError::Service(e)) => todo!(),
                Err(HandlerError::Publish(e)) => todo!(),
                Err(HandlerError::Lapin(e)) => todo!(),
            }
        }
        unreachable!()
    }
}

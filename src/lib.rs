pub use lapin;
use tower::Service;

mod error;
mod worker;

use std::fmt::Debug;
use std::time::Duration;

use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicNackOptions, BasicPublishOptions};
use lapin::protocol::basic::AMQPProperties;
use tokio::time::timeout;
use tokio_stream::StreamExt;

use self::error::Error;

pub trait AMQPTaskResult {
    type EncodeError: std::error::Error + Send + Sync + 'static;

    /// Encode the task result into a byte slice, this allows tasks to use different serialization formats
    fn encode(&self) -> Result<Vec<u8>, Self::EncodeError>;

    fn publish_exchange() -> &'static str;
    fn publish_routing_key() -> &'static str;
}

pub trait AMQPTask: Sized + Debug {
    type DecodeError: std::error::Error + Send + Sync + 'static;
    type TaskResult: AMQPTaskResult;

    /// Decode a task from a byte slice, this allows tasks to use different serialization formats
    fn decode(bytes: &[u8]) -> Result<Self, Self::DecodeError>;
    fn queue() -> &'static str;
}

#[derive(Debug)]
pub struct AMQPWorker<T, H> {
    consumer_tag: String,
    task_handler: H,
    channel: lapin::Channel,
    handler_timeout: Duration,
    max_timeouts: usize,
    max_failures: usize,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, H> AMQPWorker<T, H>
where
    T: AMQPTask + Send + 'static,
    H: Service<T> + Send + 'static,
    <H as Service<T>>::Response: AMQPTaskResult,
    <H as Service<T>>::Error: std::error::Error,
{
    pub fn new(
        name: String,
        task_handler: H,
        channel: lapin::Channel,
        handler_timeout: Duration,
        max_timeouts: usize,
        max_failures: usize,
    ) -> Self {
        Self {
            consumer_tag: name,
            task_handler,
            channel,
            handler_timeout,
            max_timeouts,
            max_failures,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Additional function to add an asynchronous tracing span on top for handling of a delivery
    #[tracing::instrument(
        skip(self, delivery),
        fields(
            consumer_tag = %self.consumer_tag,
            delivery_tag = %delivery.delivery_tag,
            exchange = %delivery.exchange,
            routing_key = %delivery.routing_key
        )
    )]
    async fn handle_delivery(
        &mut self,
        delivery: &Delivery,
        timeouts: &mut usize,
        failures: &mut usize,
    ) -> Result<(), Error> {
        let task = match T::decode(&delivery.data) {
            Ok(task) => task,
            Err(e) => {
                tracing::error!(worker_name = self.consumer_tag, ?e, "Failed to decode task");
                delivery.ack(BasicAckOptions::default()).await?;
                return Ok(());
            }
        };
        tracing::info!(worker_name = self.consumer_tag, ?task, "Received task");
        let handler_fut = self.task_handler.call(task);
        let handler_result = match timeout(self.handler_timeout, handler_fut).await {
            Ok(result) => result,
            Err(e) => {
                tracing::error!(self.consumer_tag, ?e, "Task handler timed out");
                delivery
                    .nack(BasicNackOptions {
                        multiple: false,
                        requeue: true,
                    })
                    .await?;
                *timeouts += 1;
                if *timeouts >= self.max_timeouts {
                    return Err(Error::Timeout);
                }
                return Ok(());
            }
        };
        match handler_result {
            Ok(task_result) => {
                let payload = task_result.encode().map_err(|e| Error::Encode(e.into()))?;
                self.channel
                    .basic_publish(
                        T::TaskResult::publish_exchange(),
                        T::TaskResult::publish_routing_key(),
                        BasicPublishOptions::default(),
                        &payload,
                        AMQPProperties::default(),
                    )
                    .await?;
                delivery.ack(BasicAckOptions::default()).await?;
            }
            Err(e) => {
                tracing::error!(self.consumer_tag, ?e, "Task handler failed");
                delivery
                    .nack(BasicNackOptions {
                        multiple: false,
                        requeue: true,
                    })
                    .await?;
                *failures += 1;
                if *failures >= self.max_failures {
                    return Err(Error::WorkerFailure);
                }
            }
        };
        Ok(())
    }

    /// Start consuming from the embedded `Consumer`.
    /// This function cannot return Ok(()) as its supposed to run indefinitely
    /// # Errors
    /// - If the *AMQP* stream ends (it should continue indefinitely) the function will return an error
    /// - If a `nack` or a `ack` operation fails the consumer will return the Error
    /// - If the handler times out `max_timeouts` times the function will return an error
    /// - If the handler returns an error `max_failures` times the function will return an error
    /// - If encoding task result fails the function exits, this shouldn't happen.
    /// - If publishing a result fails the function exits with error.
    pub async fn consume(&mut self) -> Result<(), Error> {
        tracing::info!(name = self.consumer_tag, "Starting worker consumer");
        let mut timeouts = 0;
        let mut failures = 0;
        let consumer: lapin::Consumer = todo!();
        while let Some(attempted_delivery) = consumer.next().await {
            let delivery = match attempted_delivery {
                Ok(delivery) => delivery,
                Err(e) => {
                    tracing::error!(name = self.consumer_tag, ?e, "Delivery of message failed");
                    continue;
                }
            };
            self.handle_delivery(&delivery, &mut timeouts, &mut failures)
                .await?;
        }
        unreachable!()
    }
}

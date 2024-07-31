use crate::{AMQPTask, AMQPTaskResult};
use tower::BoxError;

/// Error that can happen when publishing the result of a task
#[derive(Debug, thiserror::Error)]
pub enum PublishError<T: AMQPTaskResult> {
    #[error(transparent)]
    Lapin(#[from] lapin::Error),

    #[error(transparent)]
    Encode(T::EncodeError),
}

/// Error that can be returned from the handler of a AMQPTask
#[derive(Debug, thiserror::Error)]
pub enum HandlerError<T: AMQPTask> {
    #[error(transparent)]
    Publish(PublishError<T::TaskResult>),

    #[error(transparent)]
    Service(#[from] BoxError),
}

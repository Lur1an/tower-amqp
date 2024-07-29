use crate::{AMQPTask, AMQPTaskResult};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Lapin(#[from] lapin::Error),
    #[error(transparent)]
    Decode(anyhow::Error),
    #[error(transparent)]
    Encode(anyhow::Error),
    #[error("Woker timed out too often")]
    Timeout,
    #[error("Worker failed too often")]
    WorkerFailure,
}

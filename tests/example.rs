use sqlx::sqlite::SqlitePool;
use std::convert::Infallible;

use lapin_tower_worker::{AMQPTask, AMQPWorker, WorkerConfig};
use tower::Service;

#[derive(Debug)]
struct MyTask;

impl AMQPTask for MyTask {
    type DecodeError = Infallible;

    type TaskResult = ();

    fn decode(_: &[u8]) -> Result<Self, Self::DecodeError> {
        Ok(MyTask)
    }

    fn queue() -> &'static str {
        "sample-queue"
    }
}

#[derive(Clone)]
struct SQLiteStuffService<F> {
    connection: SqlitePool,
    f: F,
}

impl<F, Fut> Service<MyTask> for SQLiteStuffService<F>
where
    F: Fn(SqlitePool, MyTask) -> Fut,
    Fut: std::future::Future<Output = Result<(), Infallible>>,
{
    type Response = <MyTask as AMQPTask>::TaskResult;

    type Error = Infallible;

    type Future = Fut;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: MyTask) -> Self::Future {
        (self.f)(self.connection.clone(), req)
    }
}

#[tokio::test]
async fn test_worker() {
    tracing_subscriber::fmt::init();
    let amqp =
        lapin::Connection::connect("amqp://user:password@localhost:5672", Default::default())
            .await
            .unwrap();
    let channel = amqp.create_channel().await.unwrap();
    let connection = SqlitePool::connect("sqlite::memory:").await.unwrap();
    let handler = |_: SqlitePool, _: MyTask| async move { Ok::<_, Infallible>(()) };
    let service = SQLiteStuffService {
        connection,
        f: handler,
    };
    channel
        .basic_publish(
            "",
            "sample-queue",
            Default::default(),
            &[],
            Default::default(),
        )
        .await
        .unwrap();
    let worker = AMQPWorker::new(
        "test-worker",
        service,
        channel.clone(),
        WorkerConfig::default(),
    );
}

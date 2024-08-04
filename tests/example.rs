use std::convert::Infallible;
use std::process::exit;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use std::time::Duration;

use tower::limit::ConcurrencyLimitLayer;
use tower::Service;
use tower_amqp::{AMQPTask, AMQPWorker, WorkerConfig};

#[derive(Debug)]
struct MyTask;

const QUEUE: &str = "sample-queue";

impl AMQPTask for MyTask {
    type DecodeError = Infallible;

    type TaskResult = ();

    fn decode(_: &[u8]) -> Result<Self, Self::DecodeError> {
        Ok(MyTask)
    }

    fn queue() -> &'static str {
        QUEUE
    }
}

#[derive(Clone)]
struct CounterService<F> {
    counter: Arc<AtomicUsize>,
    f: F,
}

impl<F, Fut> Service<MyTask> for CounterService<F>
where
    F: Fn(Arc<AtomicUsize>, MyTask) -> Fut,
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
        (self.f)(self.counter.clone(), req)
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
    channel
        .queue_declare(QUEUE, Default::default(), Default::default())
        .await
        .unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let handler = |counter: Arc<AtomicUsize>, _: MyTask| async move {
        tracing::info!(?counter, "Incrementing counter");
        counter.fetch_add(1, SeqCst);
        tokio::time::sleep(Duration::from_secs(1)).await;
        if counter.load(SeqCst) > 8 {
            tracing::error!(?counter, "Counter exceeded limit");
            exit(1);
        }
        counter.fetch_sub(1, SeqCst);
        Ok::<_, Infallible>(())
    };
    let service = CounterService {
        counter: counter.clone(),
        f: handler,
    };
    let worker = AMQPWorker::new(
        "test-worker",
        service,
        channel.clone(),
        WorkerConfig::default(),
    )
    .add_layer(ConcurrencyLimitLayer::new(8));
    let mut set = tokio::task::JoinSet::new();
    worker.consume_spawn(Default::default(), Default::default(), &mut set);
    for _ in 0..20 {
        channel
            .basic_publish("", QUEUE, Default::default(), &[], Default::default())
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_secs(8)).await;
    assert_eq!(counter.load(SeqCst), 0);
}

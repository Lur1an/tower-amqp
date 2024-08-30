# lapin-tower-worker
> When you need a standardized way to build workers for AMQP tasks & data

## Usage

The library works on a `task` and `result` model, once those are defined the worker can automate the process of pulling tasks,
executing your logic and publishing results.

To standardize the way application logic is integrated we use [tower](https://github.com/tower-rs/tower) and [tokio](https://github.com/tokio-rs/tokio) as the main building blocks.
The idea for this library came to me as I ended up writing retry & timeout logic & concurrency limits for workers and noticed that I was just re-implementing worse and less reusable tower middleware.
### Defining a task
```rust
#[derive(Debug)]
struct MyAwesomeTask {
    value: String
}

impl AMQPTask for MyTask {
    type DecodeError = FromUtf8Error;

    type TaskResult = MyAwesomeResult;

    fn decode(data: Vec<u8>) -> Result<Self, Self::DecodeError> {
        String::from_utf8(data).map(|value| MyTask { value })
    }

    fn queue() -> &'static str {
        "awesome-tasks"
    }
}
```
To define a task you need to define how its decoded from a `Vec<u8>` and which queue its pulled from.

Also don't forget to add `Debug` to your task or override the `debug` function of the trait, these
are used to create readable tracing spans to track the execution of your tasks.

### Defining a result
```rust
#[derive(Debug)]
struct MyAwesomeResult {
    value: String
}

impl AMQPTaskResult for MyAwesomeResult {
    type EncodeError = Infallible;

    fn encode(self) -> Result<Vec<u8>, Self::EncodeError> {
        self.value.into_bytes()
    }

    fn publish_exchange(&self) -> String {
        "" // direct exchange
    }

    fn publish_routing_key(&self) -> String {
        "awesome-results" // through direct exchange this will end up in the queue "awesome-results"
    }
}
```
#### My tasks don't produce results...
`AmqpTaskResult` is implemented for `()`, and the `publish` implementation is a no-op. If 
you specify `()` as your `TaskResult` nothing will be published.

### Creating the worker
Put your logic into a `tower::Service`, please refer to the [tower documentation](https://docs.rs/tower/latest/tower/) for more information.
Lets use our good ol friend `Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>` to define our service future type and throw in `anyhow::Error` to keep errors simple.

Remember, your `Service` needs to be `Clone`able, this allows the worker to run tasks concurrently on your service.

`poll_ready` is used in `tower` to determine if the service is ready to handle requests, I won't
go into the details of storing futures in your service to poll on every invocation. If you wish for
your service to for some reason stop accepting tasks and terminate you can return your `Error` type instead.

I recognize tower is not easy to use, however it is a good and standardized way to apply useful middleware.
```rust
#[derive(Clone)]
struct AwesomeService {
    db_conn: DbConnection, // This thing is almost always cloneable
}

impl Service<MyAwesomeTask> for AwesomeService {
    type Response = MyAwesomeResult;
    type Error = anyhow::Error;

    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, task: MyAwesomeTask) -> Self::Future {
        let db_conn = self.db_conn.clone();
        Box::pin(async move {
            let stuff = do_awesome_stuff(&db_conn, task.value).await?;
            Ok(MyAwesomeResult { value: stuff })
        })
    }
```
Lets put everything together into the *worker* and run it.

Get a lapin connection and create a channel, thats most of what you will be doing with the crate.
```rust
let amqp =
    lapin::Connection::connect("amqp://user:password@localhost:5672", Default::default())
        .await
        .unwrap();
let channel = amqp.create_channel().await.unwrap();
```
Initialize the service and hand it over to the worker, then spawn the worker onto a `tokio::task::JoinSet`.
The worker needs a name to use for consumer tag and tracing. You can set `WorkerConfig` to alter some behaviour,
like `ack`ing the message on a decode error or not, as I don't want to take this decision away.

```rust
let service = AwesomeService {
    db_conn: get_db_conn().await.unwrap(),
};
let worker = AMQPWorker::new(
    "awesome-worker",
    service,
    channel.clone(),
    WorkerConfig::default(),
);
```
Lets add a `ConcurrencyLimitLayer` to our worker, this will limit the number of concurrent tasks that can be spawned,
otherwise the worker will pull as many tasks as are in the queue and just run them concurrently, this might be undesirable in a distributed architecture. `add_layer` returns a new worker instance everytime.
```rust
let worker = worker.add_layer(ConcurrencyLimitLayer::new(8));
```
Worker can't take too long, lets add a timeout
```rust
let worker = worker.add_layer(TimeoutLayer::new(Duration::from_secs(60*5))); // 5 minutes
```
Now we can spawn the worker onto a joinset.
```rust
let mut set = tokio::task::JoinSet::new();
let consume_options = BasicConsumeOptions::default();
let consume_arguments = FieldTable::default();
worker.consume_spawn(consume_options, consume_arguments, &mut set);
```


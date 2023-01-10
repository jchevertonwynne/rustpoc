use anyhow::Context;
use async_trait::async_trait;
use mongodb::bson::oid::ObjectId;
use rustpoc::rabbit::{Rabbit, RabbitConsumer};
use serde::Deserialize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::Level;
use Ordering::SeqCst;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .init();

    tracing::info!("hello!");

    let rabbit = Rabbit::new("amqp://localhost:5672")
        .await
        .context("failed to create rabbit conn")?;

    tracing::info!("connected to rabbit");

    rabbit.declare_exchange("jexchange").await?;
    rabbit.declare_queue("jqueue").await?;
    rabbit.bind_queue("jqueue", "jexchange").await?;

    tracing::info!("setup queue and exchange");

    let global_count = Arc::new(AtomicUsize::default());

    let delegator = (
        Arc::new(MyConsumer {
            count: AtomicUsize::default(),
            global_count: global_count.clone(),
        }),
        Arc::new(MyOtherConsumer {
            count: AtomicUsize::default(),
            global_count,
        }),
    );

    rabbit
        .consume("jqueue", delegator)
        .await
        .context("failed to start consumer")?;

    tracing::info!("ready to consume!");

    tokio::signal::ctrl_c()
        .await
        .context("ctrl c wait failed")?;

    tracing::info!("shutting down");

    rabbit.close().await?;

    tracing::info!("closed rabbit");

    Ok(())
}

struct MyConsumer {
    count: AtomicUsize,
    global_count: Arc<AtomicUsize>,
}

#[derive(Deserialize, Debug)]
struct MyConsumerMessage<'a> {
    name: &'a str,
    arms: usize,
}

// #[derive(Deserialize, Debug)]
// struct MyConsumerMessage {
//     name: String,
//     arms: usize,
// }

#[derive(thiserror::Error, Debug)]
enum MyConsumerError {
    #[error("failed to deserialize: {0}")]
    SerdeError(#[from] serde_json::Error),
}

#[async_trait]
impl RabbitConsumer for MyConsumer {
    type Message<'a> = MyConsumerMessage<'a>;
    // type Message<'a> = MyConsumerMessage;
    type ConsumerError = MyConsumerError;

    fn header_matches(&self, header: &str) -> bool {
        header == "header1"
    }

    #[tracing::instrument(
        name = "handling a received rabbit msg",
        skip(self, msg, _raw),
        fields(name=msg.name, arms=msg.arms, header="header1")
    )]
    async fn process(
        self: Arc<Self>,
        msg: Self::Message<'_>,
        _raw: &[u8],
    ) -> Result<(), Self::ConsumerError> {
        let global_count = self.global_count.fetch_add(1, SeqCst) + 1;
        let local_count = self.count.fetch_add(1, SeqCst) + 1;
        tracing::info!("my consumer: global count = {global_count}, local_count = {local_count}, msg = {msg:?}");
        if msg.name == "joseph" {
            tracing::info!("hello joseph! it's been a while")
        } else {
            tracing::info!("hello... {}? who are you?", msg.name);
        }
        Ok(())
    }
}

struct MyOtherConsumer {
    count: AtomicUsize,
    global_count: Arc<AtomicUsize>,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct MyOtherConsumerMessage {
    id: ObjectId,
    status: Status,
}

#[derive(Deserialize, Debug)]
enum Status {
    #[serde(rename = "yoloswag")]
    Active,
    Inactive,
}

#[derive(thiserror::Error, Debug)]
enum MyOtherConsumerError {
    #[error("failed to deserialize: {0}")]
    SerdeError(#[from] serde_json::Error),
}

#[async_trait]
impl RabbitConsumer for MyOtherConsumer {
    type Message<'a> = MyOtherConsumerMessage;
    type ConsumerError = MyOtherConsumerError;

    fn header_matches(&self, header: &str) -> bool {
        header == "header2"
    }

    #[tracing::instrument(
        name = "handling an other received rabbit msg",
        skip(self, msg, _raw),
        fields(header = "header2")
    )]
    async fn process(
        self: Arc<Self>,
        msg: Self::Message<'_>,
        _raw: &[u8],
    ) -> Result<(), Self::ConsumerError> {
        let global_count = self.global_count.fetch_add(1, SeqCst) + 1;
        let local_count = self.count.fetch_add(1, SeqCst) + 1;
        some_call();
        let span = tracing::span!(Level::INFO, "other msg count", local_count);
        let _enter = span.enter();
        some_call();
        tracing::info!("other consumer: global count = {global_count}, local_count = {local_count}, msg = {msg:?}");
        drop(_enter);
        some_call();
        Ok(())
    }
}

fn some_call() {
    tracing::info!("this is a call!");
}

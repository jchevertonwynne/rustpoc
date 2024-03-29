use anyhow::Context;
use async_trait::async_trait;
use mongodb::bson::oid::ObjectId;
use rustpoc::rabbit::{Rabbit, RabbitConsumer};
use serde::Deserialize;
use std::borrow::Cow;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Registry::default()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("INFO")))
        .with(Layer::new().with_line_number(true).with_file(true))
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
        MyConsumer {
            count: AtomicUsize::default(),
            global_count: global_count.clone(),
        },
        MyOtherConsumer {
            count: AtomicUsize::default(),
            global_count,
        },
    );

    let killer = CancellationToken::new();

    let consume_handle = rabbit
        .consume("jqueue", delegator, killer.clone())
        .await
        .context("failed to start consumer")?;

    tracing::info!("ready to consume!");

    tokio::signal::ctrl_c()
        .await
        .context("ctrl c wait failed")?;

    tracing::info!("shutting down");

    killer.cancel();
    rabbit.close().await?;

    consume_handle.await?;

    tracing::info!("closed rabbit");

    Ok(())
}

struct MyConsumer {
    count: AtomicUsize,
    global_count: Arc<AtomicUsize>,
}

#[derive(Deserialize, Debug)]
struct MyConsumerMessage<'a> {
    name: Cow<'a, str>,
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
        fields(name=msg.name.as_ref(), arms=msg.arms, id=ObjectId::new().to_hex())
    )]
    async fn process(
        &self,
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
        fields(id=ObjectId::new().to_hex())
    )]
    async fn process(
        &self,
        msg: Self::Message<'_>,
        _raw: &[u8],
    ) -> Result<(), Self::ConsumerError> {
        let global_count = self.global_count.fetch_add(1, SeqCst) + 1;
        let local_count = self.count.fetch_add(1, SeqCst) + 1;
        some_call();
        let span = tracing::span!(tracing::Level::INFO, "other msg count", local_count);
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

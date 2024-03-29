use anyhow::Context;
use mongodb::bson::oid::ObjectId;
use rustpoc::rabbit::Rabbit;
use serde::Serialize;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Registry::default()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("INFO")))
        .init();

    tracing::info!("connecting to rabbit...");

    let rabbit = Rabbit::new("amqp://localhost:5672")
        .await
        .context("failed to create rabbit conn")?;

    tracing::info!("connected, sending messages");

    for i in 0..5 {
        let name = if i % 2 == 0 { "joseph" } else { "ben" };

        tokio::try_join!(
            rabbit.publish_json("jexchange", "header1", MessageToSend1 { name, arms: i % 3 },),
            // rabbit.publish_json(
            //     "jexchange",
            //     "header2",
            //     MessageToSend2 {
            //         id: ObjectId::new(),
            //         status: Status::Active,
            //     },
            // ),
            // rabbit.publish_json(
            //     "jexchange",
            //     "header2",
            //     MessageToSend2 {
            //         id: ObjectId::new(),
            //         status: Status::Inactive,
            //     },
            // )
        )?;

        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    rabbit.close().await?;

    tracing::info!("done!");

    Ok(())
}

#[derive(Serialize)]
struct MessageToSend1<'a> {
    name: &'a str,
    arms: usize,
}

#[derive(Serialize)]
struct MessageToSend2 {
    id: ObjectId,
    status: Status,
}

#[allow(dead_code)]
#[derive(Serialize)]
enum Status {
    #[serde(rename = "yoloswag")]
    Active,
    Inactive,
}

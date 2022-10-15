use std::fmt::Debug;
use std::str::Utf8Error;

use futures_lite::StreamExt;
use lapin::{
    message::Delivery,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions,
        QueueBindOptions, QueueDeclareOptions,
    },
    publisher_confirm::Confirmation,
    types::{AMQPValue, FieldTable},
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
};
use serde::{de::DeserializeOwned, Serialize};

const QUEUE: &str = "queue-joseph";
const EXCHANGE: &str = "exchange-joseph";
const ROUTING: &str = "";
const CONSUMER_TAG: &str = "joseph-consumer";
pub const MESSAGE_TYPE: &str = "msg-joseph";

pub struct Rabbit {
    #[allow(dead_code)]
    conn: Connection,
    chan: Channel,
    field_table: FieldTable,
}

impl Rabbit {
    pub async fn new(address: &str) -> lapin::Result<Rabbit> {
        let conn = Connection::connect(address, ConnectionProperties::default()).await?;

        let field_table = {
            let mut ft = FieldTable::default();
            ft.insert(
                "message_type".into(),
                AMQPValue::LongString(MESSAGE_TYPE.into()),
            );
            ft.insert("x-match".into(), AMQPValue::LongString("all".into()));
            ft
        };

        let chan = conn.create_channel().await?;

        Ok(Rabbit {
            conn,
            chan,
            field_table,
        })
    }

    pub async fn setup(&self) -> lapin::Result<()> {
        self.chan
            .exchange_declare(
                EXCHANGE,
                ExchangeKind::Headers,
                ExchangeDeclareOptions {
                    passive: false,
                    durable: true,
                    auto_delete: false,
                    internal: false,
                    nowait: false,
                },
                self.field_table.clone(),
            )
            .await?;

        self.chan
            .queue_declare(
                QUEUE,
                QueueDeclareOptions {
                    passive: false,
                    durable: true,
                    auto_delete: false,
                    exclusive: false,
                    nowait: false,
                },
                self.field_table.clone(),
            )
            .await?;

        self.chan
            .queue_bind(
                QUEUE,
                EXCHANGE,
                ROUTING,
                QueueBindOptions { nowait: false },
                self.field_table.clone(),
            )
            .await?;

        Ok(())
    }

    pub async fn publish_json(&self, body: impl Serialize) -> Result<Confirmation, PublishError> {
        let body = serde_json::to_string(&body)?;
        tracing::info!("about to publish {}", body);
        let mut ft = self.field_table.clone();
        ft.insert(
            "content-type".into(),
            AMQPValue::LongString("application/json".into()),
        );
        Ok(self
            .chan
            .basic_publish(
                EXCHANGE,
                ROUTING,
                BasicPublishOptions::default(),
                body.as_bytes(),
                BasicProperties::default().with_headers(ft),
            )
            .await?
            .await?)
    }

    pub async fn consume<T>(
        &self,
        message_type_header: &'static str,
        mut receiver: Receiver<()>,
    ) -> Result<JoinHandle<()>, lapin::Error>
    where
        T: DeserializeOwned + Debug,
    {
        let mut consumer = self
            .chan
            .basic_consume(
                QUEUE,
                CONSUMER_TAG,
                BasicConsumeOptions::default(),
                self.field_table.clone(),
            )
            .await?;

        Ok(tokio::spawn(async move {
            tracing::info!("started consumer of message {}", message_type_header);
            loop {
                let received = tokio::select! {
                    received = consumer.next() => {
                        match received {
                            Some(received) => received,
                            None => {
                                tracing::error!("no value received");
                                continue;
                            }
                        }
                    },
                    _ = receiver.recv() => {
                        return;
                    }
                };

                let delivery = match received {
                    Ok(delivery) => delivery,
                    Err(err) => {
                        tracing::error!("failed to get message delivery: {:?}", err);
                        continue;
                    }
                };

                if let Err(err) = process_delivery::<T>(&delivery, message_type_header).await {
                    tracing::error!("failed to process rabbit message: {:?}", err);
                }

                if let Err(err) = delivery.ack(BasicAckOptions::default()).await {
                    tracing::error!("failed to ack: {:?}", err);
                }
            }
        }))
    }
}

use thiserror::Error;
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinHandle;

#[derive(Error, Debug)]
pub enum PublishError {
    #[error("failed to serialize struct: {0}")]
    SerializeError(#[from] serde_json::error::Error),
    #[error("rabbit operation failed: {0}")]
    RabbitError(#[from] lapin::Error),
}
#[derive(Error, Debug)]
pub enum ConsumeError {
    #[error("failed to serialize struct: {0}")]
    SerializeError(#[from] serde_json::error::Error),
    #[error("rabbit operation failed: {0}")]
    RabbitError(#[from] lapin::Error),
}

async fn process_delivery<T: DeserializeOwned + Debug>(
    delivery: &Delivery,
    header: &str,
) -> Result<(), ProcessError> {
    let message_type_header = std::str::from_utf8(
        delivery
            .properties
            .headers()
            .as_ref()
            .ok_or(ProcessError::NoHeaders)?
            .inner()
            .get("message_type")
            .ok_or(ProcessError::NoMessageTypeHeader)?
            .as_long_string()
            .ok_or(ProcessError::NonLongString)?
            .as_bytes(),
    )?;

    if message_type_header != header {
        tracing::error!(
            "headers did not match: {} != {}",
            message_type_header,
            header
        );
        return Ok(());
    }

    let received: T = serde_json::from_slice(&delivery.data)?;

    tracing::info!("received rabbit msg: {:?}", received);

    Ok(())
}

#[derive(Error, Debug)]
pub enum ProcessError {
    #[error("failed to parse header string: {0}")]
    HeaderParseError(#[from] Utf8Error),
    #[error("failed to deserialize message: {0}")]
    DeserializeError(#[from] serde_json::Error),
    #[error("no headers on the rabbit message")]
    NoHeaders,
    #[error("message-type header was missing")]
    NoMessageTypeHeader,
    #[error("message-type header was the wrong type")]
    NonLongString,
}

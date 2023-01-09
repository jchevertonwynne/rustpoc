use Ordering::SeqCst;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::str::Utf8Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use lapin::message::DeliveryResult;
use lapin::options::BasicAckOptions;
use lapin::protocol::constants::REPLY_SUCCESS;
use lapin::types::AMQPValue::LongString;
use lapin::{
    options::{
        BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions, QueueBindOptions,
        QueueDeclareOptions,
    },
    publisher_confirm::Confirmation,
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
};
use mongodb::bson::oid::ObjectId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const QUEUE: &str = "queue-joseph";
pub const EXCHANGE: &str = "exchange-joseph";
const ROUTING: &str = "";
const CONSUMER_TAG: &str = "joseph-consumer";
pub const MESSAGE_TYPE: &str = "msg-joseph";

pub struct Rabbit {
    conn: Connection,
    chan: Channel,
    headers: FieldTable,
}

impl Rabbit {
    pub async fn new(address: &str) -> lapin::Result<Rabbit> {
        let connection_properties = ConnectionProperties::default()
            .with_executor(tokio_executor_trait::Tokio::current())
            .with_reactor(tokio_reactor_trait::Tokio);
        let conn = Connection::connect(address, connection_properties).await?;

        let field_table = {
            let mut ft = FieldTable::default();
            ft.insert("x-match".into(), LongString("all".into()));
            ft
        };

        let chan = conn.create_channel().await?;

        Ok(Rabbit {
            conn,
            chan,
            headers: field_table,
        })
    }

    pub async fn declare_exchange(&self, exchange: &str) -> Result<(), lapin::Error> {
        self.chan
            .exchange_declare(
                exchange,
                ExchangeKind::Headers,
                ExchangeDeclareOptions {
                    passive: false,
                    durable: true,
                    auto_delete: false,
                    internal: false,
                    nowait: false,
                },
                self.headers.clone(),
            )
            .await
    }

    pub async fn declare_queue(&self, queue: &str) -> Result<(), lapin::Error> {
        self.chan
            .queue_declare(
                queue,
                QueueDeclareOptions {
                    passive: false,
                    durable: true,
                    auto_delete: false,
                    exclusive: false,
                    nowait: false,
                },
                self.headers.clone(),
            )
            .await
            .map(|_| ())
    }

    pub async fn bind_queue(&self, queue: &str, exchange: &str) -> Result<(), lapin::Error> {
        self.chan
            .queue_bind(
                queue,
                exchange,
                ROUTING,
                QueueBindOptions { nowait: false },
                self.headers.clone(),
            )
            .await
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
                self.headers.clone(),
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
                self.headers.clone(),
            )
            .await?;

        self.chan
            .queue_bind(
                QUEUE,
                EXCHANGE,
                ROUTING,
                QueueBindOptions { nowait: false },
                self.headers.clone(),
            )
            .await?;

        Ok(())
    }

    pub async fn close(&self) -> Result<(), lapin::Error> {
        self.chan.close(REPLY_SUCCESS, "thank you!").await?;
        self.conn.close(REPLY_SUCCESS, "thank you!").await
    }

    pub async fn publish_json(
        &self,
        exchange: &str,
        message_type: &str,
        body: impl Serialize,
    ) -> Result<Confirmation, PublishError> {
        let body = serde_json::to_string(&body)?;
        let mut headers = self.headers.clone();
        headers.insert("content-type".into(), LongString("application/json".into()));
        headers.insert("message_type".into(), LongString(message_type.into()));
        let resp = self
            .chan
            .basic_publish(
                exchange,
                ROUTING,
                BasicPublishOptions::default(),
                body.as_bytes(),
                BasicProperties::default().with_headers(headers),
            )
            .await?
            .await?;
        Ok(resp)
    }

    pub async fn consume<D>(&self, queue: &str, rabbit_delegator: D) -> Result<(), lapin::Error>
    where
        D: RabbitDelegator,
    {
        let consumer = self
            .chan
            .basic_consume(
                queue,
                CONSUMER_TAG,
                BasicConsumeOptions::default(),
                self.headers.clone(),
            )
            .await?;

        consumer.set_delegate(Delegator {
            delegator: rabbit_delegator,
            channel: self.chan.clone(),
        });

        Ok(())
    }
}

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

#[derive(Default)]
pub struct BodyConsumer {
    count: AtomicUsize,
}

#[derive(Error, Debug)]
pub enum BodyConsumerError {
    #[error("failed to parse message body: {0}")]
    SerdeError(#[from] serde_json::Error),
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
pub struct MsgBody<'a> {
    #[serde(rename = "gamesConsoleConnectorId")]
    connector_id: ObjectId,
    #[serde(rename = "type")]
    connector_type: &'a str,
}

#[async_trait]
impl RabbitConsumer for BodyConsumer {
    type Message<'a> = MsgBody<'a>;
    type ConsumerError = BodyConsumerError;

    fn header_matches(&self, header: &str) -> bool {
        header == "msg-joseph"
    }

    async fn process(
        self: Arc<Self>,
        msg: Self::Message<'_>,
        _raw: &[u8],
    ) -> Result<(), Self::ConsumerError> {
        let count = self.count.fetch_add(1, SeqCst) + 1;
        tracing::info!("processing message {:?}, total: {}", msg, count);
        Ok(())
    }
}

#[async_trait]
pub trait RabbitConsumer: Sync + Send + 'static {
    type Message<'a>: Deserialize<'a> + Send;
    type ConsumerError: std::error::Error + From<serde_json::Error>;

    fn header_matches(&self, header: &str) -> bool;

    fn parse_msg<'a>(&self, contents: &'a [u8]) -> Result<Self::Message<'a>, serde_json::Error> {
        serde_json::from_slice(contents)
    }

    async fn process(
        self: Arc<Self>,
        msg: Self::Message<'_>,
        raw: &[u8],
    ) -> Result<(), Self::ConsumerError>;

    async fn try_process(self: Arc<Self>, contents: Vec<u8>) {
        if let Err(err) = self.try_consume_inner(contents).await {
            tracing::error!("failed to process message: {}", err);
        }
    }

    async fn try_consume_inner(
        self: Arc<Self>,
        contents: Vec<u8>,
    ) -> Result<(), Self::ConsumerError> {
        let message = self.parse_msg(&contents)?;
        self.process(message, &contents).await
    }
}

#[async_trait]
pub trait RabbitDelegator: Clone + Send + Sync + 'static {
    fn delegate(&self, header: &str, contents: Vec<u8>) -> bool;
}

macro_rules! header_matcher {
    ($CONSUMER: expr, $HEADER: expr, $CONTENTS: expr) => {
        if $CONSUMER.header_matches($HEADER) {
            let consumer = Arc::clone($CONSUMER);
            tokio::spawn(consumer.try_process($CONTENTS));
            return true;
        }
    };
}

#[async_trait]
impl<T> RabbitDelegator for Arc<T>
where
    T: RabbitConsumer,
{
    fn delegate(&self, header: &str, contents: Vec<u8>) -> bool {
        header_matcher!(self, header, contents);
        false
    }
}

#[async_trait]
impl<A, B> RabbitDelegator for (Arc<A>, Arc<B>)
where
    A: RabbitConsumer,
    B: RabbitConsumer,
{
    fn delegate(&self, header: &str, contents: Vec<u8>) -> bool {
        tracing::info!("attempting to delegate: header = {}", header);
        let (a, b) = self;
        header_matcher!(a, header, contents);
        header_matcher!(b, header, contents);
        false
    }
}

struct Delegator<D> {
    delegator: D,
    channel: Channel,
}

impl<D> lapin::ConsumerDelegate for Delegator<D>
where
    D: RabbitDelegator,
{
    fn on_new_delivery(
        &self,
        delivery: DeliveryResult,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let delegator = self.delegator.clone();
        let channel = self.channel.clone();
        Box::pin(async move {
            let delivery = match delivery {
                Ok(Some(delivery)) => delivery,
                Ok(None) => return,
                Err(err) => {
                    tracing::error!("error on delivery?: {}", err);
                    return;
                }
            };

            let header = delivery
                .properties
                .headers()
                .as_ref()
                .unwrap()
                .inner()
                .get("message_type")
                .expect("messages should always have message_type header")
                .as_long_string()
                .expect("message_type header should be a long string")
                .to_string();

            if !delegator.delegate(&header, delivery.data) {
                tracing::error!("failed to delegate message with header {}", header);
            }

            if let Err(err) = channel
                .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                .await
            {
                tracing::error!("failed to ack delivery: {}", err);
            }
        })
    }
}

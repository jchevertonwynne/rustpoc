use std::fmt::Debug;
use std::str::Utf8Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::server::Body;
use async_trait::async_trait;
use futures_util::StreamExt;
use lapin::{options::{
    BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions, QueueBindOptions,
    QueueDeclareOptions,
}, publisher_confirm::Confirmation, types::{AMQPValue, FieldTable}, BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind, Consumer};
use lapin::options::BasicAckOptions;
use lapin::protocol::constants::REPLY_SUCCESS;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const QUEUE: &str = "queue-joseph";
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

    pub async fn close(&self) -> Result<(), lapin::Error> {
        self.conn.close(REPLY_SUCCESS, "thank you!").await
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

    pub async fn consume<D>(&self, queue: &str, rabbit_delegator: D) -> Result<(), lapin::Error>
        where
            D: RabbitDelegator,
    {
        let chan = self.chan.clone();
        let consumer = chan
            .basic_consume(
                queue,
                CONSUMER_TAG,
                BasicConsumeOptions::default(),
                self.field_table.clone(),
            )
            .await?;

        tokio::spawn(run_delegator(consumer, chan, rabbit_delegator));

        Ok(())
    }
}

async fn run_delegator<D: RabbitDelegator>(mut consumer: Consumer, chan: Channel, delegator: D) {
    loop {
        match consumer.next().await {
            None => {}
            Some(Ok(delivery)) => {
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
                    eprintln!("failed to delegate message with header {}", header);
                }

                if let Err(err) = chan.basic_ack(delivery.delivery_tag, BasicAckOptions::default()).await {
                    eprint!("failed to ack delivery: {}", err);
                }
            }
            Some(Err(err)) => {
                eprintln!("error polling consumer: {}", err);
            }
        }
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

#[async_trait]
impl RabbitConsumer for BodyConsumer {
    type Message<'a> = Body;
    type ConsumerError = BodyConsumerError;

    fn header_matches(&self, header: &str) -> bool {
        header == "msg-joseph"
    }

    async fn process(self: Arc<Self>, msg: Self::Message<'_>, _raw: &[u8]) -> Result<(), Self::ConsumerError> {
        let count = self.count.fetch_add(1, Ordering::SeqCst) + 1;
        println!("processing message {:?}, total: {}", msg, count);
        Ok(())
    }
}

impl From<BodyConsumerError> for DelegatorError {
    fn from(_: BodyConsumerError) -> Self {
        DelegatorError
    }
}

#[async_trait]
pub trait RabbitConsumer: Sync + Send + 'static {
    type Message<'a>: Deserialize<'a> + Send;
    type ConsumerError: Into<DelegatorError> + From<serde_json::Error> + std::error::Error;

    fn header_matches(&self, header: &str) -> bool;

    fn parse_msg<'a>(&self, contents: &'a [u8]) -> Result<Self::Message<'a>, Self::ConsumerError> {
        serde_json::from_slice(contents).map_err(|err| err.into())
    }

    async fn process(self: Arc<Self>, msg: Self::Message<'_>, raw: &[u8]) -> Result<(), Self::ConsumerError>;

    async fn try_process(self: Arc<Self>, contents: Vec<u8>) {
        if let Err(err) = self.try_consume_inner(contents).await {
            eprint!("err: {}", err);
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
pub trait RabbitDelegator: Sync + Send + Clone + 'static {
    fn delegate(&self, header: &str, contents: Vec<u8>) -> bool;
}

pub struct DelegatorError;

macro_rules! header_matcher {
    ($CONSUMER: expr, $HEADER: expr, $CONTENTS: expr) => {
        if $CONSUMER.header_matches($HEADER) {
            tokio::spawn($CONSUMER.try_process($CONTENTS));
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
        let _self = Arc::clone(self);
        header_matcher!(_self, header, contents);
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
        let (a, b) = self;
        let a = Arc::clone(a);
        header_matcher!(a, header, contents);
        let b = Arc::clone(b);
        header_matcher!(b, header, contents);
        false
    }
}

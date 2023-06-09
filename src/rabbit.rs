use async_channel::{Receiver, Sender};
use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use Ordering::SeqCst;

use async_trait::async_trait;
use futures_util::StreamExt;
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
    BasicProperties, Channel, Connection, ConnectionProperties, Consumer, ExchangeKind,
};
use mongodb::bson::oid::ObjectId;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

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
        let connection_properties = ConnectionProperties::default();
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

    pub async fn publish_json<S: Serialize>(
        &self,
        exchange: &str,
        message_type: &str,
        body: S,
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

    pub async fn consume<D: RabbitDelegator>(
        &self,
        queue: &str,
        rabbit_delegator: D,
        kill_signal: CancellationToken,
    ) -> Result<JoinHandle<()>, lapin::Error> {
        let consumer = self
            .chan
            .basic_consume(
                queue,
                CONSUMER_TAG,
                BasicConsumeOptions::default(),
                self.headers.clone(),
            )
            .await?;

        Ok(tokio::spawn(run_consumer(
            rabbit_delegator,
            consumer,
            self.chan.clone(),
            kill_signal,
        )))
    }
}

struct Payload {
    delivery_tag: u64,
    header: String,
    contents: Vec<u8>,
}

async fn run_consumer<D: RabbitDelegator>(
    delegator: D,
    mut consumer: Consumer,
    channel: Channel,
    kill_signal: CancellationToken,
) {
    let (sender, receiver): (Sender<Payload>, Receiver<_>) = async_channel::unbounded();
    let killer = CancellationToken::new();

    let mut handles = Vec::new();

    let delegator = Arc::new(delegator);
    for _ in 0..10 {
        let channel = channel.clone();
        let delegator = Arc::clone(&delegator);
        let receiver = receiver.clone();
        let kill_signal = killer.clone();
        let handle = tokio::spawn(worker(channel, receiver, delegator, kill_signal));
        handles.push(handle);
    }

    loop {
        let delivery = tokio::select! {
            _ = kill_signal.cancelled() => break,
            d = consumer.next() => d,
        };
        let delivery = match delivery {
            Some(Ok(delivery)) => delivery,
            Some(Err(err)) => {
                tracing::error!("error on delivery?: {}", err);
                continue;
            }
            None => return,
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

        let delivery_tag = delivery.delivery_tag;
        let contents = delivery.data;

        sender
            .send(Payload {
                delivery_tag,
                header,
                contents,
            })
            .await
            .expect("no receiver was able to take the delivery");
    }

    killer.cancel();

    for handle in handles {
        handle
            .await
            .expect("delegator worker thread handle could not be awaited");
    }
}

async fn worker<D: RabbitDelegator>(
    channel: Channel,
    receiver: Receiver<Payload>,
    delegator: Arc<D>,
    kill_signal: CancellationToken,
) {
    let mut kill_signal = std::pin::pin!(kill_signal.cancelled());
    loop {
        let Payload {
            delivery_tag,
            header,
            contents,
        } = tokio::select! {
            _ = kill_signal.as_mut() => {
                tracing::info!("shutting down worker!");
                return;
            },
            delivery = receiver.recv() => delivery.expect("sender should be alive"),
        };

        let span = tracing::span!(
            tracing::Level::INFO,
            "received a message to process",
            header
        );
        let _entered = span.enter();

        if !delegator.delegate(&header, contents).await {
            tracing::error!("failed to delegate message with header {}", header);
        }

        if let Err(err) = channel
            .basic_ack(delivery_tag, BasicAckOptions::default())
            .await
        {
            tracing::error!("failed to ack msg: {}", err);
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
        &self,
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
        &self,
        msg: Self::Message<'_>,
        raw: &[u8],
    ) -> Result<(), Self::ConsumerError>;

    async fn try_process(&self, contents: Vec<u8>) {
        if let Err(err) = self.try_consume_inner(contents).await {
            tracing::error!("failed to process message: {}", err);
        }
    }

    async fn try_consume_inner(
        &self,
        contents: Vec<u8>,
    ) -> Result<(), Self::ConsumerError> {
        let message = self.parse_msg(&contents)?;
        self.process(message, &contents).await
    }
}

#[async_trait]
pub trait RabbitDelegator: Send + Sync + 'static {
    async fn delegate(&self, header: &str, contents: Vec<u8>) -> bool;
}

macro_rules! delegator_tuple {
    ( $ty:tt ) => {
        #[allow(non_snake_case, unused_parens)]
        #[async_trait]
        impl< $ty > RabbitDelegator for Arc<$ty>
        where
            $ty: RabbitConsumer
        {
            async fn delegate(&self, header: &str, contents: Vec<u8>) -> bool {
                if self.header_matches(header) {
                    self.try_process(contents).await;
                    return true;
                }
                false
            }
        }

        #[allow(non_snake_case, unused_parens)]
        #[async_trait]
        impl< $ty > RabbitDelegator for ($ty,)
        where
            $ty: RabbitConsumer
        {
            async fn delegate(&self, header: &str, contents: Vec<u8>) -> bool {
                let ($ty,) = self;
                if $ty.header_matches(header) {
                    $ty.try_process(contents).await;
                    return true;
                }
                false
            }
        }
    };
    ( $($ty:tt),* ) => {
        #[allow(non_snake_case, unused_parens)]
        #[async_trait]
        impl< $($ty),* > RabbitDelegator for (
            $($ty),*
        )
        where
            $($ty: RabbitConsumer),*
        {
            async fn delegate(&self, header: &str, contents: Vec<u8>) -> bool {
                let ($($ty),*) = self;
                $(
                if $ty.header_matches(header) {
                    $ty.try_process(contents).await;
                    return true;
                }
                )*
                false
            }
        }
    }
}

delegator_tuple!(A);
delegator_tuple!(A, B);
delegator_tuple!(A, B, C);
delegator_tuple!(A, B, C, D);
delegator_tuple!(A, B, C, D, E);
delegator_tuple!(A, B, C, D, E, F);
delegator_tuple!(A, B, C, D, E, F, G);
delegator_tuple!(A, B, C, D, E, F, G, H);
delegator_tuple!(A, B, C, D, E, F, G, H, I);
delegator_tuple!(A, B, C, D, E, F, G, H, I, J);
delegator_tuple!(A, B, C, D, E, F, G, H, I, J, K);

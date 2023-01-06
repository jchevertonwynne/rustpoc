use std::future::Future;
use std::net::TcpListener;
use std::sync::Arc;

use anyhow::Context;
use mongodb::options::ClientOptions;
use tokio::sync::{Mutex, Notify};

use rustpoc::db::DataBase;
use rustpoc::grpc::voting_client::VotingClient;
use rustpoc::rabbit::{BodyConsumer, Rabbit, QUEUE};
use rustpoc::server::Server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .init();

    let shutdown = Killer::new();

    let grpc_handle = tokio::spawn(rustpoc::grpc::run_server(
        "127.0.0.1:3001"
            .parse()
            .context("failed to parse address")?,
        shutdown.kill_signal(),
    ));

    let database = Arc::new(DataBase::new({
        let mut client_options = ClientOptions::parse("mongodb://localhost:27017").await?;
        client_options.app_name = Some("joseph".to_string());
        client_options
    })?);

    tracing::info!("connected to mongo");

    let rabbit = Arc::new(
        Rabbit::new("amqp://localhost:5672")
            .await
            .context("failed to create rabbit conn")?,
    );

    tracing::info!("connected to rabbit");

    rabbit
        .setup()
        .await
        .context("failed to initialise rabbit")?;

    tracing::info!("checked rabbit queue and exchange bindings");

    let delegator = Arc::new(BodyConsumer::default());
    // let delegator = (Arc::new(BodyConsumer::default()), Arc::new(SomeOtherConsumer::default()));
    rabbit
        .consume(QUEUE, delegator)
        .await
        .context("failed to start consumer")?;

    tracing::info!("setup message listener");

    let client = Arc::new(Mutex::new(
        VotingClient::connect("http://127.0.0.1:3001")
            .await
            .context("failed to create grpc client")?,
    ));

    tracing::info!("grpc client connected");

    let listener = TcpListener::bind(("127.0.0.1", 2987))
        .context("failed to bind tcp listener to port")?;

    let app = Server::new(Arc::clone(&rabbit), database, client)
        .build_server(listener, shutdown.kill_signal())
        .context("failed to build server")?;

    let axum_handle = tokio::spawn(app);

    tokio::signal::ctrl_c()
        .await
        .context("failed to receive ctrl c")?;

    shutdown.kill();

    tracing::info!("closing down program...");

    axum_handle
        .await
        .context("join handle failure")?
        .context("axum server failure")?;

    tracing::info!("axum shutdown");

    rabbit.close().await.context("failed to close rabbit conn")?;

    tracing::info!("rabbit shutdown");

    grpc_handle.await.context("tonic server failure")?;

    tracing::info!("tonic shutdown");

    Ok(())
}

struct Killer {
    inner: Arc<Notify>,
}

impl Killer {
    fn new() -> Killer {
        Killer {
            inner: Arc::new(Notify::new()),
        }
    }

    fn kill_signal(&self) -> impl Future<Output = ()> {
        let notify = Arc::clone(&self.inner);
        async move { notify.notified().await }
    }

    fn kill(&self) {
        self.inner.notify_waiters()
    }
}

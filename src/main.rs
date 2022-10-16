use std::net::SocketAddr;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use mongodb::options::ClientOptions;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::Mutex;

use rustpoc::db::DataBase;
use rustpoc::grpc::voting_client::VotingClient;
use rustpoc::rabbit::Rabbit;
use rustpoc::rabbit::MESSAGE_TYPE;
use rustpoc::server::{Body, Server};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .init();

    let (tx, mut rx): (Sender<()>, Receiver<()>) = tokio::sync::broadcast::channel(1);

    let killer = async move {
        rx.recv().await.expect("TODO: panic message");
    };

    let grpc_handle = tokio::spawn(rustpoc::grpc::run_server(
        "127.0.0.1:3001"
            .parse()
            .context("failed to parse address")?,
        killer,
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

    let rabbit_consume_handle = tokio::spawn(
        rabbit
            .consume::<Body>(MESSAGE_TYPE, tx.subscribe())
            .await
            .context("failed to start consumer")?,
    );

    tracing::info!("setup message listener");

    let client = Arc::new(Mutex::new(
        VotingClient::connect("http://127.0.0.1:3001")
            .await
            .context("failed to create grpc client")?,
    ));

    tracing::info!("grpc client connected");

    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 3000)))
        .context("failed to bind tcp listener to port")?;

    let mut rx = tx.subscribe();
    let killer = async move {
        rx.recv().await.expect("TODO: panic message");
    };

    let app = Server::new(rabbit, database, client)
        .build_server(listener, killer)
        .context("failed to build server")?;

    let axum_handle = tokio::spawn(app);

    tokio::signal::ctrl_c()
        .await
        .context("failed to receive ctrl c")?;

    tx.send(()).context("failed to send shutdown message")?;

    std::thread::spawn(|| {
        std::thread::sleep(Duration::from_secs(10));
        tracing::error!("shutting down manually");
        std::process::exit(1);
    });

    tracing::info!("end of program");

    axum_handle
        .await
        .context("join handle failure")?
        .context("axum server failure")?;

    tracing::info!("axum shutdown");

    grpc_handle.await.context("tonic server failure")?;

    tracing::info!("tonic shutdown");

    rabbit_consume_handle
        .await
        .context("rabbit shutdown failure")?;

    tracing::info!("rabbit shutdown");

    Ok(())
}

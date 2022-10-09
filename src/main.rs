use std::net::SocketAddr;
use std::net::TcpListener;
use std::sync::Arc;

use anyhow::Context;
use mongodb::options::ClientOptions;
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

    rustpoc::grpc::run_server("127.0.0.1:3001".parse().context("failed to parse address")?);

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

    rabbit
        .consume::<Body>(MESSAGE_TYPE)
        .await
        .context("failed to start consumer")?;

    tracing::info!("setup message listener");

    let client = Arc::new(Mutex::new(
        VotingClient::connect("http://127.0.0.1:3001")
            .await
            .context("failed to create grpc client")?,
    ));

    tracing::info!("grpc client connected");

    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 3000)))
        .context("failed to bind tcp listener to port")?;

    let app = Server::new(rabbit, database, client);
    app.run(listener).await.context("app run failed")?;

    Ok(())
}

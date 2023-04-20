use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use mongodb::options::ClientOptions;
use opentelemetry::global;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use opentelemetry::sdk::{trace, Resource};
use opentelemetry::KeyValue;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, Registry};

use rustpoc::db::DataBase;
use rustpoc::grpc::voting_client::VotingClient;
use rustpoc::rabbit::{BodyConsumer, Rabbit, QUEUE};
use rustpoc::server::Server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let exporter = opentelemetry_otlp::new_exporter().tonic();

    // Define Tracer
    let oltp_tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(exporter)
        .with_trace_config(
            trace::config().with_resource(Resource::new(vec![KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_NAME.to_string(),
                "rustpoc".to_string(),
            )])),
        )
        .install_batch(opentelemetry::runtime::Tokio)?;

    // Layer to add our configured tracer.
    let tracing_layer = tracing_opentelemetry::layer()
        .with_tracer(oltp_tracer)
        .with_location(true);

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("INFO"));

    global::set_text_map_propagator(TraceContextPropagator::new());

    Registry::default()
        .with(env_filter)
        .with(Layer::new().with_line_number(true).with_file(true))
        .with(tracing_layer)
        .init();

    let shutdown = CancellationToken::new();

    let killer = {
        let shutdown = shutdown.clone();
        async move { shutdown.cancelled().await }
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

    let delegator = Arc::new(BodyConsumer::default());
    let consumer_handle = rabbit
        .consume(QUEUE, delegator, shutdown.clone())
        .await
        .context("failed to start consumer")?;

    tracing::info!("setup message listener");

    let client = Arc::new(Mutex::new(
        VotingClient::connect("http://127.0.0.1:3001")
            .await
            .context("failed to create grpc client")?,
    ));

    tracing::info!("grpc client connected");

    let listener =
        TcpListener::bind(("127.0.0.1", 2987)).context("failed to bind tcp listener to port")?;

    let app = Server::new(Arc::clone(&rabbit), database, client)
        .build_server(listener, shutdown.clone())
        .context("failed to build server")?;

    let axum_handle = tokio::spawn(app);

    tokio::signal::ctrl_c()
        .await
        .context("failed to receive ctrl c")?;

    std::thread::spawn(|| {
        std::thread::sleep(Duration::from_secs(10));
        tracing::error!("shutting down via abort");
        std::process::abort();
    });

    shutdown.cancel();

    tracing::info!("closing down program...");

    axum_handle
        .await
        .context("join handle failure")?
        .context("axum server failure")?;

    tracing::info!("axum shutdown");

    rabbit
        .close()
        .await
        .context("failed to close rabbit conn")?;

    tracing::info!("rabbit shutdown");

    consumer_handle.await?;

    tracing::info!("consumer shutdown");

    grpc_handle.await.context("tonic server failure")?;

    tracing::info!("tonic shutdown");

    global::shutdown_tracer_provider();

    Ok(())
}

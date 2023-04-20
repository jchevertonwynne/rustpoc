use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::rejection::JsonRejection;
use axum::extract::State;
use axum::response::Response;
use axum::routing::post;
use axum::{http::StatusCode, response::IntoResponse, Json, Router};
use mongodb::bson::oid::ObjectId;
use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;
use serde_with::DisplayFromStr;
use thiserror::Error;
use time::OffsetDateTime;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::Level;

use crate::db::{ChosenCollection, DataBase, Object};
use crate::grpc::voting_client::VotingClient;
use crate::grpc::voting_request::Vote;
use crate::grpc::VotingRequest;
use crate::rabbit::{PublishError, Rabbit, EXCHANGE, MESSAGE_TYPE};

pub struct Server {
    router: Router,
}

#[derive(Clone, axum_macros::FromRef)]
struct AppState {
    rabbit: Arc<Rabbit>,
    database: Arc<DataBase>,
    grpc_client: Arc<Mutex<VotingClient<Channel>>>,
}

impl Server {
    pub fn new(
        rabbit: Arc<Rabbit>,
        database: Arc<DataBase>,
        grpc_client: Arc<Mutex<VotingClient<Channel>>>,
    ) -> Server {
        let router = Router::new()
            .layer(axum_tracing_opentelemetry::opentelemetry_tracing_layer())
            .route("/divide", post(divide))
            .route("/", post(handle))
            .with_state(AppState {
                rabbit,
                database,
                grpc_client,
            });
        Server { router }
    }

    pub fn build_server(
        self,
        listener: std::net::TcpListener,
        cancel: CancellationToken,
    ) -> Result<impl Send + Future<Output = Result<(), hyper::Error>>, RunError> {
        let shutdown = async move { cancel.cancelled().await };
        let server = axum::Server::from_tcp(listener)?
            .serve(
                self.router
                    .into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(shutdown);
        Ok(server)
    }
}

#[derive(Error, Debug)]
pub enum RunError {
    #[error("failed to create server from tcp listener: {0}")]
    FromTCPError(#[from] hyper::Error),
    #[error("failed to serve: {0}")]
    ServeError(#[from] axum::Error),
}

async fn divide(
    Json(DivideRequest {
        numerator,
        denominator,
    }): Json<DivideRequest>,
) -> Json<DivideResult> {
    Json(DivideResult {
        result: numerator / denominator,
    })
}

#[derive(Deserialize)]
pub struct DivideRequest {
    numerator: isize,
    denominator: isize,
}

#[derive(Serialize)]
pub struct DivideResult {
    result: isize,
}

#[tracing::instrument(name = "hit the handler which does stuff", skip(rabbit, mongo, grpc))]
async fn handle(
    State(rabbit): State<Arc<Rabbit>>,
    State(mongo): State<Arc<DataBase>>,
    State(grpc): State<Arc<Mutex<VotingClient<Channel>>>>,
    res: Result<Json<Body>, JsonRejection>,
) -> Result<Json<ResultBody>, ServeError> {
    tracing::info!("i am in the handler!");
    let body = match res {
        Ok(Json(body)) => body,
        Err(err) => {
            tracing::info!("the payload passed was not valid json: {:?}", err);
            return Err(err.into());
        }
    };

    let conn_type_span = tracing::span!(Level::INFO, "connector type", body.connector_type);
    let _entered = conn_type_span.enter();

    let col = mongo.collection::<Object>(ChosenCollection::Sample);

    if let Err(err) = col.insert(body.clone()).await {
        tracing::error!("failed to insert doc to mongo: {:?}", err);
        return Err(err.into());
    }

    let doc_count = match col.count().await {
        Ok(count) => count,
        Err(err) => {
            tracing::error!("failed to count mongo docs: {:?}", err);
            return Err(err.into());
        }
    };

    let doc_count_span =
        tracing::span!(parent: &conn_type_span, Level::INFO, "doc count", doc_count);
    let _entered = doc_count_span.enter();

    if let Err(err) = rabbit.publish_json(EXCHANGE, MESSAGE_TYPE, body).await {
        tracing::error!("failed to publish rabbit msg: {:?}", err);
        return Err(err.into());
    }

    let vote_result = match grpc
        .lock()
        .await
        .vote(VotingRequest {
            url: "yolo".to_string(),
            vote: Vote::Up.into(),
        })
        .await
    {
        Ok(resp) => resp,
        Err(err) => {
            tracing::error!("failed to grpc vote: {:?}", err);
            return Err(err.into());
        }
    };

    tracing::info!("received vote response: {:?}", vote_result);

    Ok(Json(ResultBody { doc_count }))
}

#[derive(Serialize)]
struct ResultBody {
    doc_count: u64,
}

#[derive(Debug, Error)]
pub enum ServeError {
    #[error("error in the json payload: {0}")]
    JsonErr(#[from] JsonRejection),
    #[error("a database error occurred: {0}")]
    DatabaseErr(#[from] mongodb::error::Error),
    #[error("failed to publish to rabbit: {0}")]
    RabbitError(#[from] PublishError),
    #[error("grpc call failed: {0}")]
    GrpcError(#[from] tonic::Status),
}

impl IntoResponse for ServeError {
    fn into_response(self) -> Response {
        let code = match self {
            ServeError::JsonErr(_) => StatusCode::BAD_REQUEST,
            ServeError::DatabaseErr(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ServeError::RabbitError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ServeError::GrpcError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (code, format!("{}", self)).into_response()
    }
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Body {
    #[serde_as(as = "DisplayFromStr")]
    #[serde(rename = "gamesConsoleConnectorId")]
    pub games_console_connector_id: ObjectId,
    #[serde_as(as = "DisplayFromStr")]
    #[serde(rename = "projectId")]
    pub project_id: ObjectId,
    #[serde(rename = "type")]
    pub connector_type: String,
    #[serde(rename = "triggeredAt", with = "time::serde::rfc3339")]
    pub triggered_at: OffsetDateTime,
    #[serde(rename = "oldStatus")]
    pub old_status: String,
    #[serde(rename = "newStatus")]
    pub new_status: String,
}

impl From<Body> for Object {
    fn from(body: Body) -> Self {
        let Body {
            games_console_connector_id,
            project_id,
            connector_type,
            triggered_at,
            old_status,
            new_status,
        } = body;

        Self {
            games_console_connector_id,
            project_id,
            connector_type,
            triggered_at,
            old_status,
            new_status,
        }
    }
}

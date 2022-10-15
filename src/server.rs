use std::sync::Arc;

use axum::extract::rejection::JsonRejection;

use axum::response::Response;
use axum::routing::post;
use axum::{http::StatusCode, response::IntoResponse, Extension, Json, Router};

use mongodb::bson::oid::ObjectId;

use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;
use serde_with::DisplayFromStr;

use thiserror::Error;
use time::OffsetDateTime;
use tokio::sync::broadcast::Receiver;
use tokio::sync::Mutex;

use tonic::transport::Channel;
use tower_http::add_extension::AddExtensionLayer;

use crate::db::{ChosenCollection, DataBase, Object};
use crate::grpc::voting_client::VotingClient;
use crate::grpc::voting_request::Vote;
use crate::grpc::VotingRequest;
use crate::rabbit::{PublishError, Rabbit};
use crate::Holder;

pub struct Server {
    router: Router,
}

impl Server {
    pub fn new(
        rabbit: Arc<Rabbit>,
        database: Arc<DataBase>,
        grpc_client: Arc<Mutex<VotingClient<Channel>>>,
    ) -> Server {
        let router = Router::new()
            .route("/divide", post(divide))
            .route("/", post(handle))
            .layer(AddExtensionLayer::new(rabbit))
            .layer(AddExtensionLayer::new(database))
            .layer(AddExtensionLayer::new(grpc_client));
        Server { router }
    }

    pub async fn run(
        self,
        listener: std::net::TcpListener,
        receiver: Receiver<()>,
    ) -> Result<(), RunError> {
        axum::Server::from_tcp(listener)?
            .serve(self.router.into_make_service())
            .with_graceful_shutdown(Holder::new(receiver)).await?;

        Ok(())
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

async fn handle(
    res: Result<Json<Body>, JsonRejection>,
    Extension(rabbit): Extension<Arc<Rabbit>>,
    Extension(mongo): Extension<Arc<DataBase>>,
    Extension(grpc): Extension<Arc<Mutex<VotingClient<Channel>>>>,
) -> Result<Json<ResultBody>, ServeError> {
    let body = match res {
        Ok(Json(body)) => body,
        Err(err) => {
            tracing::info!("the payload passed was not valid json: {:?}", err);
            return Err(err.into());
        }
    };

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

    if let Err(err) = rabbit.publish_json(body).await {
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
    games_console_connector_id: ObjectId,
    #[serde_as(as = "DisplayFromStr")]
    #[serde(rename = "projectId")]
    project_id: ObjectId,
    #[serde(rename = "type")]
    connector_type: String,
    #[serde(rename = "triggeredAt", with = "time::serde::rfc3339")]
    triggered_at: OffsetDateTime,
    #[serde(rename = "oldStatus")]
    old_status: String,
    #[serde(rename = "newStatus")]
    new_status: String,
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

use std::sync::Arc;

use axum::extract::rejection::JsonRejection;
use axum::http::Request;
use axum::middleware::{from_fn, Next};
use axum::response::Response;
use axum::{http::StatusCode, response::IntoResponse, Extension, Json, Router};
use mongodb::bson::oid::ObjectId;
use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;
use serde_with::DisplayFromStr;
use thiserror::Error;
use time::OffsetDateTime;
use tower_http::add_extension::AddExtensionLayer;

use crate::db::{DataBase, COLLECTION};
use crate::rabbit::{PublishError, Rabbit};

pub struct Server {
    router: Router,
}

impl Server {
    pub fn new(rabbit: Arc<Rabbit>, database: Arc<DataBase>) -> Server {
        let router = axum::Router::new()
            .route("/", axum::routing::post(handle))
            .layer(AddExtensionLayer::new(rabbit))
            .layer(AddExtensionLayer::new(database))
            .route_layer(from_fn(bugsnag_non_200s));
        Server { router }
    }

    pub async fn run(self, listener: std::net::TcpListener) -> anyhow::Result<()> {
        axum::Server::from_tcp(listener)?
            .serve(self.router.into_make_service())
            .await?;
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum RunError {
    #[error("failed to create server from tcp listener: {0}")]
    CreateListenerError(#[from] axum::Error),
}

async fn bugsnag_non_200s<B>(req: Request<B>, next: Next<B>) -> Result<Response, StatusCode> {
    let resp = next.run(req).await;

    if resp.status() != StatusCode::OK {
        tracing::info!(
            "lmao, a request failed: code = {}, please bugsnag me",
            resp.status()
        );
    }

    Ok(resp)
}

async fn handle(
    res: Result<Json<Body>, JsonRejection>,
    Extension(rabbit): Extension<Arc<Rabbit>>,
    Extension(mongo): Extension<Arc<DataBase>>,
) -> Result<String, ServeError> {
    let body = match res {
        Ok(Json(body)) => body,
        Err(err) => {
            tracing::info!("the payload passed was not valid json: {:?}", err);
            return Err(err.into());
        }
    };

    let col = mongo.collection(COLLECTION);
    if let Err(err) = col.insert_one(body.clone(), None).await {
        tracing::error!("failed to insert doc to mongo: {:?}", err);
        return Err(err.into());
    }

    let doc_count = match col.count_documents(None, None).await {
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

    #[derive(Serialize)]
    struct Result {
        doc_count: u64,
    }

    let result = match serde_json::to_string(&Result { doc_count }) {
        Ok(result) => result,
        Err(err) => {
            tracing::error!("failed to serialize response: {:?}", err);
            return Err(err.into());
        }
    };

    Ok(result)
}

#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("error in the json payload: {0}")]
    JsonErr(#[from] JsonRejection),
    #[error("a database error occurred: {0}")]
    DatabaseErr(#[from] mongodb::error::Error),
    #[error("failed to serialize response: {0}")]
    SerializeError(#[from] serde_json::error::Error),
    #[error("failed to publish to rabbit: {0}")]
    RabbitError(#[from] PublishError),
}

impl IntoResponse for ServeError {
    fn into_response(self) -> Response {
        let code = match self {
            ServeError::JsonErr(_) => StatusCode::BAD_REQUEST,
            ServeError::DatabaseErr(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ServeError::SerializeError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ServeError::RabbitError(_) => StatusCode::INTERNAL_SERVER_ERROR,
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
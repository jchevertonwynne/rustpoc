use mongodb::bson::oid::ObjectId;
use mongodb::bson::Document;
use mongodb::options::ClientOptions;
use mongodb::results::InsertOneResult;
use mongodb::{Client, Collection};
use time::OffsetDateTime;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

const DATABASE: &str = "joseph";

pub enum ChosenCollection {
    Sample,
}

impl From<ChosenCollection> for &'static str {
    fn from(col: ChosenCollection) -> Self {
        match col {
            ChosenCollection::Sample => "sample",
        }
    }
}

pub struct DataBase {
    #[allow(dead_code)]
    client: Client,
    database: mongodb::Database,
}

impl DataBase {
    pub fn new(client_options: ClientOptions) -> Result<DataBase, mongodb::error::Error> {
        let client = Client::with_options(client_options)?;
        let database = client.database(DATABASE);
        Ok(DataBase { client, database })
    }

    pub fn collection<T: Serialize + DeserializeOwned>(
        &self,
        collection: ChosenCollection,
    ) -> MongoCollection<T> {
        MongoCollection {
            collection: self.database.collection(collection.into()),
        }
    }
}

pub struct MongoCollection<T> {
    collection: Collection<T>,
}

impl<T> MongoCollection<T> {
    pub async fn count(&self) -> Result<u64, mongodb::error::Error> {
        self.collection.count_documents(None, None).await
    }
}

impl<T: Serialize> MongoCollection<T> {
    pub async fn insert(
        &self,
        obj: impl Into<T>,
    ) -> Result<InsertOneResult, mongodb::error::Error> {
        self.collection.insert_one(obj.into(), None).await
    }
}

impl<T: DeserializeOwned + Unpin + Send + Sync> MongoCollection<T> {
    pub async fn find(
        &self,
        filter: impl Into<Option<Document>>,
    ) -> Result<Option<T>, mongodb::error::Error> {
        self.collection.find_one(filter, None).await
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Object {
    #[serde(rename = "gamesConsoleConnectorId")]
    pub games_console_connector_id: ObjectId,
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

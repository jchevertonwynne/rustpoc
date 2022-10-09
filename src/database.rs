use mongodb::bson::oid::ObjectId;
use mongodb::bson::Document;

use mongodb::{options::ClientOptions, Client};
use rustpoc::server::Body;
use serde::Deserialize;
use serde::Serialize;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse a connection string into an options struct.
    let mut client_options = ClientOptions::parse("mongodb://localhost:27017").await?;

    // Manually set an option.
    client_options.app_name = Some("joseph".to_string());

    // Get a handle to the deployment.
    let client = Client::with_options(client_options)?;

    // client
    //     .database("joseph")
    //     .create_collection("sample", CreateCollectionOptions::default())
    //     .await?;

    client
        .database("joseph")
        .collection::<Body>("sample")
        .delete_many(Document::default(), None)
        .await
        .unwrap();

    // let mut cursor = client
    //     .database("bugsnag_development")
    //     .collection::<Account>("accounts")
    //     .find(None, None)
    //     .await?;
    //
    // loop {
    //     if !cursor.advance().await? {
    //         break;
    //     }
    //
    //     let account = cursor.deserialize_current()?;
    //     println!("{:?}", account.id);
    // }

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Account {
    #[serde(rename = "_id")]
    pub id: ObjectId,
}

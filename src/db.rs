use mongodb::options::ClientOptions;
use mongodb::{Client, Collection};

use serde::{Deserialize, Serialize};

const DATABASE: &str = "joseph";
pub const COLLECTION: &str = "sample";

pub struct DataBase {
    #[allow(dead_code)]
    client: Client,
    database: mongodb::Database,
}

impl DataBase {
    pub fn new(client_options: ClientOptions) -> mongodb::error::Result<DataBase> {
        let client = Client::with_options(client_options)?;
        let database = client.database(DATABASE);
        Ok(DataBase { client, database })
    }

    pub fn collection<'a, T: Serialize + Deserialize<'a>>(
        &self,
        collection: &str,
    ) -> Collection<T> {
        self.database.collection(collection)
    }
}

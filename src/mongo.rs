// using this file to connect to the mongodb
use mongodb::sync::Client;
pub struct MongoDb {
    pub db: Client,
}

impl MongoDb {
    pub fn new() -> Self {
        // use uri from env
        let uri = std::env::var("MONGO_URI").unwrap();
        let client = Client::with_uri_str(uri).unwrap();
        MongoDb { db: client }
    }
    pub fn new_with_uri(uri: &str) -> Self {
        let client = Client::with_uri_str(uri).unwrap();
        MongoDb { db: client }
    }
    
}

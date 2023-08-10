// using this file to connect to the mongodb
use mongodb::Client;
pub struct MongoDb {
    pub db: Client,
}

impl MongoDb {
    pub async fn new() -> Self {
        // use uri from env
        let uri = std::env::var("MONGO_URI").unwrap();
        let client = Client::with_uri_str(uri).await.unwrap();
        MongoDb { db: client }
    }
    pub async fn new_with_uri(uri: &str) -> Self {
        let client = Client::with_uri_str(uri).await.unwrap();
        MongoDb { db: client }
    }
    
}

// impl FromRedisValue for Document {
//     fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
//         match *v {
//             redis::Value::Data(ref bytes) => Ok(bson::from_slice(bytes).unwrap()),
//             _ => Err((redis::ErrorKind::TypeError, "Invalid type").into()),
//         }
//     }
// }
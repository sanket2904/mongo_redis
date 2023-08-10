// use crate::dese
use crate::{
    commands::Handler,
    handler::CommandExecutionError,
    rd::{RedisDeserializer, RedisSerializer},
};
use bson::{doc, Bson, Document, RawDocumentBuf};
// use mongodb::Cursor;

use super::hash;


use redis::FromRedisValue;
use serde_json::Value;
pub struct Find {}

impl Handler for Find {
    fn new() -> Self {
        return Find {};
    }

    fn handle(
        &self,
        _request: &crate::handler::Request,
        docs: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        println!("find command");
        let doc = &docs[0];
        let db = doc.get_str("$db").unwrap();
        let collection = doc.get_str("find").unwrap();
        let filter = if doc.contains_key("filter") {
            doc.get_document("filter").unwrap().clone()
        } else {
            doc! {}
        };
        let redis_db = _request.redis_client;
        let hash = hash(&filter, collection);

        let mut con = redis_db.get_connection().unwrap();
        let data = redis::cmd("JSON.GET")
            .arg(hash.clone())
            .query::<String>(&mut con);

        match data {
            Ok(val) => {
                let j: Value = serde_json::from_str(&val).unwrap();
                let bson_doc = j.from_redis();
                let main_doc = bson_doc.as_document().unwrap();
                let c = main_doc
                    .get("cursor")
                    .unwrap()
                    .as_document()
                    .unwrap()
                    .get_array("firstBatch")
                    .unwrap();

                if c.len() == 0 {
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    let mongo_client = _request.client;
                    let res = rt.block_on(doc_finder(collection, filter, db, mongo_client));
                    // convert the document to bson
                    let doc_bson = bson::to_bson(&res.clone().unwrap()).unwrap();
                    let to_be_stored = doc_bson.to_redis().to_string();
                    let _response_redis = redis::cmd("JSON.SET")
                        .arg(hash)
                        .arg(".")
                        .arg(to_be_stored)
                        .query::<String>(&mut con);

                    return res;
                } else {
                    // convert the string to document
                    return Ok(doc! {
                        "ok": Bson::Double(1.0),
                        "cursor": {
                            "id": Bson::Int64(0),
                            "ns": format!("{}.{}", db, collection),
                            "firstBatch": c ,
                        }
                    });
                }
            }
            Err(e) => {
                if e.kind() == redis::ErrorKind::TypeError {
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    let mongo_client = _request.client;
                    let res = rt.block_on(doc_finder(collection, filter, db,mongo_client));
                    // convert the document to bson
                    let doc_bson = bson::to_bson(&res.clone().unwrap()).unwrap();
                    let to_be_stored = doc_bson.to_redis().to_string();
                    let _response_redis = redis::cmd("JSON.SET")
                        .arg(hash)
                        .arg(".")
                        .arg(to_be_stored)
                        .query::<String>(&mut con);

                    return res;
                } else {
                    return Err(CommandExecutionError::new(format!(
                        "error during find: {:?}",
                        e
                    )));
                }
            }
        }
    }
}

async fn doc_finder(
    collection: &str,
    filter: Document,
    db: &str,
    mongo_client: &mongodb::Client,
) -> Result<Document, CommandExecutionError> {
    // let mongo_client = crate::mongo::MongoDb::new().await;
    let result = mongo_client
        .database(db)
        .collection::<Document>(collection);
    
    let docs: Result<mongodb::Cursor<Document>, mongodb::error::Error> =
        result.find(filter, None).await;
    let mut res: Vec<Bson> = vec![];
    match docs {
        Ok(cursor) => {
            let mut cursor = cursor;
            while cursor.advance().await.unwrap() {
                let doc = cursor.current();
                // convert it to bson
                let bson = bson::to_bson(&doc).unwrap();
                res.push(bson);
            }

            return Ok(doc! {
                "cursor" : doc! {
                    "firstBatch": res,
                    "id": Bson::Int64(0),
                    "ns": format!("{}.{}", db, collection),
                },
                "ok": Bson::Double(1.0),
            });
        }
        Err(e) => Err(CommandExecutionError::new(format!(
            "error during find: {:?}",
            e
        ))),
    }
}

struct BsonCustom(Bson);

impl FromRedisValue for BsonCustom {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        match v {
            redis::Value::Int(ref i) => Ok(BsonCustom(Bson::Int64(*i))),
            redis::Value::Nil => Ok(BsonCustom(Bson::Null)),
            redis::Value::Data(d) => {
                let buf = RawDocumentBuf::from_bytes(d.to_vec()).unwrap();
                let raw_doc = buf.to_document();
                let bson = bson::from_bson(Bson::Document(raw_doc.unwrap())).unwrap();
                Ok(BsonCustom(bson))
            }
            redis::Value::Bulk(bulk) => {
                let mut res: Vec<Bson> = vec![];
                for item in bulk {
                    let bson = BsonCustom::from_redis_value(&item).unwrap();
                    res.push(bson.0);
                }
                Ok(BsonCustom(Bson::Array(res)))
            }
            redis::Value::Status(s) => Ok(BsonCustom(Bson::String(s.to_string()))),
            redis::Value::Okay => Ok(BsonCustom(Bson::String("OK".to_string()))),
        }
    }
}

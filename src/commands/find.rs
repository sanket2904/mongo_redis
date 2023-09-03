use std::{time, sync::Arc};

// use crate::dese
use crate::handler::CommandExecutionError;
use bson::{doc, Bson, Document};

use super::hash;
pub struct Find {}
impl Find {
    pub fn new() -> Self {
        return Find {};
    }
    pub fn handle(
        &self,
        _request: &crate::handler::Request<'_>,
        docs: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        let db = doc.get_str("$db").unwrap();
        let collection = doc.get_str("find").unwrap();
        let filter = if doc.contains_key("filter") {
            doc.get_document("filter").unwrap().clone()
        } else {
            doc! {}
        };
        let storage = _request.get_storage();
        let hash = hash(&filter);
        println!("hash: {}", hash);
        let st = storage.lock().unwrap();
        let data = st.get(&hash);
        if data.is_none() {
            println!("not found in cache");
            let mongo_client = _request.client;
            let i = time::Instant::now();
            let res = doc_finder(collection, filter, db, mongo_client).unwrap();
            println!("time taken: {:?}", i.elapsed());
            let t = res.clone();
            let storage = Arc::clone(&storage);
            std::thread::spawn(move || {
                let mut st = storage.lock().unwrap();
                st.insert(hash, t);
            });    
            return Ok(res);
        }

        println!("found in cache");
        return Ok(data.unwrap().clone());
    }
}
fn doc_finder(
    collection: &str,
    filter: Document,
    db: &str,
    mongo_client: &mongodb::sync::Client,
) -> Result<Document, CommandExecutionError> {
    // let mongo_client = crate::mongo::MongoDb::new().await;
    let result = mongo_client.database(db).collection::<Document>(collection);
    let docs = result.find(filter, None);
    match docs {
        Ok(cursor) => {
            let mut res = vec![];
            let test = cursor.into_iter();
            for doc in test {
                res.push(doc.unwrap());
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


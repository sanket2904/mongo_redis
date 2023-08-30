
use crate::{mongo::MongoDb, handler::CommandExecutionError};
use bson::{doc,Bson, Document};

pub struct ListCollections {}

impl ListCollections {
    pub fn new() -> Self {
        ListCollections {}
    }

    pub async fn handle(&self,_request: &crate::handler::Request<'_>,msg: &Vec<bson::Document>) -> Result<Document, CommandExecutionError> {
        
        let doc = &msg[0];
        let db = doc.get_str("$db").unwrap();
        let colls_doc = send_collections(db).await;
        // use while to manage cursor

        return Ok(doc! {
            "cursor": doc! {
                "id": Bson::Int64(0),
                "ns": Bson::String(format!("{}.$cmd.listCollections", db)),
                "firstBatch": Bson::Array(colls_doc),
            },
            "ok": Bson::Double(1.0),
        })
    }
}


async fn send_collections(db: &str) -> Vec<bson::Bson> {


    let mongo_client = MongoDb::new().await;
    let client = mongo_client.db.database(db);
    let mut colls = client.list_collections(None, None).await.unwrap();
    let mut colls_doc: Vec<bson::Bson> = vec![];

    while colls.advance().await.unwrap() {
        let coll = colls.current();
        let bson = bson::to_bson(&coll).unwrap();
        colls_doc.push(bson);
    }
    return colls_doc;
    
}
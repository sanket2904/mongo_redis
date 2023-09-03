pub struct Aggregate {}


impl Aggregate {
    fn new() -> Self {
        Aggregate {}
    }
    async fn handle(&self,_request: &crate::handler::Request<'_>,msg: &Vec<bson::Document>,) -> Result<bson::Document, crate::handler::CommandExecutionError> {
        let doc = &msg[0];
        let db = doc.get_str("$db").unwrap();
        let collection = doc.get_str("aggregate").unwrap();
        let pipeline = doc.get_array("pipeline").unwrap();
        let data = get_aggregate(db, collection, pipeline, _request.client).await;
        return data;
    }
}


async fn get_aggregate(db: &str , collection: &str, pipeline: &Vec<bson::Bson> , mongo_client :&mongodb::sync::Client) -> Result<bson::Document, crate::handler::CommandExecutionError> {
    // let mongo_client = crate::mongo::MongoDb::new().await.db;
    let coll = mongo_client.database(db).collection::<bson::Document>(collection); 
    let mut pipeline_vec:Vec<bson::Document> = vec![];
    for doc in pipeline {
        pipeline_vec.push(doc.as_document().unwrap().clone());
    }
    let mut cursor = coll.aggregate(pipeline_vec, None).unwrap();
    let mut docs = vec![];
    
    while cursor.advance().unwrap() {
        let doc = cursor.current();
        docs.push(bson::to_bson(&doc).unwrap());
    }
    return Ok(bson::doc! {
        "cursor": {
            "firstBatch": docs,
            "id": 0,
            "ns": format!("{}.{}", db, collection),
            "ok": 1.0
        },
        "ok": 1.0
    })
}

pub struct ListIndexes {}
impl ListIndexes {
    pub fn new() -> Self {
        ListIndexes {}
    }
    pub async fn handle(&self,_request: &crate::handler::Request<'_>,msg: &Vec<bson::Document>) -> Result<bson::Document, crate::handler::CommandExecutionError> {
        let doc = &msg[0];
        let db = doc.get_str("$db").unwrap();
        let collection = doc.get_str("listIndexes").unwrap();
        let client = _request.client;
        let coll = client.database(db).collection::<bson::Document>(collection);
        let indexes_doc =send_indexes(coll).await;
        // use while to manage cursor
        return Ok(bson::doc! {
            "cursor": {
                "id": 0,
                "ns": format!("{}.{}", db, collection),
                "firstBatch": indexes_doc
            },
            "ok": 1.0
        });
    }
}


async fn send_indexes(coll: mongodb::Collection<bson::Document>) -> Vec<bson::Bson> {
    let mut indexes_doc: Vec<bson::Bson> = vec![];
    let mut indexes = coll.list_indexes(None).await.unwrap();
    while indexes.advance().await.unwrap() {
        let index = indexes.current();
        let bson = bson::to_bson(&index).unwrap();
        indexes_doc.push(bson);
    }
    return indexes_doc;
}
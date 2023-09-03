
pub struct ListIndexes {}
impl ListIndexes {
    pub fn new() -> Self {
        ListIndexes {}
    }
    pub fn handle(&self,_request: &crate::handler::Request<'_>,msg: &Vec<bson::Document>) -> Result<bson::Document, crate::handler::CommandExecutionError> {
        let doc = &msg[0];
        let db = doc.get_str("$db").unwrap();
        let collection = doc.get_str("listIndexes").unwrap();
        let client = _request.client;
        let coll = client.database(db).collection::<bson::Document>(collection);
        let mut indexex_doc = vec![];
        let mut indexes = coll.list_indexes(None).unwrap();
        while indexes.advance().unwrap() {
            let index = indexes.current();
            let bson = bson::to_bson(&index).unwrap();
            indexex_doc.push(bson);
        }



        return Ok(bson::doc! {
            "cursor": {
                "id": 0,
                "ns": format!("{}.{}", db, collection),
                "firstBatch": indexex_doc
            },
            "ok": 1.0
        });
    }
}


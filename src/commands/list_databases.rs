
pub struct ListDatabases {}
impl ListDatabases {
    pub fn new() -> Self {
        ListDatabases {}
    }
    pub async fn  handle(&self,_request: &crate::handler::Request<'_>,msg: &Vec<bson::Document>) -> Result<bson::Document, crate::handler::CommandExecutionError> {
        let doc = &msg[0];
        let name_only = doc.get_bool("nameOnly").unwrap_or(false);
        let mongo_client = _request.client;
        let res = send_databases(name_only,mongo_client).await;

        return res;
        
    }
}
async fn send_databases(name_only: bool , mongo_client: &mongodb::Client) -> Result<bson::Document, crate::handler::CommandExecutionError> {
    let mut databases: Vec<bson::Bson> = vec![];
    // let mongo_client = MongoDb::new().await;
    let list =  mongo_client.list_databases(None, None).await.unwrap();
    let _list_name = mongo_client.list_database_names(None, None);

    for db in list {
        if name_only {
            databases.push(bson::Bson::String(db.name));
        } else {
            let b = bson::to_bson(&db).unwrap();
            databases.push(b);
        }
    }

    if name_only {
        return  Ok(bson::doc! {
            "databases": databases,
            "ok": 1.0
        })
    } else {
        return Ok(bson::doc! {
            "databases": databases,
            "totalSize": bson::Bson::Int64(0),
            "totalSizeMb": bson::Bson::Int64(0),
            "ok": bson::Bson::Double(1.0),
        })
    }

}
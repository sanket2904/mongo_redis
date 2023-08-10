// use crate::mongo::MongoDb;

// use super::Handler;



// pub struct  DbStats {}

// impl  Handler for DbStats {
//     fn new() -> Self {
//         DbStats {}
//     }
//     fn handle(&self,request: &crate::handler::Request,msg: &Vec<bson::Document>,) -> Result<bson::Document, crate::handler::CommandExecutionError> {
//         let doc = &msg[0];
//         let name = doc.get_str("$db").unwrap();
//         let scale = doc.get_f64("scale").unwrap_or(1.0);



//     }
// }



// async fn stats(db : &str) {
//     let mongo_client = MongoDb::new().await.db;
//     let collections_count = mongo_client.database(db).list_collection_names(None).await.unwrap().len();
//     let total_objects = 
// }
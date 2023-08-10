// use crate::{handler::CommandExecutionError, mongo::MongoDb};
// use bson::{doc, Bson, Document};

// use super::Handler;
// pub struct CollStats {}


// impl Handler for CollStats {
//     fn new() -> Self {
//         CollStats {}
//     }
//     fn handle(&self,request: &crate::handler::Request,msg: &Vec<Document>,) -> Result<Document, CommandExecutionError> {
//         let doc = &msg[0];
        
//     }
// }

// async fn get_coll_stats(db: &str , coll: &str) {
//     let mongo_client = MongoDb::new().await.db;
//     let coll = mongo_client.database(db).collection::<bson::Document>(coll); 
    
   
//     let count = coll.count_documents(None, None).await.unwrap();
//     let total_index_size = coll
// }

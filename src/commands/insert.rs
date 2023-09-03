use crate::handler::{CommandExecutionError, Request};

use bson::{doc, Bson, Document};
pub struct Insert {}
impl Insert {
    pub fn new() -> Self {
        Insert {}
    }
    pub fn handle(&self,_request: &Request<'_>,msg: &Vec<Document>,) -> Result<Document, CommandExecutionError> {
        let doc = &msg[0];
        let db = doc.get_str("$db").unwrap();
        let collection = doc.get_str("insert").unwrap();
        
        let docs: Vec<Document> = doc.get_array("documents").unwrap().iter().map(|d| d.as_document().unwrap().clone()).collect();

        // let docss = docs.clone();
        let mongo_client = _request.client;
        let result =  mongo_client.database(db).collection(collection).insert_many(docs , None);
        match result {
            Ok(_) => Ok(doc! {
                "ok": Bson::Double(1.0),
            }),
            Err(e) => Err(CommandExecutionError::new(e.to_string())),
        }

    }
}






// fn print_data_types(doc: &Document , collection : &str) -> String {
//     let mut redis_index_command = format!("FT.CREATE {} ON JSON PREFIX 1 {}: SCHEMA",collection, collection);

//     for (key , value ) in doc.iter() {
//         let data_type = match value {
//             Bson::String(_) => format!("$.{} AS {} TEXT",key,key),
//             Bson::Double(_) => format!("$.{} AS {} NUMERIC",key,key),
//             Bson::Int32(_) => format!("$.{} AS {} NUMERIC",key,key),
//             Bson::Int64(_) => format!("$.{} AS {} NUMERIC",key,key),
//             Bson::Boolean(_) => format!("$.{} AS {} TAG",key,key),
//             // will do the rest later
//             _ => "".to_string(),
//         };
        
//         if data_type != "" {
//             redis_index_command = format!("{} {}",redis_index_command,data_type);
//         }
//     }
//     return redis_index_command;
// }


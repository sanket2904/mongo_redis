use crate::handler::{CommandExecutionError, Request};
use crate::commands::Handler;
use bson::{doc, Bson, Document};
pub struct Create {}
impl Handler for Create {
    fn new() -> Self {
        Create {}
    }
    fn handle(
        &self,
        _request: &Request,
        docs: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        let db = doc.get_str("$db").unwrap();
        let collection = doc.get_str("create").unwrap();
        // create the collection
        let rt = tokio::runtime::Runtime::new().unwrap();
        // let result = rt.block_on( request.get_mongo_client().database(db).create_collection(collection , None) );
        let mongo_client = _request.client;
        let result = rt.block_on( mongo_client.database(db).create_collection(collection , None) );
        match result {
            Ok(_) => Ok(doc! {
                "ok": Bson::Double(1.0),
            }),
            Err(_e) => Err(CommandExecutionError::new("AlreadyExists".to_string())),
        }
    }
}
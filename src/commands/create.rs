use crate::handler::{CommandExecutionError, Request};

use bson::{doc, Bson, Document};
pub struct Create {}
impl Create {
    pub fn new() -> Self {
        Create {}
    }
    pub fn handle(
        &self,
        _request: &Request<'_>,
        docs: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        let db = doc.get_str("$db").unwrap();
        let collection = doc.get_str("create").unwrap();
        let mongo_client = _request.client;
        let result =mongo_client.database(db).create_collection(collection , None);
        match result {
            Ok(_) => Ok(doc! {
                "ok": Bson::Double(1.0),
            }),
            Err(_e) => Err(CommandExecutionError::new("AlreadyExists".to_string())),
        }
    }
}
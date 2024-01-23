use std::{collections::hash_map::DefaultHasher, hash::{Hash, Hasher}};
use crate::handler::{Request, CommandExecutionError};
use bson::Document;
pub mod is_master;
pub trait Handler {
    fn new() -> Self;
    fn handle(&self,request: &Request,msg: &Vec<Document>,) -> Result<Document, CommandExecutionError>;
}
pub fn hash(filter: &Document ) -> String {
    let mut hasher = DefaultHasher::new();
    let filter_bytes = filter.to_string().into_bytes();
    filter_bytes.hash(&mut hasher);
    let result = hasher.finish();
    let hash = format!("{:x}", result);
    hash
}
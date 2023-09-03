use std::{collections::hash_map::DefaultHasher, hash::{Hash, Hasher}};

use crate::handler::{Request, CommandExecutionError};
use bson::Document;
// use sha2::{Sha256, Digest};

pub mod find;
pub mod is_master;
pub mod ping;
pub mod hello;
pub mod get_parameters;
pub mod connection_status;
pub mod create;
pub mod insert;
pub mod build_info;
pub mod get_cmd_line_opts;
pub mod list_indexes;
pub mod list_collections;
pub mod list_databases;
// pub mod coll_stats;
// pub mod db_stats;
// pub mod count;
pub mod aggregate;

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

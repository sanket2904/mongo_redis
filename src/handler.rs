
#![allow(dead_code)]
use std::net::SocketAddr;

use bson::{Document, doc, Bson};

use crate::Wire::{OpCode, OP_MSG};

use crate::commands::Handler;


use crate::commands::is_master::IsMaster;
use crate::commands::ping::Ping;

 
pub struct Request<'a> {
    pub client: &'a mongodb::Client,
    pub peer_addr: std::net::SocketAddr,
    pub op_code: &'a OpCode,
    pub redis_client: &'a redis::Client,
}

impl <'a> Request<'a> {
    pub fn new(client: &'a mongodb::Client, peer_addr: std::net::SocketAddr, op_code: &'a OpCode , redis_client: &'a redis::Client) -> Request<'a> {
        return Request {
            client: client,
            peer_addr: peer_addr,
            op_code: op_code,
            redis_client: redis_client ,
        };
    }
    pub fn peer_addr(&self) -> std::net::SocketAddr {
        return self.peer_addr;
    }
    pub fn get_op_code(&self) -> &'a OpCode {
        return self.op_code;
    }
    pub fn get_redis_client(&self) -> &'a redis::Client {
        return self.redis_client;
    }
    pub fn get_mongo_client(&self) -> &'a mongodb::Client {
        return self.client;
    }
}

#[derive(Debug, Clone)]
pub struct CommandExecutionError {
    pub message: String,
}

impl std::error::Error for CommandExecutionError {}

impl std::fmt::Display for CommandExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl CommandExecutionError {
    pub fn new(message: String) -> Self {
        CommandExecutionError { message }
    }
}


// response struct

#[derive(Debug, Clone)]
pub struct Response<'a> {
    id: u32,
    op_code: &'a OpCode,
    docs: Vec<Document>,
}

impl <'a> Response<'a> {
    pub fn new(id: u32, op_code: &'a OpCode, docs: Vec<Document>) -> Self {
        Response { id, op_code, docs }
    }
    pub fn get_id(&self) -> u32 {
        return self.id;
    }
    pub fn get_op_code(&self) -> &'a OpCode {
        return self.op_code;
    }
    pub fn get_doc(&self) -> &Document {
        &self.docs[0]
    }
    
}


pub fn handle(id:u32, peer_addr: SocketAddr, op_code: &OpCode , mongo_client: mongodb::Client , redis_client: redis::Client) -> Result<Vec<u8>, CommandExecutionError> {


    let request = Request {
        client: &mongo_client,
        peer_addr: peer_addr,
        op_code: &op_code,
        redis_client: &redis_client,
    };

    match route(&request) {
        Ok(doc) => {
            let response = Response {
                id, op_code,docs: vec![doc],
            };
            Ok(op_code.reply(response).unwrap())
        }
        Err(e) => {
            println!("Error: {:?}", e);
            Err(e)
        }
    }
}

// route function 

fn route(request: &Request) -> Result<Document, CommandExecutionError> {
    match request.get_op_code() {
        // OpCode::OpMsg(op_msg) => op_msg.handle(request),
        OpCode::OpQuery(op_query) => run_op_query(request,  &vec![op_query.query.clone()]),
        OpCode::OpMsg(message) => handle_op_msg(request, message.clone()),
        _ => Err(CommandExecutionError::new("Unknown OpCode".to_string())),
    }
}


fn run_op_query(request: &Request, docs: &Vec<Document>) -> Result<Document, CommandExecutionError> {
    let empty = "".to_string();
    let command = docs[0].keys().next().unwrap_or(&empty);
    
    if command == "" || command == "isMaster" || command == "ismaster" {
        let is_master = IsMaster::new().handle(request, docs);
        return is_master;
    } else {
      
        Ok(doc! {
            "ok": Bson::Double(0.0),
            "errmsg": Bson::String(format!("no such command: '{}'", command).to_string()),
            "code": Bson::Int32(59),
            "codeName": "CommandNotFound",
        })
    }
}




fn  run(request: &Request, docs: &Vec<Document>) -> Result<Document, CommandExecutionError> {
    let command = docs[0].keys().next().unwrap();
    
    println!("command on line 148: {}", command);



    if command == "listDatabases" {
        return crate::commands::list_databases::ListDatabases::new().handle(request, docs);
    }
    else if command == "listCollections" {
        return crate::commands::list_collections::ListCollections::new().handle(request, docs);
    } else if command == "listIndexes" {
        return crate::commands::list_indexes::ListIndexes::new().handle(request, docs);
    } else if command == "getCmdLineOpts" {
        return crate::commands::get_cmd_line_opts::GetCmdLineOpts::new().handle(request, docs);
    } else if command == "buildInfo" || command == "buildinfo" {
        return crate::commands::build_info::BuildInfo::new().handle(request, docs);
    } else if command == "find" {
        return crate::commands::find::Find::new().handle(request, docs);
    } else if command == "insert" {
        return crate::commands::insert::Insert::new().handle(request, docs);
    } else if command == "create" {
        return crate::commands::create::Create::new().handle(request, docs);
    } else if command == "connectionStatus" {
        return crate::commands::connection_status::ConnectionStatus::new().handle(request, docs);
    } else if command == "getParameter"{
        return crate::commands::get_parameters::GetParameter::new().handle(request, docs);
    }else if command == "hello" {
        return crate::commands::hello::Hello::new().handle(request, docs);
    } else if command == "ping" {
        return Ping::new().handle(request, docs);
    } else if command == "ismaster" || command == "isMaster" {
        let is_master = IsMaster::new().handle(request, docs);
        return is_master;
    } else {
        println!("command not found");
        return Ok(doc! {
            "ok": Bson::Double(0.0),
            "errmsg": Bson::String(format!("no such command: '{}'", command).to_string()),
            "code": Bson::Int32(59),
            "codeName": "CommandNotFound",
        });
    }


    
}


fn handle_op_msg(request: &Request, msg: OP_MSG) -> Result<Document, CommandExecutionError> {
    if msg.sections.len() < 1 {
        
        return Err(CommandExecutionError::new(
            "OP_MSG must have at least one section, received none".to_string(),
        ));
    }

    let section = msg.sections[0].clone();
    if section.kind == 0 {
        let mut documents = section.documents.clone();
        if msg.sections.len() > 1 {
            for section in msg.sections[1..].iter() {
                if let Some(identifier) = section.identifier.clone() {
                    if identifier == "documents\0" {
                        let new_doc = section.documents[0].clone();
                        documents[0].insert("documents", Bson::Array(vec![new_doc.into()]));
                    }
                }
            }
        }
        return run(request, &documents);
    }

    if section.kind == 1 {
        if section.identifier.is_none() {
            
            return Err(CommandExecutionError::new(
                "all kind 1 sections on OP_MSG must have an identifier, received none".to_string(),
            ));
        }

        let mut identifier = section.identifier.unwrap();
        identifier.pop();

        if identifier == "documents" {
            if msg.sections.len() < 2 {
               
                return Err(CommandExecutionError::new(
                    "OP_MSG with a kind 1 documents section must also have at least one kind 0 section, received none".to_string(),
                ));
            }

            let mut doc = msg.sections[1].documents[0].clone();
            doc.insert(identifier, section.documents.clone());
            return run(request, &vec![doc]);
        }

        
        return Err(CommandExecutionError::new(
            format!(
                "received unknown kind 1 section identifier from OP_MSG: {}",
                identifier
            )
            .to_string(),
        ));
    }

    
    Err(CommandExecutionError::new(
        format!(
            "received unknown section kind from OP_MSG: {}",
            section.kind
        )
        .to_string(),
    ))
}

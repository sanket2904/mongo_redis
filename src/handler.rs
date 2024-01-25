use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::io::{Write, Read, Cursor};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Mutex, Arc};
use bson::{Document, doc, Bson};
use byteorder::{LittleEndian, ByteOrder};
use crate::storage;
use crate::Wire::{OpCode, OP_MSG,  HEADER_SIZE};
use crate::commands::{Handler, hash};
use crate::commands::is_master::IsMaster; 



pub type Storage = std::sync::Arc<Mutex<HashMap<String, InnerData>>>;

pub struct Request<'a,'b> {
    pub client: Arc<Mutex<rustls::Stream<'b,rustls::ClientConnection, TcpStream>>>,
    pub peer_addr: std::net::SocketAddr,
    pub op_code: &'a OpCode,
    pub storage: &'a Storage,
}

pub enum InnerData {    // the data could be document or queue of documents
    Document(Document),
    Documents(Vec<Document>),
}

impl <'a,'b> Request<'a,'b> {
    pub fn new(client: Arc<Mutex<rustls::Stream<'b, rustls::ClientConnection, TcpStream>>>, peer_addr: std::net::SocketAddr, op_code: &'a OpCode , storage:&'a  Storage) -> Request<'a,'b> {
        return Request {
            client,
            peer_addr: peer_addr,
            op_code: op_code,
            // redis_client: redis_client ,
            storage: storage,
        };
    }
    pub fn peer_addr(&self) -> std::net::SocketAddr {
        return self.peer_addr;
    }
    pub fn get_op_code(&self) -> &'a OpCode {
        return self.op_code;
    }
    pub fn get_storage(&self) -> &'a  Storage {
        return self.storage;
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
#[derive(Debug, Clone)]
pub struct Response<'a> {
    pub id: u32,
    pub op_code: &'a OpCode,
    pub docs: Vec<Document>,
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


pub fn handle(id:u32, peer_addr: SocketAddr, op_code: & OpCode , mongo_client: Arc<Mutex<rustls::Stream<rustls::ClientConnection, TcpStream>>> , storage: & Storage) -> Result<Vec<u8>, CommandExecutionError> {


    // let opcode = op_code.clone();
    let request = Request {
        client:  mongo_client,
        op_code: op_code,
        storage,
        peer_addr: peer_addr,
    };

    match route(&request) {
        Ok(doc) => {
            let response = Response {
                id, op_code: &op_code, docs: vec![doc],
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

fn route(request: &Request<'_, '_>) -> Result<Document, CommandExecutionError> {
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
    println!("command is: {}", command);
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




fn run(request: & Request<'_,'_>, docs: &Vec<Document>) -> Result<Document, CommandExecutionError> {
    let command = docs[0].keys().next().unwrap();
    println!("command is: {}", command);
    println!("docs: {:?}", docs[0]);
    if command == "find" {
        let time = std::time::SystemTime::now();
        let doc: &Document = &docs[0];
        let filter = if doc.contains_key("filter") {
            doc.get_document("filter").unwrap().clone()
        } else {
            doc! {}
        };
        let collection_name = doc.get_str("find").unwrap();
        println!("collection_name: {}", collection_name);
        let storage = request.get_storage();
        let hash = hash(format!("{}{}", collection_name, filter));
        let mut st = storage.lock().unwrap();
        let data = st.get(&hash);
        if data.is_none() {
            println!("not found in cache");
            let response_to_server = request.client.clone();
            let res = Response::new(0, request.op_code, docs.to_vec());
            let res: Vec<u8> = request.op_code.reply(res).unwrap();
            let mut lock = response_to_server.lock().unwrap();
            lock.write_all(&res).unwrap();
            lock.flush().unwrap();
            let mut size_buffer:Vec<u8> = vec![0;4];
            lock.read_exact(&mut size_buffer).unwrap();
            let mut size = LittleEndian::read_i32(&size_buffer);
            size = size - size_buffer.len() as i32;
            let mut buffer:Vec<u8> = vec![0;size as usize];
            lock.read_exact(&mut buffer).unwrap();
            let buffer = [size_buffer, buffer].concat();
            let mut cursor = Cursor::new(buffer);
            cursor.set_position((HEADER_SIZE + 5 as u32).into());
            let document = Document::from_reader(cursor).unwrap();
            st.insert(hash, InnerData::Document(document.clone()));
            return Ok(document);
        }
        else {
            println!("found in cache");
            if let InnerData::Document(data) = data.unwrap() {
                println!("time: {:?}", time.elapsed().unwrap());
                return Ok(data.clone());
            }
            else {
                panic!("data is not a document");
            }
        }
    }
    else if command == "getMore" {

        let doc = &docs[0];
        let collection_name = doc.get_str("collection").unwrap();
        let cursor_id = doc.get_i64("getMore").unwrap();
        let storage = request.get_storage();
        let hashh = hash(format!("{}{}.false", collection_name, cursor_id));
        let mut st = storage.lock().unwrap();
        let data = st.get(&hashh);
        match data {
            Some(data) => {
                if let InnerData::Documents(data) = data {
                    let mut documents = data.to_vec();
                    st.remove(&hashh);
                    // send the getMore request to the server
                    let response_to_server = request.client.clone();
                    let res = Response::new(0, request.op_code, docs.to_vec());
                    let res: Vec<u8> = request.op_code.reply(res).unwrap();
                    let mut lock = response_to_server.lock().unwrap();
                    lock.write_all(&res).unwrap();
                    lock.flush().unwrap();
                    // read the response from the server
                    let mut size_buffer:Vec<u8> = vec![0;4];
                    lock.read_exact(&mut size_buffer).unwrap();
                    let mut size = LittleEndian::read_i32(&size_buffer);
                    size = size - size_buffer.len() as i32;
                    let mut buffer:Vec<u8> = vec![0;size as usize];
                    lock.read_exact(&mut buffer).unwrap();
                    let buffer = [size_buffer, buffer].concat();
                    let mut cursor = Cursor::new(buffer);
                    cursor.set_position((HEADER_SIZE + 5 as u32).into());
                    let document = Document::from_reader(cursor).unwrap();
                    let cursor_id_server = document.get_document("cursor").unwrap().get_i64("id").unwrap();
                    if cursor_id_server == cursor_id {
                        let doc = document.clone();
                        documents.push(doc.clone());
                        st.insert(hashh, InnerData::Documents(documents.to_vec()));
                        return Ok(doc);
                    }
                    else if cursor_id_server == 0 {
                        let new_hash = hash(format!("{}{}.true", collection_name, cursor_id));
                        documents.push(document.clone());
                        st.insert(new_hash, InnerData::Documents(documents));
                        return Ok(document);
                    }
                };
            },
            None => {
                let new_hash = hash(format!("{}{}.true", collection_name, cursor_id));
                let check = st.remove(&new_hash);
                match check {
                    Some(data) => {
                        println!("found in cache");
                        if let InnerData::Documents(data_vec)  = data {
                            let time = std::time::SystemTime::now();
                            let mut docs = data_vec;
                            let first = docs.remove(0);
                            docs.push(first.clone());
                            st.insert(new_hash, InnerData::Documents(docs));
                            println!("time: {:?}", time.elapsed().unwrap());
                            return Ok(first);
                        }
                    },
                    None => {
                        let response_to_server = request.client.clone();
                        let res = Response::new(0, request.op_code, docs.to_vec());
                        let res: Vec<u8> = request.op_code.reply(res).unwrap();
                        let mut lock = response_to_server.lock().unwrap();
                        lock.write_all(&res).unwrap();
                        lock.flush().unwrap();
                        // read the response from the server
                        let mut size_buffer:Vec<u8> = vec![0;4];
                        lock.read_exact(&mut size_buffer).unwrap();
                        let mut size = LittleEndian::read_i32(&size_buffer);
                        size = size - size_buffer.len() as i32;
                        let mut buffer:Vec<u8> = vec![0;size as usize];
                        lock.read_exact(&mut buffer).unwrap();
                        let buffer = [size_buffer, buffer].concat();
                        let mut cursor = Cursor::new(buffer);
                        cursor.set_position((HEADER_SIZE + 5 as u32).into());
                        let document = Document::from_reader(cursor).unwrap();
                        let doc = document.clone();
                        let cursor_id_server = document.get_document("cursor").unwrap().get_i64("id").unwrap();
                        if cursor_id_server == cursor_id {
                            let doc = document.clone();
                            st.insert(hashh, InnerData::Documents(vec![doc.clone()]));
                            return Ok(doc);
                        }
                        else if cursor_id_server == 0 {
                            st.remove(&hashh);
                            st.insert(new_hash, InnerData::Documents(vec![doc.clone()]));
                            return Ok(doc);
                        }
                        return Ok(doc);
                    }
                }
            }
        }
        Ok(doc! {})
    }
    else {
        let response_to_server = request.client.clone();
        let res = Response::new(0, request.op_code, docs.to_vec());
        let res: Vec<u8> = request.op_code.reply(res).unwrap();
        let mut lock = response_to_server.lock().unwrap();
        lock.write_all(&res).unwrap();
        lock.flush().unwrap();
        // read the response from the server
        let mut size_buffer:Vec<u8> = vec![0;4];
        lock.read_exact(&mut size_buffer).unwrap();
        let mut size = LittleEndian::read_i32(&size_buffer);
        size = size - size_buffer.len() as i32;
        let mut buffer:Vec<u8> = vec![0;size as usize];
        lock.read_exact(&mut buffer).unwrap();
        let buffer = [size_buffer, buffer].concat();
        let mut cursor = Cursor::new(buffer);
        cursor.set_position((HEADER_SIZE + 5 as u32).into());
        let document = Document::from_reader(cursor).unwrap();
        let doc = document.clone();
        Ok(doc)
    }    
}




fn handle_op_msg(mut request: &Request<'_,'_>, msg: OP_MSG) -> Result<Document, CommandExecutionError> {
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
        return run(&mut request, &documents);
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
            return run(&mut request, &vec![doc]);
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

use bson::{doc, Bson, Document};
use byteorder::{ByteOrder, LittleEndian};
use std::{
    env,
    io::{Read, Write},
    net::{TcpListener, TcpStream}, sync::{Arc, Mutex}, collections::HashMap,
};
// use tokio::io::{AsyncWriteExt, AsyncReadExt};
use threadpool::ThreadPool;
pub mod Wire;
pub mod commands;
pub mod handler;
pub mod mongo;
pub mod rd;
pub mod storage;
pub mod utils;
// use storage::Storage;

// initial test

// type Storage = std::sync::Arc<Mutex<HashMap<String,bson::Document>>>;


fn main() {
    fn start(listen_addr: Option<String>, port: Option<u16>) {
        let ip_addr = listen_addr
            .unwrap_or(env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0".to_string()));
        let port = port.unwrap_or(
            env::var("OXIDE_PORT")
                .unwrap_or("27017".to_string())
                .parse()
                .unwrap(),
        );
        let mongo_url = env::var("MONGO_URI").ok();
        if mongo_url.is_none() {
            panic!("Mongo uri not found");
        }
        let mongo_client = mongo::MongoDb::new().db;
        // let storage = storage::Storage::new();
        start_main(ip_addr, port, mongo_client);
    }
    let listen_addr = "0.0.0.0";
    let port = 27017;
    start(Some(listen_addr.to_string()), Some(port));
}



pub fn start_main(listen_addr: String, port: u16 , mongo_client:  mongodb::sync::Client ) {
    println!("Starting server...");
    let listner = TcpListener::bind(format!("{}:{}", listen_addr, port)).unwrap();
    let pool = ThreadPool::new(100);
    let storage = storage::Storage::new();
    let mongo_client = Arc::new(mongo_client);
    println!("Server started on port {}", port);
    for stream in listner.incoming() {
        let stream = stream.unwrap();
        stream.set_nodelay(true).unwrap();
        println!("New connection: {}", stream.peer_addr().unwrap());
        let mongo_client = Arc::clone(&mongo_client);
        let storage = storage.clone();
        pool.execute(move || {
            handle_connection(stream , &mongo_client , &storage.data);
        });
    }
    println!("Shutting down server");
}

fn handle_connection(
    mut stream: TcpStream,
    mongo_client: &mongodb::sync::Client,
    storage: &Arc<Mutex<HashMap<String,Document>>>,
) {
    // need to possibly use request id here
    let addr = stream.peer_addr().unwrap();
    println!("Client connected: {}", addr);
    loop {
        let mut size_buffer = [0; 4];
        stream.peek(&mut size_buffer).unwrap();
        let size = LittleEndian::read_i32(&size_buffer);
        println!("Size: {}", size);
        if size == 0 {
            stream.flush().unwrap();
            println!("Client disconnected: {}", addr);
            break;
        }
        let mut buffer = vec![0; size as usize];
        match stream.read_exact(&mut buffer) {
            Ok(_read) => {
                

                let op_code = Wire::parse(&buffer);
                if op_code.is_err() {
                    println!("Error: {:?}", op_code);
                    stream.write(&[0x00, 0x00, 0x00, 0x00]).unwrap();
                    stream.write(&[0x00, 0x00, 0x00, 0x00]).unwrap();
                    stream.write(&[0x00, 0x00, 0x00, 0x00]).unwrap();
                    stream.write(&[0x00, 0x00, 0x00, 0x00]).unwrap();
                    return;
                }
                let op_code = op_code.unwrap();
                let mongo_client = mongo_client.clone();
                let storage = storage.clone();
                // let redis_client = redis_client.clone();

                let mut response =
                    match handler::handle(0, addr, &op_code, mongo_client, &storage) {
                        Ok(reply) => reply,
                        Err(e) => {
                            println!("Error: {}", e);
                            let err = doc! {
                                "ok": Bson::Double(0.0),
                                "errmsg": Bson::String(format!("{}", e)),
                                "code": Bson::Int32(59),
                                "codeName": "CommandNotFound",
                            };
                            let request = handler::Response::new(0, &op_code, vec![err]);
                            op_code.reply(request).unwrap()
                        }
                    };

                println!("Response size {}", response.len());
                response.flush().unwrap();
                stream.write_all(&response).unwrap();
            }
            Err(e) => {
                println!("Error: {}", e);
                return;
            }
        }
    }
}

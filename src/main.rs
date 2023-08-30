use std::{net::{TcpListener, TcpStream}, io::{Read,  Write},  env, thread};
use byteorder::{LittleEndian,  ByteOrder};
use bson::{Bson,  doc };
pub mod mongo;
pub mod rd;
pub mod handler;
pub mod Wire;
pub mod utils;
pub mod commands;
#[tokio::main]
async fn main() {
    async fn start(listen_addr: Option<String> , port: Option<u16>)  {
        let ip_addr = listen_addr.unwrap_or(env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0".to_string()));
        let port = port.unwrap_or(env::var("OXIDE_PORT").unwrap_or("27017".to_string()).parse().unwrap());
        let mongo_url = env::var("MONGO_URI").ok();
        let redis_url = env::var("REDIS_URI").ok();
        if mongo_url.is_none() {
            panic!("Mongo uri not found");
        }
        if redis_url.is_none() {
            panic!("Redis uri not found");
        }
        let mongo_client = mongo::MongoDb::new().await.db;
        let redis_client = rd::RedisDb::new().db;
        Server::new(ip_addr, port, mongo_client, redis_client).start();
    }
    let listen_addr = "127.0.0.1";
    let port = 27017;
    start(Some(listen_addr.to_string()), Some(port)).await;
}

pub struct Server {
    listen_addr: String,
    port: u16,
    mongo_client: mongodb::Client,
    redis_client: redis::Client,
}

impl Server {
    pub fn new(listen_addr: String, port: u16 , mongo_client: mongodb::Client , redis_client: redis::Client) -> Self {
        Server {
            listen_addr,
            port,
            mongo_client,
            redis_client,
        }
    }
    pub fn start(&self) {
        println!("Starting server...");
        let listner = TcpListener::bind(format!("{}:{}", self.listen_addr, self.port)).unwrap();
        println!("Server started on port {}", self.port);
        for stream in listner.incoming() {
            let stream = stream.unwrap();
            let mongo_client = self.mongo_client.clone();
            let redis_client = self.redis_client.clone();
            println!("New connection: {}", stream.peer_addr().unwrap());
            let rt = tokio::runtime::Runtime::new().unwrap();
            thread::spawn( move || {
                rt.block_on(
                    handle_connection(stream, mongo_client, redis_client)
                )
            });
        }
        println!("Shutting down server");
    }
}

async fn handle_connection(mut stream: TcpStream , mongo_client: mongodb::Client , redis_client: redis::Client) {  // need to possibly use request id here
    let addr = stream.peer_addr().unwrap();
    println!("Client connected: {}", addr);
    loop {
        let mut size_buffer = [0;4];
        let _read = stream.peek(&mut size_buffer).unwrap();
        let size = LittleEndian::read_i32(&size_buffer);
        if size == 0 {
            stream.flush().unwrap();
            break;
        }
        let mut buffer = vec![0; size as usize];
        match stream.read_exact(&mut buffer) {
            Ok(_read) => {
                use std::time::Instant;
                let now = Instant::now();
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
                let redis_client = redis_client.clone();
                let mut response = match handler::handle(0, addr, &op_code, mongo_client, redis_client).await {
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
                response.flush().unwrap();
                stream.write_all(&response).unwrap();
            }
            Err(e) => {
                println!("Error: {}", e);
                break;
            }
        }
    }
}
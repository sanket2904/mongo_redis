use bson::{doc, Bson, Document};
use byteorder::{ByteOrder, LittleEndian};
use rustls::{pki_types::ServerName, ClientConfig, RootCertStore};
use std::{
    collections::HashMap,
    env,
    io::{Cursor, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
};
use trust_dns_resolver::{
    config::{ResolverConfig, ResolverOpts},
    Resolver,
};
// use tokio::io::{AsyncWriteExt, AsyncReadExt};
use threadpool::ThreadPool;

use crate::Wire::{MsgHeader, Op_msg::Section, HEADER_SIZE, OP_MSG};
pub mod Wire;
pub mod commands;
pub mod handler;

pub mod storage;
pub mod utils;

fn main() {
    start_main("127.0.0.1".to_string(), 27017);
}

fn find_primary(mongouri: &str) -> String {
    let resolver = Resolver::new(ResolverConfig::default(), ResolverOpts::default()).unwrap();
    let uri = mongouri.split("://").collect::<Vec<&str>>()[1]
        .split("@")
        .collect::<Vec<&str>>()[1]
        .split("/")
        .collect::<Vec<&str>>()[0];
    println!("URI: {}", uri);
    let response = resolver
        .srv_lookup(format!("_mongodb._tcp.{}", uri))
        .unwrap();
    let ip: &trust_dns_resolver::proto::rr::rdata::SRV = response.iter().next().unwrap();
    let main_ip = ip.target().to_string();
    let dns_name = ServerName::try_from(main_ip.clone()).unwrap();
    // remove the last dot and add the port
    let main_ip = format!(
        "{}:{}",
        main_ip[0..main_ip.len() - 1].to_string(),
        ip.port()
    );
    // now find the primary shard
    let mut root_cert_store = RootCertStore::empty();
    root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = ClientConfig::builder()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth();
    let arc = std::sync::Arc::new(config);
    println!("Main IP: {}", main_ip);
    let mut client: rustls::ClientConnection =
        rustls::ClientConnection::new(arc.clone(), dns_name).unwrap();
    let mut server: TcpStream = TcpStream::connect(main_ip).unwrap();
    let mut mongo_client: rustls::Stream<'_, rustls::ClientConnection, TcpStream> =
        rustls::Stream::new(&mut client, &mut server);
    // send a ismaster command
    let mut op_msg = OP_MSG {
        header: MsgHeader {
            msg_length: 0,
            request_id: 0,
            response_to: 0,
            op_code: 2013,
        },
        flags: 0,
        sections: vec![Section {
            kind: 0,
            identifier: None,
            documents: vec![doc! {
                "ismaster": Bson::Int32(1),
                "$db": Bson::String("admin".to_string()),
            }],
        }],
        checksum: None,
    };
    op_msg.header.msg_length = op_msg.to_vec().len() as u32;
    let t = op_msg.to_vec();
    mongo_client.write_all(&t).unwrap();
    let mut size_buffer: Vec<u8> = vec![0; 4];
    // wait for the server to send a response
    mongo_client.read_exact(&mut size_buffer).unwrap();
    let mut size = LittleEndian::read_i32(&size_buffer);
    println!("Size: {}", size);
    size = size - size_buffer.len() as i32;
    let mut buffer = vec![0; size as usize];
    println!("Buffer size: {}", buffer.len());
    mongo_client.read_exact(&mut buffer).unwrap();
    let buffer = [size_buffer, buffer].concat();
    let mut cursor = Cursor::new(buffer);
    cursor.set_position((HEADER_SIZE + 5 as u32).into());
    let document = Document::from_reader(cursor).unwrap();
    let primary = document
        .get("primary")
        .unwrap()
        .as_str()
        .unwrap()
        .to_owned();
    primary
}

pub fn start_main(listen_addr: String, port: u16) {
    println!("Starting server...");
    let listner = TcpListener::bind(format!("{}:{}", listen_addr, port)).unwrap();
    let pool = ThreadPool::new(4);
    let storage = Arc::new(Mutex::new(HashMap::new()));
    let mongouri = env::var("MONGO_URI").ok();
    if mongouri.is_none() {
        panic!("Mongo uri not found");
    }
    let addr = find_primary(mongouri.unwrap().as_str());
    let addr = Arc::new(addr);
    println!("Server started on port {}", port);
    for stream in listner.incoming() {
        let stream = stream.unwrap();
        stream.set_nodelay(true).unwrap();
        println!("New connection: {}", stream.peer_addr().unwrap());
        // get mongodb uri from env
        let mut root_cert_store = RootCertStore::empty();
        root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let config = ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();
        let arc = std::sync::Arc::new(config);
        let a = addr.clone().split(":").collect::<Vec<&str>>()[0].to_string();
        let dns_name = ServerName::try_from(a).unwrap();
        // let mongo_client = Arc::clone(&mongo_client);
        let storage: Arc<Mutex<HashMap<String, crate::handler::InnerData>>> = storage.clone();
        let addr = addr.clone();
        pool.execute(move || {
            let client: rustls::ClientConnection =
                rustls::ClientConnection::new(arc.clone(), dns_name).unwrap();
            let server: TcpStream = TcpStream::connect(addr.to_string()).unwrap();
            // let mut tcp_out_stream: rustls::Stream<'static, rustls::ClientConnection, TcpStream> = rustls::Stream::new(&mut client, &mut server);
            handle_connection(stream, client, server, &storage);
        });
    }
    println!("Shutting down server");
}

fn handle_connection(
    mut stream: TcpStream,
    mut client: rustls::ClientConnection,
    mut server: TcpStream,
    storage: &crate::handler::Storage,
) {
    // need to possibly use request id here
    let addr = stream.peer_addr().unwrap();
    println!("Client connected: {}", addr);
    let mongo_client: rustls::Stream<'_, rustls::ClientConnection, TcpStream> =
        rustls::Stream::new(&mut client, &mut server);
    // throw error if tls handshake fails
    let mongo_client: Arc<Mutex<rustls::Stream<'_, rustls::ClientConnection, TcpStream>>> =
        Arc::new(Mutex::new(mongo_client));
    loop {
        let mut size_buffer = [0; 4];
        stream.peek(&mut size_buffer).unwrap();
        let size = LittleEndian::read_i32(&size_buffer);
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
                let mongo_client = Arc::clone(&mongo_client);
                let storage = storage.clone();
                let mut response = match handler::handle(0, addr, &op_code, mongo_client, &storage)
                {
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
                return;
            }
        }
    }
}

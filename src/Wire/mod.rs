use std::io::{Cursor, Read};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

pub mod Op_msg;
pub mod Op_query;
pub mod Op_reply;
pub mod util;
use crate::handler::Response;

pub use self::Op_msg::OP_MSG;
pub use self::Op_query::OP_QUERY;
pub use self::Op_reply::OP_REPLY;


pub const OP_MSG: u32 = 2013;
pub const OP_REPLY: u32 = 1;
pub const OP_QUERY: u32 = 2004;

pub const MAX_DOCUMENT_LEN: u32 = 16777216;
pub const MAX_MSG_LEN: u32 = 48000000;
pub const HEADER_SIZE: u32 = 16;

pub const CHECKSUM_PRESENT: u32 = 1 << 0;
pub const MORE_TO_COME: u32 = 1 << 1;
pub const EXHAUST_ALLOWED: u32 = 1 << 16;



#[derive(Debug ,Clone)]
pub struct MsgHeader {
    pub msg_length: u32,
    pub request_id: u32,
    pub response_to: u32,
    pub op_code: u32,

}

#[derive(Debug, Clone)]
pub struct OpCodeNotImplementedError {
    op_code: u32,
}

pub trait Serializable {
    fn to_vec(&self) -> Vec<u8>;
}
pub trait Replyable {
    fn reply(&self, response: Response) -> Result<Vec<u8>, UnknownMessageKindError>
    where
        Self: Sized;
}


pub fn parse(buffer: &[u8]) -> Result<OpCode, OpCodeNotImplementedError>  {
    let mut cursor = Cursor::new(buffer);
    let header = MsgHeader::parse(&mut cursor);
    if header.op_code == OP_MSG {
        let mut msg_buffer: Vec<u8> = vec![0; header.msg_length as usize ];
        cursor.set_position(0);
        cursor.read_exact(&mut msg_buffer).unwrap();
        Ok(OpCode::OpMsg(OP_MSG::from_bytes(&msg_buffer).unwrap()))
    } else if header.op_code == OP_QUERY {
        Ok(OpCode::OpQuery(OP_QUERY::parse(header,&mut cursor)))
    } else {
        Err(OpCodeNotImplementedError {
            op_code: header.op_code,
        })
    }
}

impl MsgHeader {
    pub fn parse(cursor: &mut Cursor<&[u8]>) -> MsgHeader {
        let message_length = cursor.read_u32::<LittleEndian>().unwrap();
        let request_id = cursor.read_u32::<LittleEndian>().unwrap();
        let response_to = cursor.read_u32::<LittleEndian>().unwrap();
        let op_code = cursor.read_u32::<LittleEndian>().unwrap();
        return MsgHeader {
            msg_length: message_length,
            request_id: request_id,
            response_to: response_to,
            op_code: op_code,
        };
    }
    pub fn get_response(&self, request_id: u32, message_length: u32) -> MsgHeader {
        self.get_response_with_op_code(request_id, message_length, self.op_code)
    }
    pub fn get_response_with_op_code(
        &self,
        request_id: u32,
        message_length: u32,
        op_code: u32,
    ) -> MsgHeader {
        return MsgHeader {
            msg_length: message_length,
            request_id: request_id,
            response_to: self.request_id,
            op_code: op_code,
        };
    }

    fn new(msg_length: u32, request_id: u32, response_to: u32, op_code: u32) -> MsgHeader {
        return MsgHeader {
            msg_length: msg_length,
            request_id: request_id,
            response_to: response_to,
            op_code: op_code,
        };
    }
    pub fn from_bytes(bytes: Vec<u8>) -> Result<MsgHeader, UnknownMessageKindError> {
        let mut cursor = Cursor::new(bytes);
        let message_length = cursor.read_u32::<LittleEndian>().unwrap();
        let request_id = cursor.read_u32::<LittleEndian>().unwrap();
        let response_to = cursor.read_u32::<LittleEndian>().unwrap();
        let op_code = cursor.read_u32::<LittleEndian>().unwrap();
        Ok(
            MsgHeader::new(message_length, request_id, response_to, op_code),
        )
    }
    fn to_vec(&self) -> Vec<u8> {
        let mut cursor = Cursor::new(Vec::new());
        cursor.write_u32::<LittleEndian>(self.msg_length).unwrap();
        cursor.write_u32::<LittleEndian>(self.request_id).unwrap();
        cursor.write_u32::<LittleEndian>(self.response_to).unwrap();
        cursor.write_u32::<LittleEndian>(self.op_code).unwrap();
        return cursor.into_inner();
    }
}


#[derive(Debug, Clone)]
pub enum OpCode {
    OpMsg(OP_MSG),
    OpQuery(OP_QUERY),
    OpReply(OP_REPLY),
}
#[derive(Debug, Clone)]
pub struct UnknownMessageKindError;

impl OpCode {
    pub fn reply(&self, response: Response) -> Result<Vec<u8> ,UnknownMessageKindError > {
       
        match self {
            OpCode::OpMsg(op_msg) =>Ok(op_msg.reply(response).unwrap()),
            OpCode::OpQuery(op_query) => Ok(op_query.reply(response).unwrap()),
            _ => Err(UnknownMessageKindError),
        }
    }
}






use crate::handler::Response;

use crate::Wire::Replyable;
use crate::Wire::{OpCode, UnknownMessageKindError, CHECKSUM_PRESENT, HEADER_SIZE};
use bson::{ ser,  Document};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
// use pretty_hex::pretty_hex;

use std::io::{ Cursor, Read, Write};
use std::vec;
use super::{MsgHeader, Serializable};
use super::util::parse_section;

#[derive(Debug, Clone, PartialEq)]
pub struct Section {
    pub kind: u8,
    pub identifier: Option<String>,
    pub documents: Vec<Document>,
}

#[derive(Debug, Clone)]
pub struct OP_MSG {
    pub header: MsgHeader,
    pub flags: u32,
    pub sections: Vec<Section>,
    pub checksum: Option<u32>,
}

impl Section {
    pub fn from_bytes(mut bytes: Vec<u8>) -> Result<(Section, Vec<u8>) , UnknownMessageKindError> {
        parse_section(&mut bytes)
    }
}

impl OP_MSG {
    pub fn new_with_body_kind(header: MsgHeader, flags: u32 , checksum: Option<u32> , doc: &Document) -> OP_MSG {
        OP_MSG { header, flags, sections:vec![Section {
            kind: 0,
            identifier: None,
            documents: vec![doc.to_owned()],
        }] , checksum}
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<OP_MSG, UnknownMessageKindError> {
        let mut cursor = Cursor::new(bytes);
        let mut header_buffer: Vec<u8> = vec![0u8; HEADER_SIZE as usize];
        cursor.read_exact(&mut header_buffer).unwrap();
        let header = MsgHeader::from_bytes(header_buffer).unwrap();
        let flags = cursor.read_u32::<LittleEndian>().unwrap();
        let mut bytes = vec![];
        cursor.read_to_end(&mut bytes).unwrap();
        let mut sections = vec![];
        loop {
            let (section, remaining) = parse_section(&mut bytes).unwrap();
            bytes = remaining;
            sections.push(section);
            if bytes.is_empty() {
                break;
            }
            if (bytes.len() as u64) <= 4 {
                break;
            }
        }
        let mut checksum = None;
        if flags & CHECKSUM_PRESENT != 0 {
            checksum = Some(cursor.read_u32::<LittleEndian>().unwrap());
        }
        Ok(OP_MSG { header, flags, sections, checksum })
    }
}




impl Replyable for OP_MSG {
    fn reply(&self, response: Response) -> Result<Vec<u8>, UnknownMessageKindError>
    {
        let bson_vec = ser::to_vec(&response.get_doc()).unwrap();
        let bson_data: &[u8] = &bson_vec;
        let message_length = HEADER_SIZE + 5 + bson_data.len() as u32;

        if let OpCode::OpMsg(op_msg) = response.get_op_code().to_owned() {
            let header = op_msg.header.get_response(response.get_id(), message_length);
            if self.sections.len() > 0 && self.sections[0].kind == 0 {
                return Ok(
                    OP_MSG::new_with_body_kind(header, self.flags, self.checksum, response.get_doc()).to_vec()
                );
            } else if self.sections.len() > 0 && self.sections[0].kind == 1 {
                return Ok(
                    OP_MSG::new_with_body_kind(header, self.flags, self.checksum, response.get_doc()).to_vec()
                );
            } else {
                return Err(UnknownMessageKindError);
            }
        }
        Err(UnknownMessageKindError)

    }
}




impl Serializable for OP_MSG {
    fn to_vec(&self) -> Vec<u8> {
        let mut writer = Cursor::new(Vec::new());
        writer.write_all(&self.header.to_vec()).unwrap();
        writer.write_u32::<LittleEndian>(self.flags).unwrap();
        for section in &self.sections {
            writer.write(&[section.kind]).unwrap();
            for doc in &section.documents {
                let bson_vec = ser::to_vec(&doc).unwrap();
                let bson_data: &[u8] = &bson_vec;
                writer.write(bson_data).unwrap();
            }
        }
        if (self.flags & CHECKSUM_PRESENT) != 0 {
            writer.write_u32::<LittleEndian>(self.checksum.unwrap()).unwrap();
        }
        writer.into_inner()
    }
}
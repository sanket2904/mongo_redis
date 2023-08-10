


use crate::Wire::Op_msg::Section;
use bson::{ser, Document};
use byteorder::{LittleEndian, ReadBytesExt};
// use indoc::indoc;
use std::io::{BufRead, Cursor, Read};
use crate::utils::to_cstring;
use super::UnknownMessageKindError;


pub fn parse_section(bytes: &mut Vec<u8>) -> Result<(Section,Vec<u8>),UnknownMessageKindError> {
    // parse section based on kind
    let kind = bytes[0];
    match kind {
        0 => Ok(parse_kind0(bytes.to_vec())),
        1 => Ok(parse_kind1(bytes.to_vec())),
        _ => Err(UnknownMessageKindError{}),
    }
}

fn parse_kind0(bytes: Vec<u8>) -> (Section,Vec<u8>) {
    let mut cursor = Cursor::new(bytes);
    let kind = cursor.read_u8().unwrap();
    let mut new_cursor = cursor.clone();
    let document = Document::from_reader(&mut cursor).unwrap();
    let bson_vec = ser::to_vec(&document).unwrap();
    new_cursor.set_position(new_cursor.position() + bson_vec.len() as u64);
    let mut tail: Vec<u8> = vec![];
    new_cursor.read_to_end(&mut tail).unwrap();

    (
        Section {
            kind,
            identifier: None,
            documents: vec![document],
        },
        tail,
    
    )
}

fn parse_kind1_documents(data: &[u8]) -> Vec<Document> {
    let size = data.len();
    let mut cursor = Cursor::new(data);
    let mut documents = Vec::new();

    let mut read_size = 0;
    while read_size < size as usize {
        let doc_cursor = cursor.clone();
        let document = Document::from_reader(doc_cursor).unwrap();
        let bson_vec = ser::to_vec(&document).unwrap();
        let document_size = bson_vec.len();
        cursor.set_position(cursor.position() + document_size as u64);

        documents.push(document);
        read_size += document_size;
    }

    return documents;
}

fn parse_kind1(bytes: Vec<u8>) -> (Section, Vec<u8>) {
    let mut cursor = Cursor::new(bytes);

    // session kind
    let kind = cursor.read_u8().unwrap();

    // session contents size
    let size = cursor.read_u32::<LittleEndian>().unwrap();

    // identifier
    let mut identifier_buffer: Vec<u8> = vec![];
    cursor.read_until(0, &mut identifier_buffer).unwrap();
    let identifier_size: u32 = identifier_buffer.len() as u32;
    let identifier = to_cstring(identifier_buffer);

    // whole section = size - sizeof(size) - sizeof(identifier)
    //                 size - 4 - len(identifier_buffer)
    let remaining_size: u32 = size - 4 - identifier_size;
    let mut section_buffer: Vec<u8> = vec![0u8; remaining_size as usize];
    cursor.read_exact(&mut section_buffer).unwrap();

    let documents = parse_kind1_documents(&section_buffer);
    let mut tail: Vec<u8> = vec![];
    cursor.read_to_end(&mut tail).unwrap();

    (
        Section {
            kind,
            identifier: Some(identifier),
            documents,
        },
        tail,
    )
}
#![crate_name = "sequencefile"]

extern crate byteorder;
extern crate flate2;

use std::collections::HashMap;

macro_rules! to_opt {
    ($e:expr) => (match $e {
        Ok(val) => val,
        Err(_) => return None,
    });
}

pub type ByteString = Vec<u8>;

// Sequencefile header
#[derive(Debug)]
pub struct Header {
    version: u16,
    compression_type: CompressionType,
    compression_codec: Option<String>,
    key_class: String,
    value_class: String,
    metadata: HashMap<String, String>,
    sync_marker: ByteString,
}

#[derive(Debug, PartialEq)]
pub enum CompressionType {
    None,
    Record,
    Block,
}

// modules
mod errors;
pub mod reader;

// exports
pub use reader::*;

#![crate_name = "sequencefile"]
extern crate byteorder;
extern crate flate2;

use std::io;
use std::result;
use std::collections::HashMap;
use std::str;
use std::path::Path;
use std::fs::File;

use byteorder::{ByteOrder, BigEndian};

macro_rules! to_opt {
    ($e:expr) => (match $e {
        Ok(val) => val,
        Err(_) => return None,
    });
}

pub type ByteString = Vec<u8>;

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
    Value,
    Block,
}

// modules
mod errors;
pub mod reader;

// exports
pub use reader::*;

pub fn main_read(filename: &str) {
    let path = Path::new(filename);
    let display = path.display();

    info!("Opening {}", display);

    let file = File::open(&path).unwrap();
    let seqfile = Reader::new(file);

    let kvs = seqfile.map(|(key, value)| {
        (BigEndian::read_i64(&key),
         String::from_utf8_lossy(&value[0..value.len()]).to_string())
    });

    for kv in kvs {
        println!("{:?}", kv);
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    extern crate env_logger;
    // #[test]
    // fn it_works() {
    //     let _ = env_logger::init();
    //
    //     info!("Test...");
    //
    //     main_read("test.seq")
    // }


    #[test]
    fn it_works_with_zlib() {
        let _ = env_logger::init();

        info!("Test...");

        main_read("518.seq")
    }
}

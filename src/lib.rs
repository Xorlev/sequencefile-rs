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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::fs::File;

    use byteorder::{ByteOrder, BigEndian};

    #[test]
    fn reads_standard_sequencefile() {
        let kvs = main_read("test_data/abc_long_text_none.seq");

        assert_eq!(26, kvs.len());
        assert_eq!((0, "A".to_string()), kvs[0]);
        assert_eq!((25, "Z".to_string()), kvs[25]);
    }

    fn main_read(filename: &str) -> Vec<(i64, String)> {
        let path = Path::new(filename);

        let file = File::open(&path).unwrap();
        let seqfile = Reader::new(file);

        let kvs = seqfile.map(|(key, value)| {
            (BigEndian::read_i64(&key),
             String::from_utf8_lossy(&value[2..value.len()]).to_string())
        });

        kvs.collect()
    }
}

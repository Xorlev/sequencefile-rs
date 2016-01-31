use std::io::prelude::*;
use std::convert::AsRef;
use flate2::read::{GzDecoder, ZlibDecoder};
use errors::{Error, Result};

pub const DEFAULT_CODEC: &'static str = "org.apache.hadoop.io.compress.DefaultCodec";
pub const GZIP_CODEC: &'static str = "org.apache.hadoop.io.compress.GzipCodec";
pub const SNAPPY_CODEC: &'static str = "org.apache.hadoop.io.compress.SnappyCodec";
pub const BZIP2_CODEC: &'static str = "org.apache.hadoop.io.compress.Bzip2Codec";

/// Type of compression used on the sequencefile.
#[derive(Debug, PartialEq)]
pub enum CompressionType {
    /// No compression.
    None,
    /// Record-level compression, for each k-v pair, the value is compressed.
    Record,
    /// Block-level compression, many k-v pairs are compressed into a single block
    /// This mode is recommended for the best compression characteristics.
    Block,
}

/// Compression codec
#[derive(Debug)]
pub enum Codec {
    /// Deflate is default
    Default,
    /// Gzip, standard
    Gzip,
    /// Snappy compression. This is Hadoop-flavored Snappy vs. libsnappy
    Snappy,
    /// Bzip2 compression.
    Bzip2,
}


pub fn codec(codec: &String) -> Option<Codec> {
    match codec.as_ref() {
        DEFAULT_CODEC => Some(Codec::Default),
        GZIP_CODEC => Some(Codec::Gzip),
        SNAPPY_CODEC => None, // unsupported
        BZIP2_CODEC => None, // unsupported
        _ => None,
    }
}

pub fn decompressor(codec: &Codec, buffer: &[u8]) -> Result<Vec<u8>> {
    match *codec {
        Codec::Default => {
            let mut decoder = ZlibDecoder::new(buffer);

            let mut buf = Vec::new();
            try!(decoder.read_to_end(&mut buf));

            Ok(buf)
        }
        Codec::Gzip => {
            let mut decoder = try!(GzDecoder::new(buffer));

            let mut buf = Vec::new();
            try!(decoder.read_to_end(&mut buf));

            Ok(buf)
        }
        _ => Err(Error::CompressionTypeUnknown(format!("codec not implemented: {:?}", codec))),
    }
}

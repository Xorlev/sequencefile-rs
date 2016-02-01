use std::io::prelude::*;
use std::io;
use std::convert::AsRef;
use flate2::read::{GzDecoder, ZlibDecoder};
use bzip2::reader::BzDecompressor;
use errors::Result;

pub const DEFAULT_CODEC: &'static str = "org.apache.hadoop.io.compress.DefaultCodec";
pub const GZIP_CODEC: &'static str = "org.apache.hadoop.io.compress.GzipCodec";
pub const BZIP2_CODEC: &'static str = "org.apache.hadoop.io.compress.BZip2Codec";

/// Type of compression used on the sequencefile.
#[derive(Debug, PartialEq, Copy, Clone)]
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
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum Codec {
    /// Deflate is default
    Default,
    /// Gzip, standard
    Gzip,
    /// Bzip2 compression.
    Bzip2,
}


pub fn codec(codec: &String) -> Option<Codec> {
    match codec.as_ref() {
        DEFAULT_CODEC => Some(Codec::Default),
        GZIP_CODEC => Some(Codec::Gzip),
        BZIP2_CODEC => Some(Codec::Bzip2),
        _ => None,
    }
}

pub fn decompressor(codec: &Codec, buffer: &[u8]) -> Result<Vec<u8>> {
    match *codec {
        Codec::Default => Ok(try!(decompress(&mut ZlibDecoder::new(buffer)))),
        Codec::Gzip => Ok(try!(decompress(&mut try!(GzDecoder::new(buffer))))),
        Codec::Bzip2 => Ok(try!(decompress(&mut BzDecompressor::new(buffer)))),
    }
}

fn decompress<R: io::Read>(decompressor: &mut R) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    try!(decompressor.read_to_end(&mut buf));

    Ok(buf)
}

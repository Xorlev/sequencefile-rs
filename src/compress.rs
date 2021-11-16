use bzip2::reader::BzDecompressor;
use errors::Result;
use flate2::read::{GzDecoder, ZlibDecoder};
use std::io;

pub const DEFAULT_CODEC: &str = "org.apache.hadoop.io.compress.DefaultCodec";
pub const GZIP_CODEC: &str = "org.apache.hadoop.io.compress.GzipCodec";
pub const BZIP2_CODEC: &str = "org.apache.hadoop.io.compress.BZip2Codec";

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
    /// Bzip2 compression
    Bzip2,
}

pub fn codec(codec: &str) -> Option<Codec> {
    match codec {
        DEFAULT_CODEC => Some(Codec::Default),
        GZIP_CODEC => Some(Codec::Gzip),
        BZIP2_CODEC => Some(Codec::Bzip2),
        _ => None,
    }
}

pub fn decompressor(codec: &Codec, buffer: &[u8]) -> Result<Vec<u8>> {
    match *codec {
        Codec::Default => decompress(&mut ZlibDecoder::new(buffer)),
        Codec::Gzip => decompress(&mut GzDecoder::new(buffer)?),
        Codec::Bzip2 => decompress(&mut BzDecompressor::new(buffer)),
    }
}

fn decompress<R: io::Read>(decompressor: &mut R) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    decompressor.read_to_end(&mut buf)?;

    Ok(buf)
}

use byteorder::{ReadBytesExt, BigEndian, ByteOrder};
use std::io;
use std::io::prelude::*;
use std::collections::HashMap;
use std::str;
use flate2::read::ZlibDecoder;
use errors::{Error, Result};
use {Header, CompressionType, ByteString};

const MAGIC: &'static str = "SEQ";
const DEFAULT_CODEC: &'static str = "org.apache.hadoop.io.compress.DefaultCodec";
const SYNC_SIZE: usize = 16;

pub type Item = (ByteString, ByteString);

// Reader for a SequenceFile
// Iterable, returns a tuple of (ByteString, ByteString) for now until
// a Writables analog is developed
pub struct Reader<R: io::Read> {
    header: Header,
    reader: R,
    block_buffer: Vec<Item>,
}

impl<R: io::Read> Reader<R> {
    pub fn new(mut r: R) -> Reader<R> {
        // TODO: handle this
        let header = read_header(&mut r).unwrap();

        Reader {
            header: header,
            reader: r,
            block_buffer: Vec::new(),
        }
    }
}

fn read_header<R: io::Read>(reader: &mut R) -> Result<Header> {
    let mut magic = [0; 3];
    try!(reader.read(&mut magic));
    if magic != MAGIC.as_bytes() {
        return Err(Error::BadMagic(String::from_utf8_lossy(&magic).to_string()));
    }

    let mut version = [0; 1];
    try!(reader.read(&mut version));
    let version = version[0] as u16;

    // Version 4 - block compression
    // Version 5 - custom compression codecs
    // Version 6 - metadata
    if version < 5 {
        return Err(Error::VersionNotSupported(version));
    }

    let key_class = read_string(reader).unwrap();
    let value_class = read_string(reader).unwrap();

    let mut flags = [0; 2];
    try!(reader.read(&mut flags));

    let compression_type: CompressionType = {
        let compression = flags[0] as u8;
        let block_compression = flags[1] as u8;

        // first byte: compression t/f
        // second byte: block t/f
        match (compression, block_compression) {
            (1, 1) => CompressionType::Block,
            (1, 0) => CompressionType::Value,
            (0, 0) => CompressionType::None,
            _ => return Err(Error::CompressionTypeUnknown("Undefined type".to_string())),
        }
    };

    let compression_codec = if compression_type != CompressionType::None {
        Some(try!(read_string(reader)))
    } else {
        None
    };

    let pairs = reader.read_u32::<BigEndian>().unwrap();
    for _ in 0..pairs {
        // TODO: do stuff
    }

    let mut sync_marker = [0; SYNC_SIZE];
    try!(reader.read(&mut sync_marker));

    Ok(Header {
        version: version,
        compression_type: compression_type,
        compression_codec: compression_codec,
        key_class: key_class,
        value_class: value_class,
        metadata: HashMap::new(), // TODO
        sync_marker: sync_marker.to_vec(),
    })
}

impl<R: io::Read> Iterator for Reader<R> {
    type Item = (ByteString, ByteString);

    fn next(&mut self) -> Option<Item> {
        let mut last_sync_marker = [0; SYNC_SIZE];
        let mut kv_length = to_opt!(self.reader.read_i32::<BigEndian>()) as isize;
        // ^ todo: Varint reader

        // handle sync marker
        if kv_length == -1 {
            to_opt!(self.reader.read(&mut last_sync_marker));
            if last_sync_marker.to_vec() != self.header.sync_marker {
                panic!(Error::SyncMarkerMismatch);
            }

            kv_length = to_opt!(self.reader.read_i32::<BigEndian>()) as isize;

            if kv_length == -1 {
                return None;
            }
        }

        if self.header.compression_type == CompressionType::Block {
            if self.block_buffer.len() == 0 {
                unimplemented!();
            } else {
                return self.block_buffer.pop();
            }
        }

        let (key, value) = to_opt!(read_kv(kv_length, &self.header, &mut self.reader));

        Some((key, value))
    }
}


fn read_kv<R: io::Read>(kv_length: isize,
                        header: &Header,
                        reader: &mut R)
                        -> Result<(Vec<u8>, Vec<u8>)> {
    let k_length = reader.read_i32::<BigEndian>().unwrap() as usize;

    let k_start = 0;
    let k_end = k_start + (k_length - 0);
    let v_start = k_end;
    let v_end = v_start + (kv_length as usize - k_length);

    // TODO: DataInput/DataOutput semantics per type
    // e.g. impl LongWritable...
    // re-implement common writables
    // core interface: Iterator<(u[8], u[8])> or a KV struct
    // mixin interface on KVstruct could extract key/value from
    // bytes
    // debug!("k_start: {:?}, k_end: {:?}, v_start: {:?}, v_end: {:?}",
    //          k_start,
    //          k_end,
    //          v_start,
    //          v_end);
    //
    // debug!("{:?}", header);

    let mut buffer = vec![0; kv_length as usize];
    try!(reader.read(&mut buffer));

    let key = buffer[k_start..k_end].to_vec();

    if header.compression_type == CompressionType::Value {
        if header.compression_codec == Some(DEFAULT_CODEC.to_string()) {
            let mut decoder = ZlibDecoder::new(&buffer[v_start..v_end]);

            let mut buf = Vec::new();
            try!(decoder.read_to_end(&mut buf));

            Ok((key, buf))
        } else {
            return Err(Error::CompressionTypeUnknown("x".to_string()));
        }
    } else {
        let value = buffer[v_start..v_end].to_vec();

        Ok((key, value))
    }
}

fn read_string<R: io::Read>(reader: &mut R) -> Result<String> {
    // read one byte, value len
    let value_length = reader.read_u8().unwrap() as usize;
    let mut string = vec![0; value_length];

    try!(reader.read(&mut string));
    str::from_utf8(&string).map(|v| v.to_owned()).map_err(|e| Error::BadEncoding(e))
}

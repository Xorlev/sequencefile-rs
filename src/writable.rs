use byteorder::{BigEndian, ReadBytesExt};

use crate::errors::Result;

/// Basic trait mapping hadoop.io.Writable abstract class
/// Keys and Values types should implement this type to provide automatic deserialization
pub trait Writable {
    /// reads byte from buffer and converts to a concrete instance of Writable
    fn read(buf: &mut impl std::io::Read) -> Result<Self>
    where
        Self: Sized;
}

impl Writable for Vec<u8> {
    fn read(buf: &mut impl std::io::Read) -> Result<Self> {
        let mut result = vec![];
        buf.read_to_end(&mut result)?;
        Ok(result)
    }
}

impl Writable for i64 {
    fn read(buf: &mut impl std::io::Read) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(buf.read_i64::<BigEndian>()?)
    }
}

impl Writable for u64 {
    fn read(buf: &mut impl std::io::Read) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(buf.read_u64::<BigEndian>()?)
    }
}

impl Writable for i32 {
    fn read(buf: &mut impl std::io::Read) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(buf.read_i32::<BigEndian>()?)
    }
}

impl Writable for u32 {
    fn read(buf: &mut impl std::io::Read) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(buf.read_u32::<BigEndian>()?)
    }
}

impl Writable for i16 {
    fn read(buf: &mut impl std::io::Read) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(buf.read_i16::<BigEndian>()?)
    }
}

impl Writable for u16 {
    fn read(buf: &mut impl std::io::Read) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(buf.read_u16::<BigEndian>()?)
    }
}

impl Writable for u8 {
    fn read(buf: &mut impl std::io::Read) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(buf.read_u8()?)
    }
}

impl Writable for i8 {
    fn read(buf: &mut impl std::io::Read) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(buf.read_i8()?)
    }
}

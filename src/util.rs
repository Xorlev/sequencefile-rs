use std::io::Read;
use errors::Result;

pub trait ZeroCompress : Read {
    fn decode_vint64(&mut self) -> Result<i64> {
        let mut raw_buffer = vec![0u8; 1];
        try!(self.read_exact(&mut raw_buffer));

        let value = raw_buffer[0] as i8;
        let len = if value >= -112 {
            1
        } else if value < -120 {
            -119 - value
        } else {
            -111 - value
        };

        let mut val = 0i64;
        if len == 1 {
            val = value as i64;
        } else {
            for _ in 0..(len - 1) {
                try!(self.read_exact(&mut raw_buffer));
                val = val << 8;
                val = val | (raw_buffer[0] as i64 & 0xFF)
            }
        }

        if value < -120 || (value >= -112 && value < 0) {
            Ok(val ^ -1)
        } else {
            Ok(val)
        }
    }
}

impl<R> ZeroCompress for R where R: Read
{}

use errors::Result;
use std::io::Read;

pub trait ZeroCompress: Read {
    fn decode_vint64(&mut self) -> Result<i64> {
        let mut raw_buffer = vec![0u8; 1];
        self.read_exact(&mut raw_buffer)?;

        let value = raw_buffer[0] as i8;
        let len = if value >= -112 {
            1
        } else if value < -120 {
            // [2, 8], negative result
            -119 - value
        } else {
            // [2, 8], positive result
            -111 - value
        };

        let mut val = 0i64;
        if len == 1 {
            val = value as i64;
        } else {
            // shifts buffer to make room for new set-o-bits, ors in the new byte
            for _ in 0..(len - 1) {
                self.read_exact(&mut raw_buffer)?;
                val <<= 8;
                val |= raw_buffer[0] as i64 & 0xFF
            }
        }

        if value < -120 || (value >= -112 && value < 0) {
            Ok(val ^ -1)
        } else {
            Ok(val)
        }
    }
}

impl<R> ZeroCompress for R where R: Read {}

#[cfg(test)]
mod tests {
    use super::ZeroCompress;
    use std::io::Cursor;

    #[test]
    fn decodes_single_byte() {
        let mut buf = Cursor::new(vec![0b0111_1111]);

        assert_eq!(127, buf.decode_vint64().unwrap());
    }

    #[test]
    fn decodes_multi_byte() {
        let mut buf = Cursor::new(vec![0b1000_1101, 0b1000_0000, 0b0100_0000, 0b0010_1101]);

        assert_eq!(8_405_037, buf.decode_vint64().unwrap());
    }

    #[test]
    fn decodes_multi_byte_negative() {
        let mut buf = Cursor::new(vec![0b1000_0101, 0b1000_0000, 0b0100_0000, 0b0010_1101]);

        assert_eq!(-8_405_038, buf.decode_vint64().unwrap());
    }
}

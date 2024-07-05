
#![allow(dead_code)]

use byteorder::{ReadBytesExt, BigEndian};

use std::io;

struct BytesReader<'a>(&'a [u8]);

impl<'a> BytesReader<'a> {
	pub fn new(buffer: &'a [u8]) -> Self {
		Self(buffer)
	}

	pub fn read_bytes(&mut self, bytes_num: usize) -> io::Result<&'a [u8]> {
		if self.0.len() < bytes_num {
			Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Not enough bytes"))
		} else {
			let req = &self.0[..bytes_num];
            self.0 = &self.0[bytes_num..];
            Ok(req)
		}
	}

	pub fn advance_bytes(&self, bytes_num: usize) -> io::Result<&'a [u8]> {
		if self.0.len() < bytes_num {
            Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Not enough bytes"))
        } else {
            Ok(&self.0[bytes_num..])
        }
	}

	pub fn advance_bytes_reader(&self, bytes_num: usize) -> io::Result<BytesReader<'a>> {
		Ok(BytesReader::new(self.advance_bytes(bytes_num)?))
	}

	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	pub fn get_remaining_bytes(&self) -> &'a [u8] {
		self.0
	}
	
	pub fn parse<P: KafkaMessage<'a>>(&mut self) -> io::Result<P> {
	    P::parse(self)
	}
}

impl io::Read for BytesReader<'_> {
	fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
		let amount = std::cmp::min(buf.len(), self.0.len());
		let remaining = self.read_bytes(amount)?;
		buf[..amount].copy_from_slice(&remaining[..amount]);
		Ok(amount)
	}
}

macro_rules! primitive {
    ($([$t:ty, $f:ident]),*) => {
        $(
            impl<'a> KafkaMessage<'a> for $t {
                fn parse(cursor: &mut BytesReader<'a>) -> io::Result<Self> {
                    cursor.0.$f::<byteorder::BigEndian>().map_err(io::Error::other)
                }
            }
        )*
    };
}

primitive!(
    [i16, read_i16],
    [i32, read_i32],
    [i64, read_i64],
    [u16, read_u16],
    [u32, read_u32],
    [u64, read_u64]
);

#[derive(Debug)]
struct RequestHeaderV2<'a> {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    client_id: Option<KafkaString<'a>>,
}

#[derive(Debug)]
struct KafkaString<'a>(&'a str);

trait KafkaMessage<'a>: Sized {
    fn parse(cursor: &mut BytesReader<'a>) -> io::Result<Self>;
}

impl<'a> KafkaMessage<'a> for KafkaString<'a> {
    fn parse(cursor: &mut BytesReader<'a>) -> io::Result<Self> {
        let length = cursor.read_i16::<BigEndian>()?;
        if length < 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "less than zero length"))
        }

        let data = cursor.read_bytes(length as usize)?;
        std::str::from_utf8(data).map_err(io::Error::other).map(Self)
    }
}

impl<'a> KafkaMessage<'a> for Option<KafkaString<'a>> {
    fn parse(cursor: &mut BytesReader<'a>) -> io::Result<Self> {
        let length = cursor.advance_bytes_reader(2)?.read_i16::<BigEndian>()?;
        if length == -1 {
            return Ok(None)
        }

        KafkaString::parse(cursor).map(Some)
    }
}

impl<'a> KafkaMessage<'a> for RequestHeaderV2<'a> {
    fn parse(cursor: &mut BytesReader<'a>) -> io::Result<Self> {
        Ok(RequestHeaderV2 {
            request_api_key: cursor.parse()?,
            request_api_version: cursor.parse()?,
            correlation_id: cursor.parse()?,
            client_id: cursor.parse()?,
        })
    }
}

fn main() {
    const DATA: &[u8] = &[
        0x00, 0x00, 
        0x00, 0x0e,
        0x00, 0x03,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x01,
        0x00, 0x00,
        0x00, 0x00,
    ];
    
    dbg!(RequestHeaderV2::parse(&mut BytesReader::new(DATA)));
}

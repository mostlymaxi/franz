use tokio_util::bytes::{Buf, BufMut, BytesMut};

// let <field> = <type>::decode(src);
//
// Self {
//    #(#fields,)*
// }

pub trait Decode {
    fn decode(src: &mut BytesMut) -> Self;
}

pub trait KafkaSize {
    fn get_size(&self) -> i32;
}

pub trait Encode {
    fn get_size(&self) -> i32;
    fn encode(&self, dst: &mut BytesMut);
}

impl Decode for i32 {
    fn decode(src: &mut BytesMut) -> i32 {
        src.get_i32()
    }
}

impl Decode for i64 {
    fn decode(src: &mut BytesMut) -> i64 {
        src.get_i64()
    }
}

impl Decode for u32 {
    fn decode(src: &mut BytesMut) -> u32 {
        src.get_u32()
    }
}

impl Encode for i32 {
    fn get_size(&self) -> i32 {
        std::mem::size_of::<i32>() as i32
    }

    fn encode(&self, dst: &mut BytesMut) {
        dst.put_i32(*self);
    }
}

impl Decode for i8 {
    fn decode(src: &mut BytesMut) -> i8 {
        src.get_i8()
    }
}

impl Decode for u8 {
    fn decode(src: &mut BytesMut) -> u8 {
        src.get_u8()
    }
}

impl Decode for i16 {
    fn decode(src: &mut BytesMut) -> i16 {
        src.get_i16()
    }
}

impl Encode for i16 {
    fn get_size(&self) -> i32 {
        std::mem::size_of::<i16>() as i32
    }

    fn encode(&self, dst: &mut BytesMut) {
        dst.put_i16(*self);
    }
}

impl<T: Decode> Decode for Vec<T> {
    fn decode(src: &mut BytesMut) -> Vec<T> {
        let mut values = Vec::new();
        let length = i32::decode(src);

        for _ in 0..length {
            values.push(T::decode(src));
        }

        values
    }
}

impl<T: Encode> Encode for Vec<T> {
    fn get_size(&self) -> i32 {
        std::mem::size_of::<i32>() as i32 + self.iter().map(|s| s.get_size()).sum::<i32>()
    }

    fn encode(&self, dst: &mut BytesMut) {
        dst.put_i32(self.len() as i32);
        for i in self {
            i.encode(dst);
        }
    }
}

impl<T: Decode> Decode for Option<Vec<T>> {
    fn decode(src: &mut BytesMut) -> Option<Vec<T>> {
        let mut values = Vec::new();
        let length = i32::decode(src);

        if length == -1 {
            return None;
        }

        for _ in 0..length {
            values.push(T::decode(src));
        }

        Some(values)
    }
}

impl Decode for String {
    fn decode(src: &mut BytesMut) -> String {
        let length = i16::decode(src);

        // check if casting length to usize is okay

        let value = match length {
            0.. => String::from_utf8_lossy(&src[..length as usize]).into(),
            _ => unreachable!(),
        };

        if length > 0 {
            src.advance(length as usize);
        }

        value
    }
}

impl Decode for Option<String> {
    fn decode(src: &mut BytesMut) -> Option<String> {
        let length = i16::decode(src);

        // check if casting length to usize is okay

        let value = match length {
            -1 => None,
            0.. => Some(String::from_utf8_lossy(&src[..length as usize]).into()),
            _ => unreachable!(),
        };

        if length > 0 {
            src.advance(length as usize);
        }

        value
    }
}

impl Encode for String {
    fn get_size(&self) -> i32 {
        (self.as_bytes().len() + std::mem::size_of::<i16>()) as i32
    }

    fn encode(&self, dst: &mut BytesMut) {
        // should probably check this cast but oh well.
        dst.put_i16(self.len() as i16);
        dst.put(self.as_bytes());
    }
}

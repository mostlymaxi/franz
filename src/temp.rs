use std::{
    borrow::Cow,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

use std::thread;

use tokio_util::{
    bytes::{Buf, BytesMut},
    codec::Decoder,
};

#[derive(Debug)]
struct RequestHeaderV2<'a> {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    client_id: Option<String>,
}

enum KafkaApi {
    RequestHeaderV2,
}

trait KafkaApiMessage {
    fn decode(src: &mut BytesMut) -> Self;
}

struct KafkaApiDecoder {
    request_api_key: Option<i16>,
    request_api_version: Option<i16>,
}

impl KafkaApiMessage for RequestHeaderV2 {
    fn decode(src: &mut BytesMut) -> Self {
        let correlation_id = src.get_i32();
        let length = src.get_i16();

        let client_id = match length {
            -1 => None,
            0.. => Some(String::from_utf8_lossy(&src[..length as usize]).into()),
        };

        src.advance(length as usize);

        RequestHeaderV2 {
            request_api_key,
            request_api_version,
            correlation_id,
            client_id,
        }
    }
}

impl Decoder for KafkaApiDecoder {
    type Item = KafkaApi;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // request header
        // send a response header
        // produce / fetch that we want to decode based on the api_key we received previously
        //
        if src.len() < 4 {
            // Not enough data to read length marker.
            return Ok(None);
        }

        // Read length marker.
        let length = src.get_i32() as usize;

        if src.len() < 4 + length {
            // The full string has not yet arrived.
            //
            // We reserve more space in the buffer. This is not strictly
            // necessary, but is a good idea performance-wise.
            src.reserve(4 + length - src.len());

            // We inform the Framed that we need more bytes to form the next
            // frame.
            return Ok(None);
        }

        match self.request_api_key {
            None => {}
            Some(0) => {}
            Some(_) => {}
        }

        // Read api key.
        let api_key = src.get_i16() as usize;
        // Read api version
        let api_version = src.get_i16() as usize;

        src.advance(4);

        if api_version != 0 {
            panic!("unsuported api version");
        }

        match api_key {
            0 => decode::<RequestHeaderV2>(),
            _ => unimplemented!(),
        }
    }
}

// this should be tokio_utils::Decoder
impl<'a> From<&'a [u8]> for RequestHeaderV2<'a> {
    fn from(input: &'a [u8]) -> RequestHeaderV2<'a> {
        let request_api_key = i16::from_be_bytes([input[0], input[1]]);
        let request_api_version = i16::from_be_bytes([input[2], input[3]]);
        let correlation_id = i32::from_be_bytes([input[4], input[5], input[6], input[7]]);
        let client_id_len = i16::from_be_bytes([input[8], input[9]]);

        let str_start = 10;
        let str_end = str_start + client_id_len as usize;

        let client_id = match client_id_len {
            -1 => None,
            0.. => Some(String::from_utf8_lossy(&input[str_start..str_end])),
            _ => unreachable!(),
        };

        RequestHeaderV2 {
            request_api_key,
            request_api_version,
            correlation_id,
            client_id,
        }
    }
}

fn handle_produce_test(mut stream: TcpStream) {
    let mut buf: [u8; 4] = [0; 4];
    let msg_size: i32;

    loop {
        stream.read_exact(&mut buf).unwrap();
        msg_size = i32::from_be_bytes(buf);

        match msg_size {
            ..0 => panic!(),
            _ => {}
        }
        eprintln!("{msg_size}");

        let mut buf_tmp: Vec<u8> = vec![0; msg_size as usize];

        stream.read_exact(&mut buf_tmp).unwrap();

        eprintln!("{:X?}", buf_tmp);
        let req: RequestHeaderV2 = buf_tmp.as_slice().into();

        eprintln!("{:#?}", req);

        stream
            .write_all(&(std::mem::size_of::<i32>() as i32).to_be_bytes())
            .unwrap();

        stream.write_all(&req.correlation_id.to_be_bytes()).unwrap();
        break;
    }
}

#[test]
fn kafka_api_test() {
    let listener = TcpListener::bind("127.0.0.1:8085").unwrap();

    for stream in listener.incoming() {
        let stream = stream.unwrap();

        thread::spawn(move || handle_produce_test(stream));
    }
}

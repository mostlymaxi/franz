use std::{collections::HashMap, io::Read, net::TcpStream, time::Duration};

use thiserror::Error;
use tracing::{debug, instrument};

const HANDSHAKE_TIMEOUT_SECS: u64 = 10;
const HANDSHAKE_NUM_FIELDS: usize = 16;
type HandshakeHeaderNum = u32;

/// ## The Franz Porotocol
/// [message length : u32]([key]=[value] : utf8)
///
/// ### mandatory keys
/// - version
/// - topic
/// - api
///
/// ### example
/// version=1,topic=test_topic_name,api=produce
#[derive(Debug)]
pub struct Handshake {
    pub _version: u16,
    pub group: Option<u16>,
    pub topic: String,
    pub api: String,
}

#[derive(Error, Debug)]
pub enum HandshakeError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    ParseNum(#[from] std::num::ParseIntError),
    #[error(transparent)]
    FromUtf8Error(#[from] std::string::FromUtf8Error),
    #[error("could not find expected key: {0}")]
    ExpectedKey(&'static str),
    #[error("invalid header format")]
    InvalidFormat,
}

impl Handshake {
    #[instrument]
    fn parse_length(sock: &mut TcpStream) -> Result<HandshakeHeaderNum, HandshakeError> {
        let mut handshake_length = [0; size_of::<HandshakeHeaderNum>()];
        sock.read_exact(&mut handshake_length)?;

        Ok(HandshakeHeaderNum::from_be_bytes(handshake_length))
    }

    #[instrument(skip(data))]
    fn parse_data(data: Vec<u8>) -> Result<HashMap<String, String>, HandshakeError> {
        let data = String::from_utf8(data)?;
        debug!(%data);

        let mut data = data.split(',');

        let mut handshake = HashMap::with_capacity(HANDSHAKE_NUM_FIELDS);
        for _ in 0..HANDSHAKE_NUM_FIELDS {
            let Some(kv) = data.next() else {
                break;
            };

            let mut kv = kv.split('=');
            let key = kv.next().ok_or(HandshakeError::InvalidFormat)?;
            let Some(value) = kv.next() else {
                continue;
            };

            let key = key.trim().to_lowercase();
            let value = value.trim().to_string();
            handshake.insert(key, value);
        }

        Ok(handshake)
    }

    #[instrument]
    pub fn try_parse(sock: &mut TcpStream) -> Result<Self, HandshakeError> {
        sock.set_read_timeout(Some(Duration::from_secs(HANDSHAKE_TIMEOUT_SECS)))?;

        let handshake_length = Self::parse_length(sock)?;
        debug!(?handshake_length);

        let mut data = vec![0; handshake_length as usize];
        sock.read_exact(&mut data)?;

        let handshake = Self::parse_data(data)?;

        let group = match handshake.get("group") {
            None => None,
            Some(g) => Some(g.parse()?),
        };

        sock.set_read_timeout(None)?;

        Ok(Handshake {
            _version: handshake
                .get("version")
                .ok_or(HandshakeError::ExpectedKey("version"))?
                .parse()?,
            group,
            topic: handshake
                .get("topic")
                .ok_or(HandshakeError::ExpectedKey("topic"))?
                .parse()
                .expect("infalible"),
            api: handshake
                .get("api")
                .ok_or(HandshakeError::ExpectedKey("api"))?
                .to_string(),
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_data() {
        let input = "version=1,topic=test,api=producer";
        let mut data = Vec::new();
        // data.extend_from_slice(&(input.len() as HandshakeHeader).to_be_bytes());
        data.extend_from_slice(input.as_bytes());

        let h = Handshake::parse_data(data).unwrap();
        eprintln!("{:#?}", h);
    }
}

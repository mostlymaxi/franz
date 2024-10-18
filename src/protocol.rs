use std::{collections::HashMap, io::Read, net::TcpStream, time::Duration};

const HANDSHAKE_TIMEOUT_SECS: u64 = 10;
const HANDSHAKE_NUM_FIELDS: usize = 16;
type HandshakeHeader = u32;

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

impl Handshake {
    fn parse_length(sock: &mut TcpStream) -> Option<HandshakeHeader> {
        let mut handshake_length = [0; size_of::<HandshakeHeader>()];
        sock.read_exact(&mut handshake_length).ok()?;

        Some(HandshakeHeader::from_be_bytes(handshake_length))
    }

    fn parse_data(data: Vec<u8>) -> Option<HashMap<String, String>> {
        let data = String::from_utf8(data).ok()?;
        let mut data = data.split(',');

        let mut handshake = HashMap::with_capacity(HANDSHAKE_NUM_FIELDS);
        for _ in 0..HANDSHAKE_NUM_FIELDS {
            let Some(kv) = data.next() else {
                break;
            };

            let mut kv = kv.split('=');
            let key = kv.next()?;
            let Some(value) = kv.next() else {
                continue;
            };

            let key = key.trim().to_lowercase();
            let value = value.trim().to_string();
            handshake.insert(key, value);
        }

        Some(handshake)
    }

    pub fn parse(sock: &mut TcpStream) -> Option<Self> {
        sock.set_read_timeout(Some(Duration::from_secs(HANDSHAKE_TIMEOUT_SECS)))
            .ok()?;

        let handshake_length = Self::parse_length(sock)?;

        let mut data = vec![0; handshake_length as usize];
        sock.read_exact(&mut data).ok()?;

        let handshake = Self::parse_data(data)?;

        let group = match handshake.get("group") {
            None => None,
            Some(g) => Some(g.parse().ok()?),
        };

        sock.set_read_timeout(None).ok()?;

        Some(Handshake {
            _version: handshake.get("version")?.parse().ok()?,
            group,
            topic: handshake.get("topic")?.parse().ok()?,
            api: handshake.get("api")?.to_string(),
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

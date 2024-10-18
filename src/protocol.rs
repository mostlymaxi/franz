use std::{collections::HashMap, io::Read, net::TcpStream, time::Duration};

const HANDSHAKE_TIMEOUT_SECS: u64 = 10;
const HANDSHAKE_NUM_FIELDS: usize = 4;
type HandshakeHeaderType = u32;

pub struct Handshake {
    pub version: u16,
    pub group: Option<u16>,
    pub topic: String,
    pub connection_type: String,
}

impl Handshake {
    fn parse_length(sock: &mut TcpStream) -> Option<HandshakeHeaderType> {
        let mut handshake_length = [0; size_of::<HandshakeHeaderType>()];
        sock.read_exact(&mut handshake_length).ok()?;

        Some(HandshakeHeaderType::from_be_bytes(handshake_length))
    }

    fn parse_data(data: Vec<u8>) -> Option<HashMap<String, String>> {
        let data = String::from_utf8(data).ok()?;
        let mut data = data.split('=');

        let mut handshake = HashMap::with_capacity(HANDSHAKE_NUM_FIELDS);
        loop {
            let Some(field) = data.next() else {
                break;
            };

            let field = field.trim().to_lowercase();
            let value = data.next()?.trim().to_string();
            handshake.insert(field, value);
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
            version: handshake.get("version")?.parse().ok()?,
            group,
            topic: handshake.get("topic")?.parse().ok()?,
            connection_type: handshake.get("connection_type")?.to_string(),
        })
    }
}

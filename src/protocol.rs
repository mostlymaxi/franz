use franz_core::Decode;
use franz_core::Encode;
use franz_macros::Decode;
use franz_macros::Encode;
use tokio::net::TcpListener;

use futures::SinkExt;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::{
    bytes::{Buf, BufMut, BytesMut},
    codec::{Decoder, Encoder, Framed},
};

pub mod header_v2 {
    use franz_macros::Decode;
    use franz_macros::Encode;
    use tokio_util::bytes::BytesMut;

    #[derive(Debug, Decode)]
    pub struct RequestHeader {
        pub request_api_key: i16,
        pub request_api_version: i16,
        pub correlation_id: i32,
        pub client_id: Option<String>,
    }

    #[derive(Debug, Encode)]
    pub struct ResponseHeader {
        pub correlation_id: i32,
    }
}

pub mod produce_v0 {
    use franz_macros::Decode;
    use franz_macros::Encode;
    use tokio_util::bytes::BytesMut;

    #[derive(Debug, Decode)]
    pub struct ProduceRequest {
        acks: i16,
        timeout_ms: i32,
        topic_data: Vec<TopicData>,
    }

    #[derive(Debug, Decode)]
    struct TopicData {
        name: String,
        partition_data: Vec<PartitionData>,
    }

    #[derive(Debug, Decode)]
    struct PartitionData {
        index: i32,
        // make a special record type that decodes VERY FAST
        records: Option<Vec<u8>>,
    }
}

pub mod fetch_v0 {
    use franz_macros::Decode;
    use franz_macros::Encode;
    use tokio_util::bytes::BytesMut;

    #[derive(Debug, Decode)]
    struct FetchRequest {
        replica_id: i32,
        max_wait_ms: i32,
        min_bytes: i32,
        topics: Vec<Topic>,
    }

    #[derive(Debug, Decode)]
    struct Topic {
        name: String,
        partitions: Vec<Partition>,
    }

    #[derive(Debug, Decode)]
    struct Partition {
        partition_index: i32,
        timestamp: i64,
        max_num_offsets: i32,
    }
}

// broker - sever ip address and port (only thing we need to store)
// topics - list of disk-ringbuf folders (search through topics folder)
// partitions - sub folders: list of disk-ringbufs (maybe in the future search through partitions
// folder)
//
// topic: /var/franz/topics/topic1
// partitions: /var/franz/topic1/part?/?.bin

pub mod metadata_v0 {
    use franz_macros::Decode;
    use franz_macros::Encode;
    use tokio_util::bytes::BytesMut;

    #[derive(Debug, Decode)]
    pub struct MetadataRequest {
        pub topics: Vec<String>,
    }

    #[derive(Debug, Encode)]
    pub struct MetadataResponse {
        pub header: super::header_v2::ResponseHeader,
        pub brokers: Vec<Broker>,
        pub topics: Vec<Topic>,
    }

    #[derive(Debug, Decode, Encode)]
    pub struct Broker {
        pub node_id: i32,
        pub host: String,
        pub port: i32,
    }

    #[derive(Debug, Decode, Encode)]
    pub struct Topic {
        pub error_code: i16,
        pub name: String,
        pub partitions: Vec<Partition>,
    }

    #[derive(Debug, Decode, Encode)]
    pub struct Partition {
        pub error_code: i16,
        pub partition_index: i32,
        pub leader_id: i32,
        pub replica_nodes: Vec<i32>,
        pub isr_nodes: Vec<i32>,
    }

    pub fn get_test_brokers() -> Vec<Broker> {
        vec![Broker {
            node_id: 0,
            host: "127.0.0.1".to_string(),
            port: 8085,
        }]
    }

    pub fn get_test_topics() -> Vec<Topic> {
        vec![Topic {
            error_code: 0,
            name: "test".to_string(),
            partitions: vec![Partition {
                error_code: 0,
                isr_nodes: vec![1],
                leader_id: 0,
                partition_index: 0,
                replica_nodes: vec![0],
            }],
        }]
    }
}

pub mod list_offset_v0 {
    use franz_macros::Decode;
    use franz_macros::Encode;
    use tokio_util::bytes::BytesMut;

    #[derive(Debug, Decode)]
    pub struct ListOffsetsRequest {
        replica_id: i32,
        topics: Vec<Topic>,
    }

    #[derive(Debug, Decode)]
    struct Topic {
        name: String,
        partitions: Vec<Partition>,
    }

    #[derive(Debug, Decode)]
    struct Partition {
        partition_index: i32,
        timestamp: i64,
        max_num_offsets: i32,
    }
}

pub mod offset_commit_v0 {
    pub struct OffsetCommitRequest {
        pub group_id: String,
        pub topics: Vec<Topic>,
    }

    pub struct Topic {
        pub name: String,
        pub partitions: Vec<Partition>,
    }

    pub struct Partition {
        partition_index: i32,
        commited_offset: i64,
        commited_metadata: Option<String>,
    }
}

#[derive(Debug)]
pub enum KafkaApiRequests {
    ProduceRequest(produce_v0::ProduceRequest),
    ListOffsetsRequest(list_offset_v0::ListOffsetsRequest),
    MetadataRequest(metadata_v0::MetadataRequest),
}

#[derive(Debug, Encode)]
pub enum KafkaApiResponse {
    MetadataResponse(metadata_v0::MetadataResponse),
}

#[derive(Debug)]
pub struct KafkaApiConnection;

impl Decoder for KafkaApiConnection {
    type Item = (i32, KafkaApiRequests);
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        log::trace!("raw src: {:X?}", src);
        if src.len() < 4 {
            src.reserve(4);
            // Not enough data to read length marker.
            return Ok(None);
        }

        let _length = src.get_i32();
        // TODO: optimization here to get full message

        let header = header_v2::RequestHeader::decode(src);
        log::trace!("header: {:?}", header);

        if header.request_api_version != 0 {
            panic!("unsuported api version");
        }

        let req = match header.request_api_key {
            0 => KafkaApiRequests::ProduceRequest(produce_v0::ProduceRequest::decode(src)),
            2 => KafkaApiRequests::ListOffsetsRequest(list_offset_v0::ListOffsetsRequest::decode(
                src,
            )),
            3 => KafkaApiRequests::MetadataRequest(metadata_v0::MetadataRequest::decode(src)),
            _ => return Ok(None),
        };

        log::trace!("request: {:?}", req);

        Ok(Some((header.correlation_id, req)))
    }
}

impl Encoder<KafkaApiResponse> for KafkaApiConnection {
    type Error = std::io::Error;

    fn encode(&mut self, item: KafkaApiResponse, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let length = item.get_size();
        log::trace!("encode length: {length}");
        dst.put_i32(length);

        match item {
            KafkaApiResponse::MetadataResponse(req) => {
                req.encode(dst);
                log::trace!("raw dst: {:X?}", dst);
                Ok(())
            }
        }
    }
}

pub async fn handle_client(stream: TcpStream) {
    let decoder = KafkaApiConnection {};

    let mut f = Framed::new(stream, decoder);

    while let Some(msg) = f.next().await {
        match msg {
            Ok((id, KafkaApiRequests::MetadataRequest(req))) => {
                let header_res = header_v2::ResponseHeader { correlation_id: id };

                let metadata_res = metadata_v0::MetadataResponse {
                    header: header_res,
                    brokers: metadata_v0::get_test_brokers(),
                    topics: metadata_v0::get_test_topics(),
                };

                f.send(KafkaApiResponse::MetadataResponse(metadata_res))
                    .await
                    .unwrap();
            }
            Ok((id, KafkaApiRequests::ProduceRequest(req))) => log::trace!("{id} {:#?}", req),
            Ok((id, KafkaApiRequests::ListOffsetsRequest(req))) => log::trace!("{id} {:#?}", req),
            Err(e) => log::error!("{e}"),
        }
    }
}

// fn decode
// parse the request header -> return Api Key
// send header response
// parse message -> return message
// do stuff with message
// send response
// set api_key to None
//
//

#[test]
fn size_of_val_test() {
    let v1 = vec!["asdf".to_string()];
    let req = metadata_v0::MetadataRequest { topics: v1 };

    eprintln!("{:?}", std::mem::size_of_val(&req));
}

#[tokio::test]
async fn kafka_api_test() {
    env_logger::init();
    let listener = TcpListener::bind("127.0.0.1:8085").await.unwrap();

    loop {
        let (socket, addr) = match listener.accept().await {
            Ok((s, a)) => (s, a),
            Err(e) => {
                log::error!("failed to accept connection: {:?}", e);
                continue;
            }
        };

        log::info!("({}) accepted a producer client", &addr);

        tokio::spawn(async move {
            handle_client(socket).await;

            log::info!("({}) disconnected", &addr);
        });
    }
}

use crate::protocol::*;
use futures::SinkExt;
use std::sync::Arc;
use tokio_stream::StreamExt;

use disk_ringbuffer::ringbuf;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::server::FranzServer;

pub struct FranzClient<'a> {
    server: Arc<&'a FranzServer>,
    stream: TcpStream,
    rx: Option<ringbuf::Reader>,
    tx: Option<ringbuf::Writer>,
}

impl<'a> FranzClient<'a> {
    pub fn new(server: &FranzServer, stream: TcpStream) -> FranzClient {
        let server = Arc::new(server);

        FranzClient {
            server,
            stream,
            rx: None,
            tx: None,
        }
    }

    pub async fn handle_client(stream: TcpStream) {
        let decoder = KafkaApiConnection;
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
                Ok((id, KafkaApiRequests::ListOffsetsRequest(req))) => {
                    // self.server.get_offsets();
                    log::trace!("{id} {:#?}", req)
                }
                Err(e) => log::error!("{e}"),
            }
        }
    }
}

use disk_ringbuffer::ringbuf;
use num_derive::FromPrimitive;
use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::{select, signal};

#[derive(Clone)]
pub struct FranzServer {
    server_path: PathBuf,
    sock_addr: SocketAddr,
    stop_rx: watch::Receiver<()>,
    default_max_pages: usize,
    topics: Arc<RwLock<HashMap<String, (ringbuf::Writer, ringbuf::Reader)>>>,
}

#[repr(u8)]
#[derive(Debug, FromPrimitive)]
enum ClientKind {
    Produce = 0,
    Consume = 1,
    Info = 2,
}

impl FranzServer {
    pub const DEFAULT_IPV4: (u8, u8, u8, u8) = (127, 0, 0, 1);

    pub fn new(server_path: PathBuf, port: u16, default_max_pages: usize) -> FranzServer {
        let (stop_tx, stop_rx) = watch::channel(());

        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            let _ = stop_tx.send(());
        });

        let (a, b, c, d) = Self::DEFAULT_IPV4;

        let sock_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(a, b, c, d)), port);
        let topics = Arc::new(RwLock::new(HashMap::new()));

        FranzServer {
            sock_addr,
            default_max_pages,
            server_path,
            topics,
            stop_rx,
        }
    }

    async fn get_or_create_topic(&self, topic: String) -> (ringbuf::Writer, ringbuf::Reader) {
        let topics = self.topics.read().unwrap();
        match topics.get(&topic) {
            Some((tx, rx)) => (tx.clone(), rx.clone()),
            None => {
                // unlock topics RWLock
                drop(topics);

                let (mut tx, mut rx) =
                    ringbuf::new(self.server_path.join(&topic), self.default_max_pages).unwrap();

                tx.super_unsafe_page_cleanup_never_call_this_unless_you_know_what_youre_doing();
                rx.super_unsafe_page_cleanup_never_call_this_unless_you_know_what_youre_doing();

                let tx_c = tx.clone();
                let rx_c = rx.clone();

                let mut topics = self.topics.write().unwrap();

                topics.insert(topic, (tx, rx));
                (tx_c, rx_c)
            }
        }
    }

    async fn handle_produce(&self, sock: TcpStream, addr: SocketAddr, topic: String) {
        let (mut tx, _) = self.get_or_create_topic(topic).await;

        tokio::spawn(async move {
            let stream = BufReader::new(sock);
            let mut stream_lines = stream.lines();
            while let Some(line) = stream_lines.next_line().await.unwrap() {
                tx.push(line).unwrap();
            }

            log::info!("({}) disconnected", &addr);
        });
    }

    async fn handle_consume(&self, sock: TcpStream, addr: SocketAddr, topic: String) {
        let (_, rx) = self.get_or_create_topic(topic).await;

        let mut stream = BufWriter::new(sock);
        tokio::spawn(async move {
            // can make BufWriter if needed
            for msg in rx {
                match msg {
                    Err(e) => {
                        log::error!("{e}");
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Ok(None) => {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    Ok(Some(msg)) => {
                        log::trace!("{msg}");
                        stream.write_all(msg.as_bytes()).await.unwrap();
                        stream.write_all(&[b'\n']).await.unwrap();
                    }
                }
            }

            log::info!("({}) disconnected", &addr);
        });
    }

    async fn client_handler(&self) -> Result<(), io::Error> {
        let listener = TcpListener::bind(&self.sock_addr).await?;

        loop {
            let (mut socket, addr) = match listener.accept().await {
                Ok((s, a)) => (s, a),
                Err(e) => {
                    log::error!("failed to accept connection: {:?}", e);
                    continue;
                }
            };

            let (sock_rdr, _) = socket.split();
            let mut sock_rdr = BufReader::new(sock_rdr);

            log::info!("({}) accepted a producer client", &addr);

            // this is the efficient version:
            //let _msg_length = socket.read_u32().await.unwrap();
            //let msg_what: MsgWhat =
            //    num::FromPrimitive::from_u8(socket.read_u8().await.unwrap()).unwrap();
            //let topic_length = socket.read_u32().await.unwrap();
            //let mut buf = vec![0; topic_length as usize];
            //socket.read_exact(&mut buf).await.unwrap();
            //let topic = String::from_utf8(buf).unwrap();

            let mut client_kind = String::new();
            sock_rdr.read_line(&mut client_kind).await.unwrap();
            let client_kind = client_kind.trim().parse::<u8>().unwrap();
            let client_kind = num::FromPrimitive::from_u8(client_kind).unwrap();

            let mut topic = String::new();
            sock_rdr.read_line(&mut topic).await.unwrap();
            let topic = topic.trim().to_string();

            match client_kind {
                ClientKind::Produce => self.handle_produce(socket, addr, topic).await,
                ClientKind::Consume => self.handle_consume(socket, addr, topic).await,
                ClientKind::Info => unreachable!(),
            }
        }
    }

    pub async fn run(self) {
        log::debug!("starting queue server...");

        let mut stop_rx_clone = self.stop_rx.clone();
        let self_clone = self.clone();

        let client_handler = tokio::spawn(async move {
            select! {
                _ = stop_rx_clone.changed() => {},
                _ = self_clone.client_handler() => {}
            }
        });

        log::info!("queue server ready");
        client_handler.await.unwrap();

        log::info!("shutting down...")
    }
}

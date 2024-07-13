use crate::protocol;
use disk_ringbuffer::ringbuf;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::{select, signal};

#[derive(Clone)]
pub struct FranzServer {
    addr_producer: SocketAddr,
    addr_consumer: SocketAddr,
    ghost_rx: ringbuf::Reader,
    ghost_tx: ringbuf::Writer,
    stop_rx: watch::Receiver<()>,
}

impl FranzServer {
    pub const DEFAULT_MAX_PAGES: usize = 0;
    pub const DEFAULT_PRODUCER_PORT: u16 = 8084;
    pub const DEFAULT_CONSUMER_PORT: u16 = 8085;
    pub const DEFAULT_IPV4: (u8, u8, u8, u8) = (127, 0, 0, 1);

    pub fn new(path: &Path, max_pages: usize) -> FranzServer {
        let (mut ghost_tx, mut ghost_rx) = ringbuf::new(path, max_pages).unwrap();
        ghost_tx.super_unsafe_page_cleanup_never_call_this_unless_you_know_what_youre_doing();
        ghost_rx.super_unsafe_page_cleanup_never_call_this_unless_you_know_what_youre_doing();

        let (stop_tx, stop_rx) = watch::channel(());

        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            let _ = stop_tx.send(());
        });

        let (a, b, c, d) = Self::DEFAULT_IPV4;

        let addr_producer = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(a, b, c, d)),
            Self::DEFAULT_PRODUCER_PORT,
        );
        let addr_consumer = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(a, b, c, d)),
            Self::DEFAULT_CONSUMER_PORT,
        );

        FranzServer {
            addr_producer,
            addr_consumer,
            ghost_rx,
            ghost_tx,
            stop_rx,
        }
    }

    async fn producer_client_handler(&self) -> Result<(), io::Error> {
        let listener = TcpListener::bind(&self.addr_producer).await?;

        loop {
            let (socket, addr) = match listener.accept().await {
                Ok((s, a)) => (s, a),
                Err(e) => {
                    log::error!("failed to accept connection: {:?}", e);
                    continue;
                }
            };

            log::info!("({}) accepted a producer client", &addr);

            let mut tx = self.ghost_tx.clone();

            tokio::spawn(async move {
                let stream = tokio::io::BufReader::new(socket);
                let mut stream_lines = stream.lines();
                while let Some(line) = stream_lines.next_line().await.unwrap() {
                    tx.push(line).unwrap();
                }

                log::info!("({}) disconnected", &addr);
            });
        }
    }

    async fn consumer_client_handler(&self) -> Result<(), io::Error> {
        let listener = TcpListener::bind(&self.addr_consumer).await?;

        loop {
            let (mut socket, addr) = match listener.accept().await {
                Ok((s, a)) => (s, a),
                Err(e) => {
                    log::error!("failed to accept connection: {:?}", e);
                    continue;
                }
            };

            log::info!("({}) accepted a consumer client", &addr);

            let rx = self.ghost_rx.clone();

            tokio::spawn(async move {
                // can make BufWriter if needed
                for msg in rx {
                    match msg {
                        Err(e) => {
                            log::error!("{e}");
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                        Ok(None) => {
                            // eventually get rid of this and just wait for consumer to poll
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                        Ok(Some(msg)) => {
                            log::trace!("{msg}");
                            socket.write_all(msg.as_bytes()).await.unwrap();
                            socket.write(&[b'\n']).await.unwrap();
                        }
                    }
                }

                log::info!("({}) disconnected", &addr);
            });
        }
    }

    pub async fn run(self) {
        log::debug!("starting queue server...");

        let mut stop_rx_clone = self.stop_rx.clone();
        let self_clone = self.clone();
        let producer_task = tokio::spawn(async move {
            select! {
                _ = stop_rx_clone.changed() => {},
                _ = self_clone.producer_client_handler() => {}
            }
        });

        let mut stop_rx_clone = self.stop_rx.clone();
        let self_clone = self.clone();
        let consumer_task = tokio::spawn(async move {
            select! {
                _ = stop_rx_clone.changed() => {},
                _ = self_clone.consumer_client_handler() => {}
            }
        });

        log::info!("queue server ready");
        producer_task.await.unwrap();
        consumer_task.await.unwrap();
    }
}

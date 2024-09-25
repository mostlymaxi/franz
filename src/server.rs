use disk_ringbuffer::ringbuf::{self, DiskRing};
use num_derive::FromPrimitive;
use std::fs;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};

use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::{select, signal};

#[derive(Clone)]
pub struct FranzServer {
    server_path: PathBuf,
    sock_addr: SocketAddr,
    stop_rx: watch::Receiver<()>,
    _default_max_pages: usize,
}

#[derive(thiserror::Error, Debug)]
pub enum FranzError {
    #[error(transparent)]
    RingbufError(#[from] ringbuf::RingbufError),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

#[repr(u8)]
#[derive(Debug, FromPrimitive)]
enum ClientKind {
    Produce = 0,
    Consume = 1,
    Info = 2,
}

impl FranzServer {
    // pub const DEFAULT_IPV4: (u8, u8, u8, u8) = (0, 0, 0, 0);

    pub fn new(server_path: PathBuf, port: u16, _default_max_pages: usize) -> FranzServer {
        let (stop_tx, stop_rx) = watch::channel(());

        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            let _ = stop_tx.send(());
        });

        // let (a, b, c, d) = Self::DEFAULT_IPV4;

        // let sock_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(a, b, c, d)), port);
        let sock_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port);

        FranzServer {
            sock_addr,
            _default_max_pages,
            server_path,
            stop_rx,
        }
    }

    async fn handle_produce<P: AsRef<Path>>(
        &self,
        sock: TcpStream,
        addr: SocketAddr,
        topic: P,
    ) -> Result<(), FranzError> {
        fs::create_dir_all(self.server_path.join(&topic))?;
        let mut tx = DiskRing::<ringbuf::Sender>::new(self.server_path.join(topic))?;

        log::info!("({}) accepted producer", &addr);

        tokio::spawn(async move {
            let stream = BufReader::new(sock);
            let mut stream_lines = stream.lines();
            while let Some(line) = stream_lines.next_line().await? {
                log::trace!("{line}");
                tx.push(line)?;
            }

            log::info!("({}) disconnected", &addr);
            Ok::<(), FranzError>(())
        });

        Ok(())
    }

    async fn handle_consume(
        &self,
        mut sock: TcpStream,
        addr: SocketAddr,
        topic: String,
    ) -> Result<(), FranzError> {
        fs::create_dir_all(self.server_path.join(&topic))?;
        let rx = DiskRing::<ringbuf::Receiver>::new(self.server_path.join(topic))?;

        log::info!("({}) accepted consumer", &addr);

        tokio::spawn(async move {
            let mut poll = String::new();
            let (sock_rdr, sock_wtr) = sock.split();
            let mut sock_rdr = BufReader::new(sock_rdr);
            let mut sock_wtr = BufWriter::new(sock_wtr);

            // can make BufWriter if needed
            for msg in rx {
                match msg? {
                    None => {
                        sock_rdr.read_line(&mut poll).await?;
                        poll.clear();
                    }
                    Some(msg) => {
                        log::trace!("{msg}");
                        sock_wtr.write_all(msg.as_bytes()).await?;
                        sock_wtr.write_all(&[b'\n']).await?;
                        sock_wtr.flush().await?;
                    }
                }
            }

            log::info!("({}) disconnected", &addr);
            Ok::<(), FranzError>(())
        });

        Ok(())
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

            log::info!("({}) new client", &addr);

            // "0" => PRODUCE
            // "1" => CONSUME
            let mut client_kind = String::new();
            if let Err(e) = sock_rdr.read_line(&mut client_kind).await {
                log::error!("{e}");
                continue;
            }
            let client_kind = match client_kind.trim().parse::<u8>() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("{e}");
                    continue;
                }
            };
            let client_kind = match num::FromPrimitive::from_u8(client_kind) {
                Some(c) => c,
                None => {
                    log::error!("failed to parse client kind {client_kind}");
                    continue;
                }
            };
            // \n

            // topic_name
            let mut topic = String::new();
            if let Err(e) = sock_rdr.read_line(&mut topic).await {
                log::error!("{e}");
                continue;
            }
            let topic = topic.trim().to_string();
            // \n

            let _ = match client_kind {
                ClientKind::Produce => self.handle_produce(socket, addr, topic).await,
                ClientKind::Consume => self.handle_consume(socket, addr, topic).await,
                ClientKind::Info => unreachable!(),
            };
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

use disk_ringbuffer::ringbuf::{self, DiskRing};
use num_derive::FromPrimitive;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::timeout;

use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::{select, signal};

#[derive(Clone)]
pub struct FranzServer {
    server_path: PathBuf,
    sock_addr: SocketAddr,
    stop_rx: watch::Receiver<()>,
    default_max_pages: usize,
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

// TODO: tracing instead of logging
impl FranzServer {
    pub fn new(
        server_path: PathBuf,
        bind_ip: IpAddr,
        port: u16,
        default_max_pages: usize,
    ) -> FranzServer {
        let (stop_tx, stop_rx) = watch::channel(());

        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            let _ = stop_tx.send(());
        });

        let sock_addr = SocketAddr::new(bind_ip, port);

        FranzServer {
            sock_addr,
            default_max_pages,
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
        let mut tx = DiskRing::<ringbuf::Sender>::new(self.server_path.join(&topic))?;
        let max_qpages = disk_ringbuffer::ringbuf::get_or_update_max_qpage(
            self.server_path.join(&topic),
            self.default_max_pages,
        )?;

        // TODO: fix what gets printed
        log::info!(
            "({}) accepted producer for {:?} with {} max qpages",
            &addr,
            topic.as_ref(),
            max_qpages
        );

        tokio::spawn(async move {
            let stream = BufReader::new(sock);
            let mut stream_lines = stream.lines();
            // TODO: Select
            while let Some(line) = stream_lines.next_line().await? {
                log::trace!("{line}");
                tx.push(line)?;
            }

            log::info!("({}) disconnected", &addr);
            Ok::<(), FranzError>(())
        });

        Ok(())
    }

    async fn handle_consume<P: AsRef<Path>>(
        &self,
        mut sock: TcpStream,
        addr: SocketAddr,
        topic: P,
    ) -> Result<(), FranzError> {
        fs::create_dir_all(self.server_path.join(&topic))?;
        let rx = DiskRing::<ringbuf::Receiver>::new(self.server_path.join(&topic))?;
        let max_qpages = disk_ringbuffer::ringbuf::get_or_update_max_qpage(
            self.server_path.join(&topic),
            self.default_max_pages,
        )?;

        log::info!(
            "({}) accepted consumer for {:?} with {} max qpages",
            &addr,
            topic.as_ref(),
            max_qpages
        );

        let stop_rx_clone = self.stop_rx.clone();
        let (timeout_tx, timeout_rx) = tokio::sync::watch::channel(());
        let (sock_rdr, sock_wtr) = sock.into_split();

        // KEEPALIVE
        tokio::spawn(async move {
            let mut poll = String::new();
            let mut sock_rdr = BufReader::new(sock_rdr);

            loop {
                match timeout(Duration::from_secs(75), sock_rdr.read_line(&mut poll)).await {
                    Ok(_) => {}
                    Err(_) => {
                        log::warn!(
                            "({}) failed to PING within 75 seconds... disconnecting",
                            &addr
                        );
                        break;
                    }
                }

                match poll.trim() {
                    "PING" => {}
                    _ => break,
                }

                poll.clear();
            }

            let _ = timeout_tx.send(());
        });

        tokio::spawn(async move {
            let mut sock_wtr = BufWriter::new(sock_wtr);

            // can make BufWriter if needed
            for msg in rx {
                match msg {
                    Err(_) => break,
                    Ok(None) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        // TODO: SERVER SIDE POLLING
                        // hey server, any new messages bub??
                        // yep! here's 1000 new messages
                        // nope D:
                        //
                        // TODO:
                        // INSTEAD
                        // a single thread that polls for new messages
                        // on new messages wake other threads
                    }
                    Ok(Some(msg)) => {
                        log::trace!("{msg}");
                        if sock_wtr.write_all(msg.as_bytes()).await.is_err() {
                            break;
                        };
                        if sock_wtr.write_all(&[b'\n']).await.is_err() {
                            break;
                        };
                        if sock_wtr.flush().await.is_err() {
                            break;
                        };
                    }
                }

                if stop_rx_clone.has_changed().unwrap_or(true) {
                    break;
                }

                if timeout_rx.has_changed().unwrap_or(true) {
                    break;
                }
            }

            log::info!("({}) disconnected", &addr);
        });

        Ok(())
    }

    async fn client_handler(&self) -> Result<(), io::Error> {
        let listener = TcpListener::bind(&self.sock_addr).await?;

        loop {
            // TODO: Select
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
                log::error!("({}) {e}", &addr);
                continue;
            }
            let client_kind = match client_kind.trim().parse::<u8>() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("({}) {e}", &addr);
                    continue;
                }
            };
            let client_kind = match num::FromPrimitive::from_u8(client_kind) {
                Some(c) => c,
                None => {
                    log::error!("({}) failed to parse client kind {client_kind}", &addr);
                    continue;
                }
            };
            // \n

            // topic_name
            let mut topic = String::new();
            if let Err(e) = sock_rdr.read_line(&mut topic).await {
                log::error!("({}) {e}", &addr);
                continue;
            }
            let topic = topic.trim().to_string();

            let _ = match client_kind {
                ClientKind::Produce => self.handle_produce(socket, addr, topic).await,
                ClientKind::Consume => self.handle_consume(socket, addr, topic).await,
                ClientKind::Info => unreachable!(),
            };
        }
    }

    pub async fn run(self) {
        log::info!("starting queue server...");

        let mut stop_rx_clone = self.stop_rx.clone();
        let self_clone = self.clone();

        let client_handler = tokio::spawn(async move {
            select! {
                _ = stop_rx_clone.changed() => {},
                _ = self_clone.client_handler() => {}
            }
        });

        log::info!("queue server ready!");
        client_handler.await.unwrap();

        log::info!("shutting down...")
    }
}

use disk_mpmc::manager::DataPagesManager;
use disk_mpmc::{Grouped, Receiver, Sender};
use num_derive::FromPrimitive;
use std::collections::HashMap;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, instrument, trace, warn};

use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::{select, signal};

pub struct FranzServer {
    server_path: PathBuf,
    topics: HashMap<PathBuf, DataPagesManager>,
    sock_addr: SocketAddr,
    twitter: CancellationToken,
    tracker: TaskTracker, // default_max_pages: usize,
}

#[repr(u8)]
#[derive(Debug, FromPrimitive)]
enum ClientKind {
    Produce = 0,
    Consume = 1,
    Info = 2,
}

impl FranzServer {
    #[instrument]
    pub fn new(server_path: PathBuf, bind_ip: IpAddr, port: u16) -> FranzServer {
        let sock_addr = SocketAddr::new(bind_ip, port);
        let topics = HashMap::new();
        let tracker = TaskTracker::new();
        let twitter = CancellationToken::new();

        let twitter_c = twitter.clone();

        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            twitter_c.cancel();
        });

        FranzServer {
            sock_addr,
            topics,
            server_path,
            tracker,
            twitter,
        }
    }

    #[instrument(err, skip(self, topic), fields(topic = %topic.as_ref().display()))]
    async fn handle_produce<P: AsRef<Path>>(
        &mut self,
        sock: TcpStream,
        topic: P,
    ) -> Result<(), std::io::Error> {
        let dp_man = match self.topics.get(topic.as_ref()) {
            Some(d) => d.clone(),
            None => {
                fs::create_dir_all(self.server_path.join(&topic))?;
                let d = DataPagesManager::new(self.server_path.join(&topic))?;

                self.topics.insert(topic.as_ref().into(), d.clone());

                debug!(topic = %topic.as_ref().display(), "created new topic");

                d
            }
        };

        let tx = Sender::new(dp_man.clone())?;
        let twitter_c = self.twitter.clone();

        self.tracker.spawn(async move {
            let stream = BufReader::new(sock);
            let mut stream_lines = stream.lines();

            while let Some(line) = select! {
                biased;

                _ = twitter_c.cancelled() => Ok(None),
                line = stream_lines.next_line() => line,
            }? {
                trace!(%line);
                tx.push(line)?;
            }

            info!("client disconnected");

            Ok::<(), std::io::Error>(())
        });

        Ok(())
    }

    // TODO: renamed and clean up
    async fn push_messages(
        sock: OwnedWriteHalf,
        rx: Receiver<Grouped>,
    ) -> Result<(), std::io::Error> {
        let mut sock_wtr = BufWriter::new(sock);

        // epoll struct
        // sync / send safe
        // AND doesn't pop a message
        // ^^^ easy to implement
        loop {
            let msg = tokio::task::block_in_place(|| rx.pop())?;
            sock_wtr.write_all(msg).await?;
            sock_wtr.write_all(b"\n").await?;
            sock_wtr.flush().await?;
        }

        #[allow(unreachable_code)]
        Ok::<(), std::io::Error>(())
    }

    #[instrument(err, skip(self, topic), fields(topic = %topic.as_ref().display()))]
    async fn handle_consume<P: AsRef<Path>>(
        &mut self,
        sock: TcpStream,
        group: usize,
        topic: P,
    ) -> Result<(), std::io::Error> {
        let dp_man = match self.topics.get(topic.as_ref()) {
            Some(d) => d.clone(),
            None => {
                fs::create_dir_all(self.server_path.join(&topic))?;
                let d = DataPagesManager::new(self.server_path.join(&topic))?;

                self.topics.insert(topic.as_ref().into(), d.clone());

                d
            }
        };

        // TODO: assert that group is less than
        // maximum groups
        let rx = Receiver::new(group, dp_man.clone())?;

        info!(topic = %topic.as_ref().display(), "accepted consumer");

        let timeout_token = CancellationToken::new();
        let (sock_rdr, sock_wtr) = sock.into_split();

        let timeout_token_c = timeout_token.clone();
        self.tracker.spawn(async move {
            let mut poll = String::new();
            let mut sock_rdr = BufReader::new(sock_rdr);

            loop {
                match timeout(Duration::from_secs(75), sock_rdr.read_line(&mut poll)).await {
                    Ok(_) => debug!("keepalive"),
                    Err(_) => {
                        warn!("failed to PING within 75 seconds... disconnecting",);
                        break;
                    }
                }

                match poll.trim() {
                    "PING" => {}
                    _ => break,
                }

                poll.clear();
            }

            timeout_token_c.cancel();
        });

        let twitter_c = self.twitter.clone();
        self.tracker.spawn(async move {
            select! {
                _ = timeout_token.cancelled() => {}
                _ = twitter_c.cancelled() => {}
                _ = Self::push_messages(sock_wtr, rx) => {},
            }
        });

        Ok(())
    }

    #[instrument(skip(self))]
    async fn client_handler(&mut self) -> Result<(), io::Error> {
        let listener = TcpListener::bind(&self.sock_addr).await?;

        loop {
            // TODO: Select
            let (mut socket, addr) = match listener.accept().await {
                Ok((s, a)) => (s, a),
                Err(e) => {
                    error!("failed to accept connection: {:?}", e);
                    continue;
                }
            };

            let (sock_rdr, _) = socket.split();
            let mut sock_rdr = BufReader::new(sock_rdr);

            info!(%addr, "new client");

            // "0" => PRODUCE
            // "1" => CONSUME
            let mut client_kind = String::new();
            if let Err(e) = sock_rdr.read_line(&mut client_kind).await {
                error!(%addr, %e);
                continue;
            }
            let client_kind = match client_kind.trim().parse::<u8>() {
                Ok(c) => c,
                Err(e) => {
                    error!(%addr, %e);
                    continue;
                }
            };
            let client_kind = match num::FromPrimitive::from_u8(client_kind) {
                Some(c) => c,
                None => {
                    error!(%addr, %client_kind, "failed to parse client kind");
                    continue;
                }
            };

            let mut topic = String::new();
            if let Err(e) = sock_rdr.read_line(&mut topic).await {
                error!(%addr, %e);
                continue;
            }
            let topic = topic.trim().to_string();

            let _ = match client_kind {
                ClientKind::Produce => self.handle_produce(socket, topic).await,
                ClientKind::Consume => self.handle_consume(socket, 0, topic).await,
                ClientKind::Info => unreachable!(),
            };
        }
    }

    pub async fn run(mut self) {
        info!("starting queue server...");

        let twitter_c = self.twitter.clone();

        select! {
            _ = twitter_c.cancelled() => {},
            _ = self.client_handler() => {}
        }

        self.twitter.cancel();
        self.tracker.close();
        if let Err(e) = timeout(Duration::from_secs(60), self.tracker.wait()).await {
            error!(%e, "failed to shutdown within timeout");
        }

        info!("shutting down...")
    }
}

use disk_mpmc::manager::DataPagesManager;
use disk_mpmc::{Grouped, Receiver, Sender};
use num_derive::FromPrimitive;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, BufWriter, Lines, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{fs, thread};

use tracing::{debug, error, info, instrument, trace, warn};
//
//use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
//use tokio::net::tcp::OwnedWriteHalf;
//use tokio::net::{TcpListener, TcpStream};
//use tokio::task::yield_now;
//use tokio::time::timeout;
//use tokio::{select, signal};
//use tokio_util::sync::CancellationToken;
//use tokio_util::task::TaskTracker;

pub struct FranzServer {
    server_path: PathBuf,
    topics: HashMap<PathBuf, DataPagesManager>,
    sock_addr: SocketAddr,
    //twitter: CancellationToken,
    //tracker: TaskTracker, // default_max_pages: usize,
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
        //let tracker = TaskTracker::new();
        //let twitter = CancellationToken::new();

        //let twitter_c = twitter.clone();
        //
        //tokio::spawn(async move {
        //    let _ = signal::ctrl_c().await;
        //    twitter_c.cancel();
        //});

        FranzServer {
            sock_addr,
            topics,
            server_path,
            //tracker,
            //twitter,
        }
    }

    #[instrument(err, skip(self, topic), fields(topic = %topic.as_ref().display()))]
    fn handle_produce<P: AsRef<Path>>(
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

        let mut tx = Sender::new(dp_man.clone())?;
        std::thread::spawn(move || {
            let stream = BufReader::new(sock);
            let mut stream_lines = stream.lines();

            while let Some(line) = stream_lines.next() {
                let line = line?;

                trace!(%line);
                tx.push(line)?;
            }

            info!("client disconnected");

            Ok::<(), std::io::Error>(())
        });

        Ok(())
    }

    fn push_messages(sock: TcpStream, mut rx: Receiver<Grouped>) -> Result<(), std::io::Error> {
        let mut sock_wtr = BufWriter::new(sock);

        loop {
            let msg = rx.pop()?;
            sock_wtr.write_all(msg)?;
            sock_wtr.write_all(b"\n")?;
            sock_wtr.flush()?;
        }

        #[allow(unreachable_code)]
        Ok::<(), std::io::Error>(())
    }

    #[instrument]
    fn keepalive(sock: TcpStream, timeout: Duration) {
        let mut poll = String::new();
        if let Err(e) = sock.set_read_timeout(Some(timeout)) {
            error!(%e);
            let _ = sock.shutdown(std::net::Shutdown::Both);
            return;
        }
        let mut sock_rdr = BufReader::new(sock);

        loop {
            match sock_rdr.read_line(&mut poll) {
                Ok(_) => debug!("keepalive"),
                Err(_) => {
                    warn!("failed to PING within 75 seconds... disconnecting",);
                    let _ = sock_rdr.into_inner().shutdown(std::net::Shutdown::Both);
                    break;
                }
            }

            match poll.trim() {
                "PING" => {}
                _ => break,
            }

            poll.clear();
        }
    }

    #[instrument(err, skip(self, topic), fields(topic = %topic.as_ref().display()))]
    fn handle_consume<P: AsRef<Path>>(
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

        let sock_c = sock.try_clone()?;

        std::thread::spawn(move || Self::keepalive(sock_c, Duration::from_secs(75)));
        std::thread::spawn(move || Self::push_messages(sock, rx));

        Ok(())
    }

    #[instrument(skip(self))]
    fn client_handler(&mut self) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(self.sock_addr)?;

        loop {
            // TODO: Select
            let (socket, addr) = match listener.accept() {
                Ok((s, a)) => (s, a),
                Err(e) => {
                    error!("failed to accept connection: {:?}", e);
                    continue;
                }
            };

            if let Err(e) = socket.set_read_timeout(Some(Duration::from_secs(10))) {
                error!(?socket, %e);
                continue;
            };

            let Ok(socket_c) = socket.try_clone() else {
                error!(?socket, "failed to clone socket");
                continue;
            };

            let mut sock_rdr = BufReader::new(socket_c);

            info!(%addr, "new client");

            // "0" => PRODUCE
            // "1" => CONSUME
            let mut client_kind = String::new();
            if let Err(e) = sock_rdr.read_line(&mut client_kind) {
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
            if let Err(e) = sock_rdr.read_line(&mut topic) {
                error!(%addr, %e);
                continue;
            }
            let topic = topic.trim().to_string();

            if let Err(e) = socket.set_read_timeout(None) {
                error!(?socket, %e);
            }

            let _ = match client_kind {
                ClientKind::Produce => self.handle_produce(socket, topic),
                ClientKind::Consume => self.handle_consume(socket, 0, topic),
                ClientKind::Info => unreachable!(),
            };
        }
    }

    pub fn run(mut self) {
        info!("starting queue server...");

        let running = Arc::new(AtomicBool::new(true));
        let r = running.clone();

        ctrlc::set_handler(move || {
            r.store(false, Ordering::Relaxed);
        })
        .expect("Error setting Ctrl-C handler");

        let handle = std::thread::spawn(move || self.client_handler().unwrap());

        while running.load(Ordering::Relaxed) {
            if handle.is_finished() {
                handle.join().unwrap();
                break;
            }

            std::thread::sleep(Duration::from_secs(5));
        }

        info!("shutting down...")
    }
}

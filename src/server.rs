use disk_mpmc::manager::DataPagesManager;
use disk_mpmc::{GenReceiver, Receiver, Sender};
use std::collections::HashMap;
use std::ffi::OsString;
use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, error, info, instrument, trace, warn};

use crate::protocol;

pub struct FranzServer {
    server_path: PathBuf,
    topics: HashMap<OsString, DataPagesManager>,
    sock_addr: SocketAddr,
    running: Arc<AtomicBool>,
}

impl FranzServer {
    #[instrument]
    pub fn new(server_path: PathBuf, bind_ip: IpAddr, port: u16) -> FranzServer {
        let sock_addr = SocketAddr::new(bind_ip, port);
        let topics = HashMap::new();

        let running = Arc::new(AtomicBool::new(true));
        let r = running.clone();

        ctrlc::set_handler(move || {
            r.store(false, Ordering::Relaxed);
        })
        .expect("Error setting Ctrl-C handler");

        FranzServer {
            sock_addr,
            topics,
            server_path,
            running,
        }
    }

    #[instrument(err, skip(self, topic), fields(topic = %topic.as_ref().display()))]
    fn handle_produce<P: AsRef<Path>>(
        &mut self,
        sock: TcpStream,
        topic: P,
    ) -> Result<(), std::io::Error> {
        let Some(topic) = topic.as_ref().file_name() else {
            return Err(std::io::Error::other("unable to parse topic"));
        };

        let dp_man = match self.topics.get(topic) {
            Some(d) => d.clone(),
            None => {
                fs::create_dir_all(self.server_path.join(topic))?;
                let d = DataPagesManager::new(self.server_path.join(topic))?;

                self.topics.insert(topic.into(), d.clone());

                debug!(topic = ?topic, "created new topic");

                d
            }
        };

        let mut tx = Sender::new(dp_man.clone())?;
        std::thread::spawn(move || {
            let stream = BufReader::new(sock);

            for line in stream.lines() {
                let line = line?;

                trace!(%line);
                tx.push(line)?;
            }

            info!("client disconnected");

            Ok::<(), std::io::Error>(())
        });

        Ok(())
    }

    fn push_messages<R: GenReceiver>(sock: TcpStream, mut rx: R) -> Result<(), std::io::Error> {
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
        group: Option<u16>,
        topic: P,
    ) -> Result<(), std::io::Error> {
        let Some(topic) = topic.as_ref().file_name() else {
            return Err(std::io::Error::other("unable to parse topic"));
        };

        let dp_man = match self.topics.get(topic) {
            Some(d) => d.clone(),
            None => {
                fs::create_dir_all(self.server_path.join(topic))?;
                let d = DataPagesManager::new(self.server_path.join(topic))?;

                self.topics.insert(topic.into(), d.clone());

                d
            }
        };

        info!(topic = ?topic, "accepted consumer");

        let sock_c = sock.try_clone()?;

        std::thread::spawn(move || Self::keepalive(sock_c, Duration::from_secs(75)));

        std::thread::spawn(move || match group {
            Some(g) => {
                let rx = Receiver::new(g.into(), dp_man).unwrap();

                Self::push_messages(sock, rx).unwrap();
            }
            None => {
                let rx = Receiver::new_anon(dp_man).unwrap();
                Self::push_messages(sock, rx).unwrap();
            }
        });

        Ok(())
    }

    #[instrument(skip(self))]
    fn client_handler(&mut self) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(self.sock_addr)?;

        loop {
            if self.running.load(Ordering::Relaxed) {
                break;
            }

            let (mut sock, _) = match listener.accept() {
                Ok((s, a)) => (s, a),
                Err(e) => {
                    error!(%e, "failed to accept connection:");
                    continue;
                }
            };

            if self.running.load(Ordering::Relaxed) {
                break;
            }

            let Some(handshake) = protocol::Handshake::parse(&mut sock) else {
                error!(?sock, "failed to parse handshake:");
                continue;
            };

            if let Err(e) = match handshake.api.as_str() {
                "produce" => self.handle_produce(sock, handshake.topic),
                "consume" => self.handle_consume(sock, handshake.group, handshake.topic),
                "info" => Ok(()),
                _ => Ok(()),
            } {
                error!(%e);
            }
        }

        Ok(())
    }

    pub fn run(mut self) {
        info!(%self.sock_addr, "starting franz server:");

        let running = self.running.clone();

        let handle = std::thread::spawn(move || self.client_handler().unwrap());

        while running.load(Ordering::Relaxed) {
            if handle.is_finished() {
                handle.join().unwrap();
                break;
            }

            std::thread::sleep(Duration::from_secs(2));
        }

        info!("shutting down...")
    }
}

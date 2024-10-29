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

use tracing::{debug, debug_span, error, info, info_span, instrument, trace, trace_span, warn};

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

        info!(topic = ?topic, "accepted producer");

        let thread_span = trace_span!("producer_thread");

        let mut tx = Sender::new(dp_man.clone())?;
        std::thread::spawn(move || {
            let _entered = thread_span.entered();

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

            // WARN: this needs to be feature flagged
            let msg = String::from_utf8_lossy(msg);
            trace!(%msg);
        }

        #[allow(unreachable_code)]
        Ok::<(), std::io::Error>(())
    }

    #[instrument(skip(sock))]
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
                // TODO: check if error is actually a timeout
                // or something else
                Err(_) => {
                    warn!("failed to PING within 75 seconds... disconnecting",);
                    let _ = sock_rdr.into_inner().shutdown(std::net::Shutdown::Both);
                    break;
                }
            }

            match poll.trim() {
                "PING" => {}
                m => warn!(%m, "recieved keepalive message that was not 'PING'... exiting"),
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

        let topic = topic.to_owned();

        let dp_man = match self.topics.get(&topic) {
            Some(d) => d.clone(),
            None => {
                fs::create_dir_all(self.server_path.join(&topic))?;
                let d = DataPagesManager::new(self.server_path.join(&topic))?;

                self.topics.insert(topic.clone(), d.clone());

                debug!(topic = ?topic, "created new topic");

                d
            }
        };

        let sock_c = sock.try_clone()?;

        let keepalive_span = info_span!("keepalive_thread");
        std::thread::spawn(move || {
            let _entered = keepalive_span.entered();
            Self::keepalive(sock_c, Duration::from_secs(75))
        });

        let thread_span = trace_span!("consumer_thread");
        std::thread::spawn(move || match group {
            Some(g) => {
                let _entered = thread_span.entered();
                let rx = Receiver::new(g.into(), dp_man).unwrap();

                info!(topic = ?topic, group = ?g, "accepted consumer");

                Self::push_messages(sock, rx).unwrap();
            }
            None => {
                let _entered = thread_span.entered();
                let rx = Receiver::new_anon(dp_man).unwrap();

                info!(topic = ?topic, "accepted anonymous consumer");
                Self::push_messages(sock, rx).unwrap();
            }
        });

        Ok(())
    }

    #[instrument(skip(self))]
    fn client_handler(&mut self) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(self.sock_addr)?;

        loop {
            if !self.running.load(Ordering::Relaxed) {
                debug!("exiting client handler (1)");
                break;
            }

            let (mut sock, _) = match listener.accept() {
                Ok((s, a)) => (s, a),
                Err(e) => {
                    error!(%e, "failed to accept connection:");
                    continue;
                }
            };

            if !self.running.load(Ordering::Relaxed) {
                debug!("exiting client handler (2)");
                break;
            }

            let handshake = match protocol::Handshake::try_parse(&mut sock) {
                Ok(h) => h,
                Err(e) => {
                    error!(?sock, ?e, "failed to parse handshake");
                    continue;
                }
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

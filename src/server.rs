use crate::client::FranzClient;
use crate::protocol;
use disk_ringbuffer::ringbuf::{self, Writer};
use memmap2::MmapMut;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::{select, signal};

// mmap -> cast to struct
// doing that in rust sucks ^
// ptr = mmap(file);
//
// struct = (struct*) ptr
//
// atomic_fetch_inc(struct->idx, 1);
//

#[repr(C)]
struct ConsumerState {
    idx: AtomicUsize,
    consumers: [Consumer; 1024],
}

#[repr(C)]
struct Consumer {
    last_idx: usize,
    id: usize,
}

#[derive(Clone)]
pub struct FranzServer {
    broker: BrokerInfo,
    ghost_rx: ringbuf::Reader,
    ghost_tx: ringbuf::Writer,
    stop_rx: watch::Receiver<()>,
}

// on produce req:
//    match tx {
//        None => self.tx = Some(new writer),
//        Some(tx) => just write bro
//    }

#[derive(Clone)]
pub struct BrokerInfo {
    node_id: usize,
    host: SocketAddr,
}

impl FranzServer {
    pub const DEFAULT_MAX_PAGES: usize = 0;
    pub const DEFAULT_PORT: u16 = 8084;
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

        let host = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(a, b, c, d)), Self::DEFAULT_PORT);

        let broker = BrokerInfo { node_id: 0, host };

        FranzServer {
            broker,
            ghost_rx,
            ghost_tx,
            stop_rx,
        }
    }

    pub fn get_producer(&self) -> Writer {
        self.ghost_tx.clone()
    }

    pub fn get_consumer(&self) -> Reader {
        self.ghost_rx.clone()
    }

    async fn client_handler(&self) -> Result<(), io::Error> {
        let listener = TcpListener::bind(&self.broker.host).await?;
        let server = Arc::new(self);

        loop {
            let (socket, addr) = match listener.accept().await {
                Ok((s, a)) => (s, a),
                Err(e) => {
                    log::error!("failed to accept connection: {:?}", e);
                    continue;
                }
            };

            log::info!("({}) accepted a producer client", &addr);

            let client = FranzClient::new(server.clone(), socket);

            // let mut tx = self.ghost_tx.clone();

            tokio::spawn(async move {
                client.handle_client(socket);
                protocol::handle_client(socket).await;
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
                _ = self_clone.client_handler() => {}
            }
        });

        let mut stop_rx_clone = self.stop_rx.clone();
        let self_clone = self.clone();
        let consumer_task = tokio::spawn(async move {
            select! {
                _ = stop_rx_clone.changed() => {},
                _ = self_clone.client_handler() => {}
            }
        });

        log::info!("queue server ready");
        producer_task.await.unwrap();
        consumer_task.await.unwrap();
    }
}
// struct ConsumerState {
//     idx: usize,
//     consumers: [Consumer; 1024],
// }

#[test]
fn test_mmap() {
    let f = std::fs::File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open("test")
        .unwrap();

    f.set_len(std::mem::size_of::<ConsumerState>() as u64)
        .unwrap();

    let consumer_state = unsafe {
        let m = memmap2::MmapMut::map_mut(&f).unwrap();
        let m = Box::new(m);
        let m: &'static MmapMut = Box::leak(m);

        let consumer_state: &mut ConsumerState = std::mem::transmute(m.as_ptr());

        consumer_state
    };

    consumer_state.idx += 1;

    unsafe {
        let map: *mut MmapMut = std::mem::transmute(consumer_state);
        drop(map);
    }
}

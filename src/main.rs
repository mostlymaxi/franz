mod client;
mod protocol;
mod server;
use clap::Parser;
use std::path::PathBuf;
use tokio_util::bytes::{Buf, BytesMut};

// a server
// handle writers:
// - give writers a copy of a sender (ringbuf::Writer)
// - listen to new messages
// - dump to ringbuf
// handle readers:
// - give readers a copy of a receiver (ringbuf::Reader)
// - wait for polling or something?
// - dump new messages to socket
//
// make compatible with the kafka protocol
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    path: PathBuf,
    #[arg(short, long, default_value_t = 0)]
    max_pages: usize,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Args::parse();

    let server = server::FranzServer::new(&args.path, args.max_pages);
    server.run().await;
}

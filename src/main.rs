mod protocol;
mod server;
use clap::Parser;
use std::{
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    path: PathBuf,
    #[arg(long, default_value_t = IpAddr::V4(Ipv4Addr::UNSPECIFIED))]
    bind_ip: IpAddr,
    #[arg(long, default_value_t = 8085)]
    port: u16,
    #[arg(short, long, default_value_t = 0)]
    max_pages: usize,
}

fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let server = server::FranzServer::new(args.path, args.bind_ip, args.port);
    server.run();
}

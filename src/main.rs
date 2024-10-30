mod protocol;
mod server;
use clap::Parser;
use serde::Deserialize;
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

#[derive(Deserialize)]
struct Config {
    data_dir: String,
    #[serde(default = "default_listener")]
    listener: IpAddr,
    #[serde(default = "default_port")]
    port: u16,
    // TODO: this should be a max size in bytes
    // and then we calculate the pages
    // because wtf is a page!?
    default_max_pages: usize,
    default_start_position: bool,
    auto_create_topic: bool,
}

fn default_listener() -> IpAddr {
    IpAddr::V4(Ipv4Addr::UNSPECIFIED)
}

fn default_port() -> u16 {
    8085
}

fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let server = server::FranzServer::new(args.path, args.bind_ip, args.port);
    server.run();
}

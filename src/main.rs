mod server;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    path: PathBuf,
    #[arg(long, default_value_t = 8085)]
    port: u16,
    #[arg(short, long, default_value_t = 0)]
    max_pages: usize,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Args::parse();
    let server = server::FranzServer::new(args.path, args.port, args.max_pages);
    server.run().await;
}

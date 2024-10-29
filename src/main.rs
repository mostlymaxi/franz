mod protocol;
mod server;
use clap::Parser;
use std::{
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
};

use opentelemetry::trace::{Tracer, TracerProvider as _};
use opentelemetry_sdk::trace::TracerProvider;
use tracing::{error, span};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

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
    // Create a new OpenTelemetry trace pipeline that prints to stdout
    let provider = TracerProvider::builder()
        .with_simple_exporter(opentelemetry_stdout::SpanExporter::default())
        .build();

    let tracer = provider.tracer("readme_example");

    // Create a tracing layer with the configured tracer
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    // Use the tracing subscriber `Registry`, or any other subscriber
    // that impls `LookupSpan`
    let subscriber = Registry::default().with(telemetry);

    // Trace executed code
    tracing::subscriber::set_global_default(subscriber).expect("to work");
    // Spans will be sent to the configured OpenTelemetry exporter

    error!("This event will be logged in the root span.");

    // tracing_subscriber::fmt::init();
    let args = Args::parse();
    let server = server::FranzServer::new(args.path, args.bind_ip, args.port);
    server.run();
}

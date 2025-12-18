use fast_socks5::client::{Config, Socks5Stream};
use std::net::SocketAddr;
use structopt::StructOpt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, StructOpt)]
struct Args {
    #[structopt(long)]
    socks_server: SocketAddr,

    #[structopt(long)]
    target_host: String,

    #[structopt(long)]
    target_port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::from_args();
    let mut stream = Socks5Stream::connect(
        args.socks_server,
        args.target_host,
        args.target_port,
        Config::default(),
    )
    .await?;

    stream.write_all(b"get").await?;

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await?;
    print!("{}", String::from_utf8_lossy(&buf));

    Ok(())
}


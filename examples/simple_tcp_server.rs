use log::{error, info};
use std::net::SocketAddr;
use structopt::StructOpt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Debug, StructOpt)]
struct Args {
    #[structopt(long, default_value = "127.0.0.1:8080")]
    listen_addr: SocketAddr,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::from_args();
    let listener = TcpListener::bind(args.listen_addr).await?;
    info!("TCP echo server listening on {}", args.listen_addr);

    loop {
        let (mut stream, peer_addr) = listener.accept().await?;
        tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            loop {
                let n = match stream.read(&mut buf).await {
                    Ok(0) => return,
                    Ok(n) => n,
                    Err(e) => {
                        error!("读取失败 ({}): {:?}", peer_addr, e);
                        return;
                    }
                };

                if let Err(e) = stream.write_all(&buf[..n]).await {
                    error!("写入失败 ({}): {:?}", peer_addr, e);
                    return;
                }
            }
        });
    }
}


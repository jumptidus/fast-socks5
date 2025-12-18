use fast_socks5::server::{DenyAuthentication, Socks5Server};
use log::{error, info};
use std::net::SocketAddr;
use structopt::StructOpt;
use tokio_stream::StreamExt;

#[derive(Debug, StructOpt)]
struct Args {
    #[structopt(long, default_value = "127.0.0.1:1337")]
    listen_addr: SocketAddr,

    #[structopt(long, default_value = "0")]
    udp_port: u16,

    #[structopt(long, default_value = "5")]
    udp_cleanup_interval: u64,

    #[structopt(long, default_value = "30")]
    udp_timeout: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::from_args();
    info!("SOCKS5 server listening on {}", args.listen_addr);

    let server = Socks5Server::<DenyAuthentication>::bind(
        args.listen_addr,
        args.udp_port,
        args.udp_cleanup_interval,
        args.udp_timeout,
    )
    .await?;

    let mut incoming = server.incoming();
    while let Some(socket_res) = incoming.next().await {
        match socket_res {
            Ok(socket) => {
                tokio::spawn(async move {
                    if let Err(e) = socket.upgrade_to_socks5().await {
                        error!("处理连接失败: {:?}", e);
                    }
                });
            }
            Err(e) => {
                error!("接受连接失败: {:?}", e);
            }
        }
    }

    Ok(())
}


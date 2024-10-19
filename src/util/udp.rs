use anyhow::{Context, Result};
use dashmap::{DashMap, DashSet};
use std::{
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
};
use tokio::net::UdpSocket;

use crate::{new_udp_header, parse_udp_request};

pub async fn listen_udp_request(udp_port: u16) -> Result<()> {
    let inbound = Arc::new(UdpSocket::bind(format!("127.0.0.1:{}", udp_port)).await?);

    debug!("listen udp request on 127.0.0.1:{}", udp_port);

    let outbound_map: Arc<DashMap<String, Arc<UdpSocket>>> = Arc::new(DashMap::new());
    let client_map: Arc<DashMap<String, SocketAddr>> = Arc::new(DashMap::new());
    let active_outbound_listener_ports: DashSet<u16> = DashSet::new();

    let mut buf = vec![0u8; 0x10000];
    loop {
        let (size, client_addr) = inbound.recv_from(&mut buf).await?;
        debug!("[UDP] Server recieve udp from {}", client_addr);

        let (frag, target_addr, data) = parse_udp_request(&buf[..size]).await?;

        if frag != 0 {
            debug!("[UDP] Discard UDP frag packets sliently.");
            return Ok(());
        }

        debug!("Server forward to packet to {}", target_addr);

        let mut target_addr = target_addr
            .to_socket_addrs()?
            .next()
            .context("unreachable")?;

        target_addr.set_ip(match target_addr.ip() {
            std::net::IpAddr::V4(v4) => std::net::IpAddr::V6(v4.to_ipv6_mapped()),
            v6 @ std::net::IpAddr::V6(_) => v6,
        });

        let outbound_key = format!(
            "{}:{}:{}:{}",
            target_addr.ip(),
            target_addr.port(),
            client_addr.ip(),
            client_addr.port()
        );
        if let Some(outbound) = outbound_map.get(&outbound_key) {
            outbound.send_to(data, target_addr).await?;
            client_map.insert(
                format!("{}:{}", target_addr.ip(), outbound.local_addr()?.port()),
                client_addr,
            );

            if !active_outbound_listener_ports.contains(&outbound.local_addr()?.port()) {
                active_outbound_listener_ports.insert(outbound.local_addr()?.port());
                tokio::spawn(listen_udp_response(
                    inbound.clone(),
                    outbound.clone(),
                    client_map.clone(),
                ));
            } else {
                debug!(
                    "Outbound listener port {} already exists",
                    outbound.local_addr()?.port()
                );
            }
        } else {
            let outbound = Arc::new(UdpSocket::bind("[::]:0").await?);
            outbound.send_to(data, target_addr).await?;
            client_map.insert(
                format!("{}:{}", target_addr.ip(), outbound.local_addr()?.port()),
                client_addr,
            );

            outbound_map.insert(outbound_key, outbound.clone());

            if !active_outbound_listener_ports.contains(&outbound.local_addr()?.port()) {
                active_outbound_listener_ports.insert(outbound.local_addr()?.port());
                tokio::spawn(listen_udp_response(
                    inbound.clone(),
                    outbound.clone(),
                    client_map.clone(),
                ));
            } else {
                debug!(
                    "Outbound listener port {} already exists",
                    outbound.local_addr()?.port()
                );
            }
        }
    }
}

async fn listen_udp_response(
    inbound: Arc<UdpSocket>,
    outbound: Arc<UdpSocket>,
    client_map: Arc<DashMap<String, SocketAddr>>,
) -> Result<()> {
    let mut buf = vec![0u8; 0x10000];
    loop {
        let (size, remote_addr) = outbound.recv_from(&mut buf).await?;
        debug!("Recieve packet from {}", remote_addr);

        let mut data = new_udp_header(remote_addr)?;
        data.extend_from_slice(&buf[..size]);

        let client_key = format!("{}:{}", remote_addr.ip(), outbound.local_addr()?.port());
        if let Some(client_addr) = client_map.get(&client_key) {
            inbound.send_to(&data, *client_addr).await?;
        } else {
            warn!("No client found for {}", client_key);
        }
    }
}

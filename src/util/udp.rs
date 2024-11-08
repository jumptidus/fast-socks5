use anyhow::{Context, Result};
use dashmap::{DashMap, DashSet};
use std::{
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{net::UdpSocket, sync::oneshot, task::JoinHandle};

use crate::{new_udp_header, parse_udp_request};

use super::target_addr::TargetAddr;

struct UdpManager {
    inbound: Arc<UdpSocket>,
    outbound_map: Arc<DashMap<String, (Arc<UdpSocket>, Instant)>>,
    client_map: Arc<DashMap<String, (SocketAddr, Instant)>>,
    active_outbound_listener_ports: DashSet<u16>,
    task_handles: DashMap<u16, (JoinHandle<Result<()>>, oneshot::Sender<()>)>,
}

impl UdpManager {
    async fn new(udp_port: u16) -> Result<Self> {
        let inbound = Arc::new(UdpSocket::bind(format!("127.0.0.1:{}", udp_port)).await?);
        info!("[UDP]listen udp request on 127.0.0.1:{}", udp_port);

        Ok(Self {
            inbound,
            outbound_map: Arc::new(DashMap::new()),
            client_map: Arc::new(DashMap::new()),
            active_outbound_listener_ports: DashSet::new(),
            task_handles: DashMap::new(),
        })
    }

    async fn handle_inbound_packet(
        &self,
        buf: &[u8],
        size: usize,
        client_addr: SocketAddr,
    ) -> Result<()> {
        let (frag, target_addr, data) = parse_udp_request(&buf[..size]).await?;

        if frag != 0 {
            debug!("[UDP] Discard UDP frag packets silently.");
            return Ok(());
        }

        trace!("[UDP]Server forward packet to {}", target_addr);

        let target_addr = self.normalize_target_addr(target_addr)?;
        let outbound_key = self.generate_outbound_key(&target_addr, &client_addr);

        self.process_outbound(outbound_key, target_addr, client_addr, data)
            .await
    }

    fn normalize_target_addr(&self, target_addr: TargetAddr) -> Result<SocketAddr> {
        let mut addr = target_addr
            .to_socket_addrs()?
            .next()
            .context("unreachable")?;
        addr.set_ip(match addr.ip() {
            std::net::IpAddr::V4(v4) => std::net::IpAddr::V6(v4.to_ipv6_mapped()),
            v6 @ std::net::IpAddr::V6(_) => v6,
        });
        Ok(addr)
    }

    fn generate_outbound_key(&self, target_addr: &SocketAddr, client_addr: &SocketAddr) -> String {
        format!(
            "{}:{}:{}:{}",
            target_addr.ip(),
            target_addr.port(),
            client_addr.ip(),
            client_addr.port()
        )
    }

    async fn process_outbound(
        &self,
        outbound_key: String,
        target_addr: SocketAddr,
        client_addr: SocketAddr,
        data: &[u8],
    ) -> Result<()> {
        let now = Instant::now();
        let outbound = if let Some(outbound) = self.outbound_map.get(&outbound_key) {
            outbound.0.clone()
        } else {
            let new_outbound = Arc::new(UdpSocket::bind("[::]:0").await?);
            self.outbound_map
                .insert(outbound_key, (new_outbound.clone(), now));
            new_outbound
        };

        outbound.send_to(data, target_addr).await?;
        self.client_map.insert(
            format!("{}:{}", target_addr.ip(), outbound.local_addr()?.port()),
            (client_addr, now),
        );
        trace!(
            "[UDP] Insert client {}:{} to client map",
            target_addr.ip(),
            outbound.local_addr()?.port()
        );

        self.spawn_listener_if_needed(outbound).await
    }

    async fn spawn_listener_if_needed(&self, outbound: Arc<UdpSocket>) -> Result<()> {
        let port = outbound.local_addr()?.port();
        trace!("[UDP] choose port {} listener", port);

        if !self.active_outbound_listener_ports.contains(&port) {
            trace!(
                "[UDP] new outbound listener port {} inserted to active outbound listener ports",
                port
            );
            self.active_outbound_listener_ports.insert(port);
            let inbound = self.inbound.clone();
            let client_map = self.client_map.clone();

            let (tx, rx) = oneshot::channel();
            let handle = tokio::spawn(listen_udp_response(inbound, outbound, client_map, rx));
            self.task_handles.insert(port, (handle, tx));
        } else {
            trace!("[UDP] outbound listener port {} already running", port);
        }
        Ok(())
    }

    fn cleanup_expired_sockets(&self, timeout: Duration) {
        trace!(
            "Cleanup expired sockets, sockets: {}, clients: {}",
            self.outbound_map.len(),
            self.client_map.len(),
        );

        let now = Instant::now();

        self.outbound_map.retain(|_, (socket, last_used)| {
            if now.duration_since(*last_used) > timeout {
                if let Ok(addr) = socket.local_addr() {
                    self.active_outbound_listener_ports.remove(&addr.port());
                    if let Some((_, (handle, tx))) = self.task_handles.remove(&addr.port()) {
                        trace!("Remove listener for port {}", addr.port());

                        let result = tx.send(());

                        if result.is_err() {
                            warn!(
                                "Failed to send stop signal to listener for port {}, force stop!",
                                addr.port()
                            );

                            let _ = handle.abort(); // force stop the listener
                        }
                    }
                }
                false
            } else {
                true
            }
        });

        self.client_map
            .retain(|_, (_, last_used)| now.duration_since(*last_used) <= timeout)
    }
}

pub async fn listen_udp_request(udp_port: u16, cleanup_interval: u64, timeout: u64) -> Result<()> {
    loop {
        match run_udp_server(udp_port, cleanup_interval, timeout).await {
            Ok(_) => {
                error!("[UDP] Server stopped unexpectedly");
            }
            Err(e) => {
                error!(
                    "[UDP] Server error: {:?}, attempting restart in 5 seconds",
                    e
                );
            }
        }

        // 等待5秒后重试
        tokio::time::sleep(Duration::from_secs(5)).await;
        info!("[UDP] Attempting to restart UDP server...");
    }
}

async fn run_udp_server(udp_port: u16, cleanup_interval: u64, timeout: u64) -> Result<()> {
    let state = UdpManager::new(udp_port).await?;
    let cleanup_interval = Duration::from_secs(cleanup_interval);
    let timeout = Duration::from_secs(timeout);

    let mut buf = vec![0u8; 0x10000];
    loop {
        tokio::select! {
            result = state.inbound.recv_from(&mut buf) => {
                if let Err(e) = handle_recv_result(&state, &buf, result).await {
                    error!("[UDP] Error handling packet: {:?}", e);
                    continue;
                }
            }
            _ = tokio::time::sleep(cleanup_interval) => {
                state.cleanup_expired_sockets(timeout);
            }
        }
    }
}

async fn handle_recv_result(
    state: &UdpManager,
    buf: &[u8],
    result: std::io::Result<(usize, SocketAddr)>,
) -> Result<()> {
    let (size, client_addr) = result?;
    trace!("[UDP] Server receive udp from {}", client_addr);
    state.handle_inbound_packet(buf, size, client_addr).await
}

async fn listen_udp_response(
    inbound: Arc<UdpSocket>,
    outbound: Arc<UdpSocket>,
    client_map: Arc<DashMap<String, (SocketAddr, Instant)>>,
    mut stop_signal: oneshot::Receiver<()>,
) -> Result<()> {
    debug!(
        "[UDP] start listening udp response on {}",
        outbound.local_addr()?.port()
    );

    let mut buf = vec![0u8; 0x10000];

    loop {
        tokio::select! {
            result = outbound.recv_from(&mut buf) => {
                let (size, remote_addr) = result?;
                trace!("[UDP] Receive packet from {}", remote_addr);

                let mut data = new_udp_header(remote_addr)?;
                data.extend_from_slice(&buf[..size]);

                let client_key = format!("{}:{}", remote_addr.ip(), outbound.local_addr()?.port());

                if let Some(mut entry) = client_map.get_mut(&client_key) {
                    let (client_addr, ref mut last_used) = entry.value_mut();
                    inbound.send_to(&data, *client_addr).await?;
                    *last_used = Instant::now();
                } else {
                    warn!("No client found for {}", client_key);
                }
            }
            _ = &mut stop_signal => {
                trace!(
                    "[UDP] Stop signal received, stopping udp response listener {}",
                    outbound.local_addr()?.port()
                );
                break;
            }
        }
    }
    trace!(
        "[UDP] listener for port {} stopped successfully",
        outbound.local_addr()?.port()
    );
    Ok(())
}

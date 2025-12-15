use anyhow::{Context, Result};
use dashmap::{DashMap, DashSet};
use std::{
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{net::UdpSocket, sync::oneshot, task::JoinHandle};

use crate::{new_udp_header, parse_udp_request};

use super::target_addr::TargetAddr;

// ============================================================================
// 常量定义
// ============================================================================

const UDP_BUFFER_SIZE: usize = 0x10000; // 64KB

/// 全局最大 outbound socket 数量，防止资源耗尽
const MAX_OUTBOUND_SOCKETS: usize = 1024;

// ============================================================================
// 类型定义
// ============================================================================

/// Outbound 连接的唯一标识: (target_ip, target_port, client_ip, client_port)
/// 注意: 当前模型为每个 (target, client) 组合创建独立 socket
type OutboundKey = (IpAddr, u16, IpAddr, u16);

/// Client 映射的唯一标识: (target_ip, local_port)
///
/// 约束说明: 忽略 target_port 是因为当前模型下每个 outbound socket 只对应一个 target，
/// 通过 local_port 即可唯一确定 outbound。若将来复用 socket（同 IP 多端口），需重新设计此 key。
type ClientKey = (IpAddr, u16);

// ============================================================================
// 工具函数
// ============================================================================

/// 将 IPv6 映射地址转换回 IPv4，纯 IPv6 保持不变
/// 用于统一 key 的生成，确保 IPv4 场景下 key 一致
#[inline]
fn normalize_ip(ip: IpAddr) -> IpAddr {
    match ip {
        v4 @ IpAddr::V4(_) => v4,
        IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(v6)), // 纯 IPv6 保持不变
    }
}

// ============================================================================
// UdpManager
// ============================================================================

/// Outbound 条目：(socket, local_port, last_used)
/// 将 local_port 存入结构避免依赖 socket.local_addr()
type OutboundEntry = (Arc<UdpSocket>, u16, Instant);

struct UdpManager {
    inbound: Arc<UdpSocket>,
    /// key -> (socket, local_port, last_used)
    outbound_map: Arc<DashMap<OutboundKey, OutboundEntry>>,
    client_map: Arc<DashMap<ClientKey, (SocketAddr, Instant)>>,
    active_outbound_listener_ports: Arc<DashSet<u16>>,
    task_handles: DashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>,
}

impl UdpManager {
    async fn new(udp_port: u16) -> Result<Self> {
        let udp_addr = format!("127.0.0.1:{}", udp_port);
        let udp_socket = UdpSocket::bind(&udp_addr)
            .await
            .with_context(|| format!("[UDP] 绑定端口 {} 失败", udp_addr))?;

        info!("[UDP] 监听 UDP 请求: {}", &udp_addr);

        Ok(Self {
            inbound: Arc::new(udp_socket),
            outbound_map: Arc::new(DashMap::new()),
            client_map: Arc::new(DashMap::new()),
            active_outbound_listener_ports: Arc::new(DashSet::new()),
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
            trace!("[UDP] 丢弃分片包");
            return Ok(());
        }

        trace!("[UDP] 转发数据包到 {}", target_addr);

        let target_addr = self.resolve_target_addr(target_addr).await?;
        let outbound_key = self.make_outbound_key(&target_addr, &client_addr);

        self.process_outbound(outbound_key, target_addr, client_addr, data)
            .await
    }

    /// 解析目标地址，仅支持 IPv4
    ///
    /// 当前 outbound socket 绑定在 0.0.0.0，仅支持 IPv4 目标。
    /// 若域名仅有 AAAA 记录或目标为纯 IPv6，返回错误。
    async fn resolve_target_addr(&self, target_addr: TargetAddr) -> Result<SocketAddr> {
        let resolved_addr = match target_addr {
            TargetAddr::Domain(_, _) => target_addr.resolve_dns().await?,
            ip @ TargetAddr::Ip(_) => ip,
        };

        let addr = resolved_addr
            .to_socket_addrs()?
            .next()
            .context("无法解析目标地址")?;

        // 仅支持 IPv4，拒绝纯 IPv6
        let normalized_ip = normalize_ip(addr.ip());
        match normalized_ip {
            IpAddr::V4(_) => Ok(SocketAddr::new(normalized_ip, addr.port())),
            IpAddr::V6(_) => {
                anyhow::bail!("[UDP] 不支持纯 IPv6 目标地址: {}", addr)
            }
        }
    }

    #[inline]
    fn make_outbound_key(&self, target_addr: &SocketAddr, client_addr: &SocketAddr) -> OutboundKey {
        (
            target_addr.ip(), // 已经过 normalize
            target_addr.port(),
            normalize_ip(client_addr.ip()),
            client_addr.port(),
        )
    }

    #[inline]
    fn make_client_key(&self, remote_ip: IpAddr, local_port: u16) -> ClientKey {
        (normalize_ip(remote_ip), local_port)
    }

    async fn process_outbound(
        &self,
        outbound_key: OutboundKey,
        target_addr: SocketAddr,
        client_addr: SocketAddr,
        data: &[u8],
    ) -> Result<()> {
        let now = Instant::now();

        let (outbound, local_port) = if let Some(mut entry) = self.outbound_map.get_mut(&outbound_key) {
            // 刷新 last_used (index 2)
            entry.value_mut().2 = now;
            (entry.value().0.clone(), entry.value().1)
        } else {
            // 检查资源上限
            if self.outbound_map.len() >= MAX_OUTBOUND_SOCKETS {
                anyhow::bail!(
                    "[UDP] 已达到最大 socket 数量限制 ({})，丢弃请求",
                    MAX_OUTBOUND_SOCKETS
                );
            }

            let new_outbound = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
            let port = new_outbound.local_addr()?.port();
            self.outbound_map
                .insert(outbound_key, (new_outbound.clone(), port, now));
            (new_outbound, port)
        };

        outbound.send_to(data, target_addr).await?;

        let client_key = self.make_client_key(target_addr.ip(), local_port);
        self.client_map.insert(client_key, (client_addr, now));
        trace!(
            "[UDP] 注册客户端映射 {:?} -> {}",
            client_key,
            client_addr
        );

        self.spawn_listener_if_needed(outbound, local_port, outbound_key).await
    }

    async fn spawn_listener_if_needed(
        &self,
        outbound: Arc<UdpSocket>,
        port: u16,
        outbound_key: OutboundKey,
    ) -> Result<()> {
        // 使用 insert 返回值避免 TOCTOU 竞态
        if self.active_outbound_listener_ports.insert(port) {
            trace!("[UDP] 启动端口 {} 的响应监听器", port);

            let inbound = self.inbound.clone();
            let client_map = self.client_map.clone();
            let outbound_map = self.outbound_map.clone();
            let active_ports = self.active_outbound_listener_ports.clone();

            let (tx, rx) = oneshot::channel();
            let handle = tokio::spawn(listen_udp_response(
                inbound,
                outbound,
                client_map,
                outbound_map,
                active_ports,
                port,
                outbound_key,
                rx,
            ));
            self.task_handles.insert(port, (handle, tx));
        }

        Ok(())
    }

    fn cleanup_expired_sockets(&self, timeout: Duration) {
        trace!(
            "[UDP] 清理过期连接, sockets: {}, clients: {}",
            self.outbound_map.len(),
            self.client_map.len(),
        );

        let now = Instant::now();

        // 阶段1: 收集需要清理的端口（使用存储的 port 而非 local_addr()）
        let mut expired_ports: Vec<u16> = Vec::new();

        self.outbound_map.retain(|_, (_socket, port, last_used)| {
            if now.duration_since(*last_used) > timeout {
                expired_ports.push(*port);
                false
            } else {
                true
            }
        });

        // 阶段2: 在 retain 外部清理相关状态，避免嵌套锁
        for port in expired_ports {
            self.active_outbound_listener_ports.remove(&port);

            if let Some((_, (handle, tx))) = self.task_handles.remove(&port) {
                trace!("[UDP] 停止端口 {} 的监听器", port);

                if tx.send(()).is_err() {
                    warn!("[UDP] 端口 {} 监听器无响应，强制终止", port);
                    handle.abort();
                }
            }
        }

        // 清理过期的客户端映射
        self.client_map
            .retain(|_, (_, last_used)| now.duration_since(*last_used) <= timeout);
    }
}

// ============================================================================
// 公开 API
// ============================================================================

pub async fn run_udp_server(
    udp_port: u16,
    cleanup_interval: u64,
    timeout: u64,
) -> Result<JoinHandle<()>> {
    let udp_manager = UdpManager::new(udp_port).await?;

    let handle = tokio::spawn(async move {
        let cleanup_interval = Duration::from_secs(cleanup_interval);
        let timeout = Duration::from_secs(timeout);

        let mut buf = vec![0u8; UDP_BUFFER_SIZE];

        let mut tick = tokio::time::interval(cleanup_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                result = udp_manager.inbound.recv_from(&mut buf) => {
                    if let Err(e) = handle_recv_result(&udp_manager, &buf, result).await {
                        warn!("[UDP] 处理数据包失败: {:?}", e);
                    }
                }
                _ = tick.tick() => {
                    udp_manager.cleanup_expired_sockets(timeout);
                }
            }
        }
    });

    Ok(handle)
}

// ============================================================================
// 内部函数
// ============================================================================

async fn handle_recv_result(
    state: &UdpManager,
    buf: &[u8],
    result: std::io::Result<(usize, SocketAddr)>,
) -> Result<()> {
    let (size, client_addr) = result?;
    trace!("[UDP] 收到来自 {} 的数据包", client_addr);
    state.handle_inbound_packet(buf, size, client_addr).await
}

async fn listen_udp_response(
    inbound: Arc<UdpSocket>,
    outbound: Arc<UdpSocket>,
    client_map: Arc<DashMap<ClientKey, (SocketAddr, Instant)>>,
    outbound_map: Arc<DashMap<OutboundKey, OutboundEntry>>,
    active_ports: Arc<DashSet<u16>>,
    port: u16,
    outbound_key: OutboundKey,
    mut stop_signal: oneshot::Receiver<()>,
) {
    debug!("[UDP] 端口 {} 响应监听器已启动", port);

    let mut buf = vec![0u8; UDP_BUFFER_SIZE];

    loop {
        tokio::select! {
            result = outbound.recv_from(&mut buf) => {
                let (size, remote_addr) = match result {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("[UDP] 端口 {} 接收失败: {:?}", port, e);
                        continue;
                    }
                };

                trace!("[UDP] 收到来自 {} 的响应", remote_addr);

                // 构造响应数据
                let data = match new_udp_header(remote_addr) {
                    Ok(mut header) => {
                        header.extend_from_slice(&buf[..size]);
                        header
                    }
                    Err(e) => {
                        warn!("[UDP] 构造响应头失败: {:?}", e);
                        continue;
                    }
                };

                // 使用规范化的 key 查找客户端
                let client_key = (normalize_ip(remote_addr.ip()), port);

                if let Some(mut entry) = client_map.get_mut(&client_key) {
                    let (client_addr, ref mut last_used) = entry.value_mut();
                    if let Err(e) = inbound.send_to(&data, *client_addr).await {
                        warn!("[UDP] 发送响应到 {} 失败: {:?}", client_addr, e);
                        continue;
                    }
                    let now = Instant::now();
                    *last_used = now;

                    // 使用 outbound_key 直接 get_mut，O(1) 更新，避免遍历
                    if let Some(mut outbound_entry) = outbound_map.get_mut(&outbound_key) {
                        outbound_entry.value_mut().2 = now;
                    }
                } else {
                    trace!("[UDP] 未找到客户端映射: {:?}", client_key);
                }
            }
            _ = &mut stop_signal => {
                trace!("[UDP] 端口 {} 收到停止信号", port);
                break;
            }
        }
    }

    // 清理自身状态
    active_ports.remove(&port);
    debug!("[UDP] 端口 {} 响应监听器已停止", port);
}

// ============================================================================
// 测试模块
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    // ------------------------------------------------------------------------
    // normalize_ip 测试
    // ------------------------------------------------------------------------

    #[test]
    fn test_normalize_ip_v4_unchanged() {
        let ipv4 = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));
        let normalized = normalize_ip(ipv4);

        assert_eq!(normalized, ipv4, "IPv4 地址应保持不变");
    }

    #[test]
    fn test_normalize_ip_v6_mapped_to_v4() {
        // IPv6 映射地址应转换回 IPv4
        let v4 = Ipv4Addr::new(192, 168, 1, 1);
        let v6_mapped = IpAddr::V6(v4.to_ipv6_mapped());
        let normalized = normalize_ip(v6_mapped);

        assert_eq!(normalized, IpAddr::V4(v4), "IPv6 映射地址应转换为 IPv4");
    }

    #[test]
    fn test_normalize_ip_pure_v6_unchanged() {
        // 纯 IPv6 地址保持不变
        let ipv6 = IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1));
        let normalized = normalize_ip(ipv6);

        assert_eq!(normalized, ipv6, "纯 IPv6 地址应保持不变");
    }

    #[test]
    fn test_normalize_ip_loopback() {
        let ipv4_loopback = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let normalized = normalize_ip(ipv4_loopback);

        assert_eq!(normalized, ipv4_loopback, "IPv4 回环地址应保持不变");

        // IPv6 映射的回环地址应转换回 IPv4
        let v6_loopback_mapped = IpAddr::V6(Ipv4Addr::LOCALHOST.to_ipv6_mapped());
        let normalized = normalize_ip(v6_loopback_mapped);

        assert_eq!(normalized, ipv4_loopback, "IPv6 映射回环地址应转换为 IPv4");
    }

    #[test]
    fn test_normalize_ip_idempotent() {
        // 多次调用应保持结果一致
        let v4 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let normalized_once = normalize_ip(v4);
        let normalized_twice = normalize_ip(normalized_once);

        assert_eq!(normalized_once, normalized_twice, "normalize_ip 应是幂等操作");
    }

    // ------------------------------------------------------------------------
    // Key 一致性测试
    // ------------------------------------------------------------------------

    #[test]
    fn test_client_key_consistency() {
        // 验证 IPv4 和其对应的 IPv6 映射地址生成相同的 ClientKey（都转为 IPv4）
        let ipv4 = IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8));
        let ipv6_mapped = IpAddr::V6(Ipv4Addr::new(8, 8, 8, 8).to_ipv6_mapped());
        let port = 12345u16;

        let key1: ClientKey = (normalize_ip(ipv4), port);
        let key2: ClientKey = (normalize_ip(ipv6_mapped), port);

        assert_eq!(key1, key2, "IPv4 和 IPv6 映射地址应生成相同的 ClientKey");
        assert_eq!(key1.0, ipv4, "规范化后应为 IPv4");
    }

    #[test]
    fn test_outbound_key_consistency() {
        let target_v4 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)), 80);
        let target_v6 = SocketAddr::new(
            IpAddr::V6(Ipv4Addr::new(1, 2, 3, 4).to_ipv6_mapped()),
            80,
        );
        let client = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)), 54321);

        let key1: OutboundKey = (
            normalize_ip(target_v4.ip()),
            target_v4.port(),
            normalize_ip(client.ip()),
            client.port(),
        );
        let key2: OutboundKey = (
            normalize_ip(target_v6.ip()),
            target_v6.port(),
            normalize_ip(client.ip()),
            client.port(),
        );

        assert_eq!(key1, key2, "IPv4 和 IPv6 映射地址应生成相同的 OutboundKey");
        assert_eq!(key1.0, target_v4.ip(), "规范化后目标地址应为 IPv4");
    }

    // ------------------------------------------------------------------------
    // UdpManager 异步测试
    // ------------------------------------------------------------------------

    #[tokio::test]
    async fn test_udp_manager_creation() {
        // 使用随机端口避免冲突
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = socket.local_addr().unwrap().port();
        drop(socket);

        let manager = UdpManager::new(port).await;
        assert!(manager.is_ok(), "UdpManager 应成功创建");

        let manager = manager.unwrap();
        assert_eq!(manager.outbound_map.len(), 0);
        assert_eq!(manager.client_map.len(), 0);
        assert_eq!(manager.active_outbound_listener_ports.len(), 0);
    }

    #[tokio::test]
    async fn test_udp_manager_cleanup_expired() {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = socket.local_addr().unwrap().port();
        drop(socket);

        let manager = UdpManager::new(port).await.unwrap();

        // 插入一个过期的 outbound
        let outbound = Arc::new(UdpSocket::bind("0.0.0.0:0").await.unwrap());
        let outbound_port = outbound.local_addr().unwrap().port();
        let expired_time = Instant::now() - Duration::from_secs(120);

        let key: OutboundKey = (
            normalize_ip(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))),
            53,
            normalize_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            12345,
        );
        manager.outbound_map.insert(key, (outbound, outbound_port, expired_time));

        // 插入对应的 client 映射
        let client_key: ClientKey = (
            normalize_ip(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))),
            outbound_port,
        );
        manager.client_map.insert(
            client_key,
            (
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12345),
                expired_time,
            ),
        );

        assert_eq!(manager.outbound_map.len(), 1);
        assert_eq!(manager.client_map.len(), 1);

        // 执行清理，超时设为 60 秒
        manager.cleanup_expired_sockets(Duration::from_secs(60));

        assert_eq!(manager.outbound_map.len(), 0, "过期的 outbound 应被清理");
        assert_eq!(manager.client_map.len(), 0, "过期的 client 映射应被清理");
    }

    #[tokio::test]
    async fn test_udp_manager_cleanup_preserves_active() {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = socket.local_addr().unwrap().port();
        drop(socket);

        let manager = UdpManager::new(port).await.unwrap();

        // 插入一个活跃的 outbound
        let outbound = Arc::new(UdpSocket::bind("0.0.0.0:0").await.unwrap());
        let outbound_port = outbound.local_addr().unwrap().port();
        let active_time = Instant::now();

        let key: OutboundKey = (
            normalize_ip(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))),
            53,
            normalize_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            12345,
        );
        manager.outbound_map.insert(key, (outbound, outbound_port, active_time));

        assert_eq!(manager.outbound_map.len(), 1);

        // 执行清理，超时设为 60 秒
        manager.cleanup_expired_sockets(Duration::from_secs(60));

        assert_eq!(manager.outbound_map.len(), 1, "活跃的 outbound 应被保留");
    }

    #[tokio::test]
    async fn test_spawn_listener_idempotent() {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = socket.local_addr().unwrap().port();
        drop(socket);

        let manager = UdpManager::new(port).await.unwrap();

        let outbound = Arc::new(UdpSocket::bind("0.0.0.0:0").await.unwrap());
        let outbound_port = outbound.local_addr().unwrap().port();

        let outbound_key: OutboundKey = (
            normalize_ip(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))),
            53,
            normalize_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            12345,
        );

        // 第一次调用应创建监听器
        manager
            .spawn_listener_if_needed(outbound.clone(), outbound_port, outbound_key)
            .await
            .unwrap();
        assert!(manager.active_outbound_listener_ports.contains(&outbound_port));
        assert!(manager.task_handles.contains_key(&outbound_port));

        let handle_count_before = manager.task_handles.len();

        // 第二次调用应为幂等操作
        manager
            .spawn_listener_if_needed(outbound.clone(), outbound_port, outbound_key)
            .await
            .unwrap();

        assert_eq!(
            manager.task_handles.len(),
            handle_count_before,
            "重复调用不应创建新的监听器"
        );
    }
}

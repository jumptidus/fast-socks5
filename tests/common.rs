//! 集成测试共享基础设施
//!
//! 提供 SocksServer 测试所需的通用工具：
//! - TestSocksServer: 封装服务启动/停止
//! - MockEchoServer: 模拟目标服务器
//! - 端口分配与超时保护

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use fast_socks5::client::{Config as ClientConfig, Socks5Datagram, Socks5Stream};
use fast_socks5::server::{
    AcceptAuthentication, Config, Socks5Server, DEFAULT_MAX_OUTBOUND_SOCKETS,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;

// ============================================================================
// 常量
// ============================================================================

/// 测试超时时间
pub const TEST_TIMEOUT: Duration = Duration::from_secs(10);

/// 端口范围起始（避免与常用端口冲突）
static PORT_COUNTER: AtomicU16 = AtomicU16::new(30000);

// ============================================================================
// 端口分配
// ============================================================================

/// 分配一个未使用的端口
pub fn alloc_port() -> u16 {
    PORT_COUNTER.fetch_add(1, Ordering::SeqCst)
}

/// 分配多个端口
pub fn alloc_ports<const N: usize>() -> [u16; N] {
    let mut ports = [0u16; N];
    for port in &mut ports {
        *port = alloc_port();
    }
    ports
}

// ============================================================================
// TestSocksServer - SOCKS5 服务器测试封装
// ============================================================================

type Socks5Config = Config<AcceptAuthentication>;

/// 测试用 SOCKS5 服务器
///
/// 模拟主项目中 SocksServer 的使用模式
pub struct TestSocksServer {
    pub port: u16,
    pub udp_port: u16,
    pub reply_ip: IpAddr,
    pub reply_port: u16,
    handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl TestSocksServer {
    /// 创建并启动测试服务器
    pub async fn start() -> Result<Self> {
        let [port, udp_port] = alloc_ports();
        Self::start_with_ports(port, udp_port).await
    }

    /// 使用指定端口启动
    pub async fn start_with_ports(port: u16, udp_port: u16) -> Result<Self> {
        let reply_ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        let reply_port = udp_port;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (ready_tx, ready_rx) = oneshot::channel();

        let handle = tokio::spawn(Self::run_server(
            port,
            udp_port,
            reply_ip,
            reply_port,
            ready_tx,
            shutdown_rx,
        ));

        // 等待服务器就绪
        timeout(Duration::from_secs(5), ready_rx)
            .await
            .context("等待服务器启动超时")?
            .context("服务器启动失败")?;

        Ok(Self {
            port,
            udp_port,
            reply_ip,
            reply_port,
            handle: Some(handle),
            shutdown_tx: Some(shutdown_tx),
        })
    }

    async fn run_server(
        port: u16,
        udp_port: u16,
        reply_ip: IpAddr,
        reply_port: u16,
        ready_tx: oneshot::Sender<()>,
        shutdown_rx: oneshot::Receiver<()>,
    ) {
        let mut config = Socks5Config::default();
        config.set_request_timeout(30);
        config.set_udp_support(true);

        let addr = format!("127.0.0.1:{}", port);

        let mut socks5_server = match <Socks5Server<AcceptAuthentication>>::bind(
            &addr,
            udp_port,
            5,  // cleanup_interval
            30, // timeout
            Arc::new(AtomicUsize::new(DEFAULT_MAX_OUTBOUND_SOCKETS)),
        )
        .await
        {
            Ok(server) => server,
            Err(e) => {
                eprintln!("服务器绑定失败: {:?}", e);
                return;
            }
        };

        socks5_server.update_config(config);

        // 通知服务器已就绪
        let _ = ready_tx.send(());

        // 使用 tokio_stream::StreamExt 的 while let 模式
        // 在后台监听 shutdown 信号
        let shutdown_handle = tokio::spawn(async move {
            let _ = shutdown_rx.await;
        });

        use tokio_stream::StreamExt;
        let mut incoming = socks5_server.incoming();

        while let Some(socket_res) = incoming.next().await {
            // 检查是否需要关闭
            if shutdown_handle.is_finished() {
                break;
            }

            match socket_res {
                Ok(mut socket) => {
                    socket.set_reply_ip(reply_ip);
                    socket.set_reply_port(reply_port);
                    tokio::spawn(async move {
                        let _ = socket.upgrade_to_socks5().await;
                    });
                }
                Err(e) => {
                    eprintln!("接受连接错误: {:?}", e);
                }
            }
        }
    }

    /// 获取服务器地址
    pub fn addr(&self) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), self.port)
    }

    /// 创建 TCP 客户端连接
    pub async fn connect_tcp(&self, target: &str, port: u16) -> Result<Socks5Stream<TcpStream>> {
        let stream = Socks5Stream::connect(
            self.addr(),
            target.to_string(),
            port,
            ClientConfig::default(),
        )
        .await?;
        Ok(stream)
    }

    /// 创建 UDP 客户端
    pub async fn connect_udp(&self) -> Result<Socks5Datagram<TcpStream>> {
        let backing = TcpStream::connect(self.addr()).await?;
        let datagram = Socks5Datagram::bind(backing, "127.0.0.1:0").await?;
        Ok(datagram)
    }
}

impl Drop for TestSocksServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

// ============================================================================
// MockTcpEchoServer - TCP 回显服务器
// ============================================================================

/// TCP 回显服务器，用于测试 TCP CONNECT
pub struct MockTcpEchoServer {
    pub port: u16,
    handle: Option<JoinHandle<()>>,
}

impl MockTcpEchoServer {
    pub async fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();

        let handle = tokio::spawn(async move {
            loop {
                if let Ok((mut stream, _)) = listener.accept().await {
                    tokio::spawn(async move {
                        let mut buf = [0u8; 1024];
                        while let Ok(n) = stream.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                            if stream.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                    });
                }
            }
        });

        Ok(Self {
            port,
            handle: Some(handle),
        })
    }
}

impl Drop for MockTcpEchoServer {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

// ============================================================================
// MockUdpEchoServer - UDP 回显服务器
// ============================================================================

/// UDP 回显服务器，用于测试 UDP ASSOCIATE
pub struct MockUdpEchoServer {
    pub port: u16,
    handle: Option<JoinHandle<()>>,
}

impl MockUdpEchoServer {
    pub async fn start() -> Result<Self> {
        let socket = UdpSocket::bind("127.0.0.1:0").await?;
        let port = socket.local_addr()?.port();

        let handle = tokio::spawn(async move {
            let mut buf = [0u8; 65535];
            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((n, addr)) => {
                        let _ = socket.send_to(&buf[..n], addr).await;
                    }
                    Err(_) => break,
                }
            }
        });

        Ok(Self {
            port,
            handle: Some(handle),
        })
    }

    pub fn addr(&self) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), self.port)
    }
}

impl Drop for MockUdpEchoServer {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

// ============================================================================
// 工具函数
// ============================================================================

/// 带超时执行测试
pub async fn with_timeout<F, T>(future: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    timeout(TEST_TIMEOUT, future).await.context("测试超时")?
}

//! SocksServer 集成测试
//!
//! 模拟主项目对 fast-socks5 的调用模式，验证：
//! - 服务启动与配置
//! - TCP CONNECT 命令
//! - UDP ASSOCIATE 命令
//! - reply_ip/reply_port 正确返回
//! - 并发客户端处理

mod common;

use std::time::Duration;

use anyhow::Result;
use fast_socks5::client::{Config as ClientConfig, Socks5Datagram, Socks5Stream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use common::{with_timeout, MockTcpEchoServer, MockUdpEchoServer, TestSocksServer};

// ============================================================================
// 服务启动测试
// ============================================================================

/// 验证服务器能正常启动并监听
#[tokio::test]
async fn test_server_starts_successfully() -> Result<()> {
    with_timeout(async {
        let server = TestSocksServer::start().await?;

        // 验证可以建立 TCP 连接
        let stream = TcpStream::connect(server.addr()).await?;
        assert!(stream.peer_addr().is_ok());

        Ok(())
    })
    .await
}

/// 验证服务器配置正确应用
#[tokio::test]
async fn test_server_config_applied() -> Result<()> {
    with_timeout(async {
        let server = TestSocksServer::start().await?;

        // reply_ip 和 reply_port 应该正确设置
        assert_eq!(
            server.reply_ip,
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1))
        );
        assert_eq!(server.reply_port, server.udp_port);

        Ok(())
    })
    .await
}

// ============================================================================
// TCP CONNECT 测试
// ============================================================================

/// TCP 连接到本地回显服务器，验证数据传输
#[tokio::test]
async fn test_tcp_connect_echo() -> Result<()> {
    with_timeout(async {
        let server = TestSocksServer::start().await?;
        let echo = MockTcpEchoServer::start().await?;

        // 通过 SOCKS5 连接到回显服务器
        let mut stream = server.connect_tcp("127.0.0.1", echo.port).await?;

        // 发送数据
        let test_data = b"Hello, SOCKS5!";
        stream.write_all(test_data).await?;

        // 读取回显
        let mut buf = vec![0u8; test_data.len()];
        stream.read_exact(&mut buf).await?;

        assert_eq!(&buf, test_data);

        Ok(())
    })
    .await
}

/// TCP 连接使用域名解析
#[tokio::test]
async fn test_tcp_connect_domain() -> Result<()> {
    with_timeout(async {
        let server = TestSocksServer::start().await?;
        let echo = MockTcpEchoServer::start().await?;

        // 使用 localhost 域名
        let mut stream = server.connect_tcp("localhost", echo.port).await?;

        let test_data = b"Domain test";
        stream.write_all(test_data).await?;

        let mut buf = vec![0u8; test_data.len()];
        stream.read_exact(&mut buf).await?;

        assert_eq!(&buf, test_data);

        Ok(())
    })
    .await
}

/// TCP 传输大数据量
#[tokio::test]
async fn test_tcp_connect_large_data() -> Result<()> {
    with_timeout(async {
        let server = TestSocksServer::start().await?;
        let echo = MockTcpEchoServer::start().await?;

        let mut stream = server.connect_tcp("127.0.0.1", echo.port).await?;

        // 发送 64KB 数据
        let test_data: Vec<u8> = (0..65536).map(|i| (i % 256) as u8).collect();
        stream.write_all(&test_data).await?;

        // 读取回显
        let mut buf = vec![0u8; test_data.len()];
        stream.read_exact(&mut buf).await?;

        assert_eq!(buf, test_data);

        Ok(())
    })
    .await
}

/// TCP 连接目标不可达
#[tokio::test]
async fn test_tcp_connect_unreachable() -> Result<()> {
    with_timeout(async {
        let server = TestSocksServer::start().await?;

        // 尝试连接不存在的端口
        let result = server.connect_tcp("127.0.0.1", 1).await;

        // 应该返回错误
        assert!(result.is_err());

        Ok(())
    })
    .await
}

// ============================================================================
// UDP ASSOCIATE 测试
// ============================================================================

/// UDP 关联基础功能
#[tokio::test]
async fn test_udp_associate_basic() -> Result<()> {
    with_timeout(async {
        let server = TestSocksServer::start().await?;
        let echo = MockUdpEchoServer::start().await?;

        let tunnel = server.connect_udp().await?;

        // 发送 UDP 数据
        let test_data = b"UDP test message";
        tunnel.send_to(test_data, echo.addr()).await?;

        // 接收响应
        let mut buf = [0u8; 1024];
        let (len, _addr) = timeout(Duration::from_secs(5), tunnel.recv_from(&mut buf)).await??;

        assert_eq!(&buf[..len], test_data);

        Ok(())
    })
    .await
}

/// UDP 多次发送接收
#[tokio::test]
async fn test_udp_associate_multiple_packets() -> Result<()> {
    with_timeout(async {
        let server = TestSocksServer::start().await?;
        let echo = MockUdpEchoServer::start().await?;

        let tunnel = server.connect_udp().await?;

        for i in 0..5 {
            let test_data = format!("Packet {}", i);
            tunnel.send_to(test_data.as_bytes(), echo.addr()).await?;

            let mut buf = [0u8; 1024];
            let (len, _) = timeout(Duration::from_secs(5), tunnel.recv_from(&mut buf)).await??;

            assert_eq!(&buf[..len], test_data.as_bytes());
        }

        Ok(())
    })
    .await
}

/// UDP 发送到多个目标
#[tokio::test]
async fn test_udp_associate_multiple_targets() -> Result<()> {
    with_timeout(async {
        let server = TestSocksServer::start().await?;
        let echo1 = MockUdpEchoServer::start().await?;
        let echo2 = MockUdpEchoServer::start().await?;

        let tunnel = server.connect_udp().await?;

        // 发送到第一个目标
        let data1 = b"To target 1";
        tunnel.send_to(data1, echo1.addr()).await?;

        // 发送到第二个目标
        let data2 = b"To target 2";
        tunnel.send_to(data2, echo2.addr()).await?;

        // 接收两个响应（顺序可能不确定）
        let mut received = Vec::new();
        for _ in 0..2 {
            let mut buf = [0u8; 1024];
            let (len, _) = timeout(Duration::from_secs(5), tunnel.recv_from(&mut buf)).await??;
            received.push(buf[..len].to_vec());
        }

        // 验证两个响应都收到
        assert!(received.iter().any(|r| r == data1));
        assert!(received.iter().any(|r| r == data2));

        Ok(())
    })
    .await
}

/// 验证 reply_ip 和 reply_port 正确返回给客户端
#[tokio::test]
async fn test_udp_associate_reply_addr() -> Result<()> {
    with_timeout(async {
        let server = TestSocksServer::start().await?;

        let tunnel = server.connect_udp().await?;

        // 获取代理地址
        let proxy_addr = tunnel.proxy_addr()?;

        // 验证返回的地址与配置一致
        match proxy_addr {
            fast_socks5::util::target_addr::TargetAddr::Ip(addr) => {
                assert_eq!(addr.ip(), server.reply_ip);
                assert_eq!(addr.port(), server.reply_port);
            }
            _ => panic!("期望 IP 地址类型"),
        }

        Ok(())
    })
    .await
}

// ============================================================================
// 并发测试
// ============================================================================

/// 多个 TCP 客户端并发连接
#[tokio::test]
async fn test_concurrent_tcp_clients() -> Result<()> {
    with_timeout(async {
        let server = TestSocksServer::start().await?;
        let echo = MockTcpEchoServer::start().await?;

        let mut handles = Vec::new();

        // 启动 10 个并发客户端
        for i in 0..10 {
            let server_addr = server.addr();
            let echo_port = echo.port;

            let handle = tokio::spawn(async move {
                let mut stream = Socks5Stream::connect(
                    server_addr,
                    "127.0.0.1".to_string(),
                    echo_port,
                    ClientConfig::default(),
                )
                .await?;

                let test_data = format!("Client {}", i);
                stream.write_all(test_data.as_bytes()).await?;

                let mut buf = vec![0u8; test_data.len()];
                stream.read_exact(&mut buf).await?;

                anyhow::ensure!(buf == test_data.as_bytes(), "数据不匹配");

                Ok::<(), anyhow::Error>(())
            });

            handles.push(handle);
        }

        // 等待所有客户端完成
        for handle in handles {
            handle.await??;
        }

        Ok(())
    })
    .await
}

/// 多个 UDP 客户端并发
#[tokio::test]
async fn test_concurrent_udp_clients() -> Result<()> {
    with_timeout(async {
        let server = TestSocksServer::start().await?;
        let echo = MockUdpEchoServer::start().await?;

        let mut handles = Vec::new();

        // 启动 5 个并发 UDP 客户端
        for i in 0..5 {
            let server_addr = server.addr();
            let echo_addr = echo.addr();

            let handle = tokio::spawn(async move {
                let backing = TcpStream::connect(server_addr).await?;
                let tunnel = Socks5Datagram::bind(backing, "127.0.0.1:0").await?;

                let test_data = format!("UDP Client {}", i);
                tunnel.send_to(test_data.as_bytes(), echo_addr).await?;

                let mut buf = [0u8; 1024];
                let (len, _) =
                    timeout(Duration::from_secs(5), tunnel.recv_from(&mut buf)).await??;

                anyhow::ensure!(&buf[..len] == test_data.as_bytes(), "数据不匹配");

                Ok::<(), anyhow::Error>(())
            });

            handles.push(handle);
        }

        // 等待所有客户端完成
        for handle in handles {
            handle.await??;
        }

        Ok(())
    })
    .await
}

/// TCP 和 UDP 混合并发
#[tokio::test]
async fn test_mixed_tcp_udp_concurrent() -> Result<()> {
    with_timeout(async {
        let server = TestSocksServer::start().await?;
        let tcp_echo = MockTcpEchoServer::start().await?;
        let udp_echo = MockUdpEchoServer::start().await?;

        let mut handles = Vec::new();

        // TCP 客户端
        for i in 0..5 {
            let server_addr = server.addr();
            let echo_port = tcp_echo.port;

            handles.push(tokio::spawn(async move {
                let mut stream = Socks5Stream::connect(
                    server_addr,
                    "127.0.0.1".to_string(),
                    echo_port,
                    ClientConfig::default(),
                )
                .await?;

                let test_data = format!("TCP {}", i);
                stream.write_all(test_data.as_bytes()).await?;

                let mut buf = vec![0u8; test_data.len()];
                stream.read_exact(&mut buf).await?;

                anyhow::ensure!(buf == test_data.as_bytes(), "TCP 数据不匹配");

                Ok::<(), anyhow::Error>(())
            }));
        }

        // UDP 客户端
        for i in 0..5 {
            let server_addr = server.addr();
            let echo_addr = udp_echo.addr();

            handles.push(tokio::spawn(async move {
                let backing = TcpStream::connect(server_addr).await?;
                let tunnel = Socks5Datagram::bind(backing, "127.0.0.1:0").await?;

                let test_data = format!("UDP {}", i);
                tunnel.send_to(test_data.as_bytes(), echo_addr).await?;

                let mut buf = [0u8; 1024];
                let (len, _) =
                    timeout(Duration::from_secs(5), tunnel.recv_from(&mut buf)).await??;

                anyhow::ensure!(&buf[..len] == test_data.as_bytes(), "UDP 数据不匹配");

                Ok::<(), anyhow::Error>(())
            }));
        }

        // 等待所有客户端完成
        for handle in handles {
            handle.await??;
        }

        Ok(())
    })
    .await
}

// ============================================================================
// 边界条件测试
// ============================================================================

/// 空数据发送
#[tokio::test]
async fn test_tcp_empty_data() -> Result<()> {
    with_timeout(async {
        let server = TestSocksServer::start().await?;
        let echo = MockTcpEchoServer::start().await?;

        let mut stream = server.connect_tcp("127.0.0.1", echo.port).await?;

        // 发送空数据（实际只是不发送）
        stream.write_all(b"").await?;

        // 发送实际数据验证连接正常
        stream.write_all(b"test").await?;

        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"test");

        Ok(())
    })
    .await
}

/// 连接后立即关闭
#[tokio::test]
async fn test_tcp_immediate_close() -> Result<()> {
    with_timeout(async {
        let server = TestSocksServer::start().await?;
        let echo = MockTcpEchoServer::start().await?;

        let stream = server.connect_tcp("127.0.0.1", echo.port).await?;

        // 立即 drop，测试服务器处理
        drop(stream);

        // 服务器应该仍然正常运行，可以接受新连接
        let mut stream2 = server.connect_tcp("127.0.0.1", echo.port).await?;

        stream2.write_all(b"still works").await?;

        let mut buf = [0u8; 11];
        stream2.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"still works");

        Ok(())
    })
    .await
}

use anyhow::Result;
use once_cell::sync::OnceCell;
use std::{net::IpAddr, time::Duration};
use trust_dns_resolver::{
    config::{LookupIpStrategy, NameServerConfig, Protocol, ResolverConfig, ResolverOpts},
    TokioAsyncResolver,
};

// 全局静态实例
static DNS_RESOLVER: OnceCell<TokioAsyncResolver> = OnceCell::new();

// 简单的辅助函数来获取或初始化解析器
async fn get_resolver() -> Result<&'static TokioAsyncResolver> {
    if DNS_RESOLVER.get().is_none() {
        let mut config = ResolverConfig::new();
        config.add_name_server(NameServerConfig::new(
            "114.114.114.114:53".parse()?,
            Protocol::Udp,
        ));
        config.add_name_server(NameServerConfig::new(
            "223.6.6.6:53".parse()?,
            Protocol::Udp,
        ));
        config.add_name_server(NameServerConfig::new(
            "119.29.29.29:53".parse()?,
            Protocol::Udp,
        ));
        config.add_name_server(NameServerConfig::new(
            "180.76.76.76:53".parse()?,
            Protocol::Udp,
        ));
        config.add_name_server(NameServerConfig::new("8.8.8.8:53".parse()?, Protocol::Udp));

        let mut opts = ResolverOpts::default();
        opts.timeout = std::time::Duration::from_secs(3); // 超时3秒
        opts.ip_strategy = LookupIpStrategy::Ipv4Only; // 只解析ipv4
        opts.positive_max_ttl = Some(Duration::from_secs(600)); // 成功解析缓存600秒
        opts.negative_max_ttl = Some(Duration::from_secs(300)); // 失败解析缓存300秒
        opts.attempts = 1; // 只尝试一次

        let resolver = TokioAsyncResolver::tokio(config, opts);
        DNS_RESOLVER.set(resolver).unwrap();
    }

    Ok(DNS_RESOLVER.get().unwrap())
}

// 单个公共方法用于DNS解析
pub async fn resolve(domain: &str) -> Result<Vec<IpAddr>> {
    let resolver = get_resolver().await?;
    let response = resolver.lookup_ip(domain).await?;
    Ok(response.iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_dns() {
        let ips = resolve("www.baidu.com").await.unwrap();
        println!("Resolved IPs: {:?}", ips);
    }
}

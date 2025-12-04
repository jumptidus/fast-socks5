# Repository Guidelines

## Rule
- 请一直使用简体中文进行交互

## Project Structure & Module Organization
- Core library entry in `src/lib.rs`; client and server flows live in `src/client.rs` and `src/server.rs`.
- SOCKS4 support is isolated under `src/socks4/`; shared helpers (DNS resolution, UDP handling, stream wrappers) sit in `src/util/`.
- Integration-style async tests live in `tests/sock5_client_test.rs`; runnable examples are in `examples/` (`client`, `server`, `simple_tcp_server`).
- Enable optional SOCKS4 behavior with the `socks4` feature flag when needed.

## Build, Test, and Development Commands
- `cargo build` — compile the library; add `--features socks4` to include SOCKS4 code paths.
- `cargo test` — run async tests (uses `tokio`/`tokio-test`); prefer `RUST_LOG=debug` when diagnosing failures.
- `cargo fmt --all` — format using the repo’s Rust 2021 rustfmt settings.
- `cargo clippy --all-targets --all-features -- -D warnings` — lint for stricter hygiene when developing locally.
- Example runs: `RUST_LOG=debug cargo run --example server -- --listen-addr 127.0.0.1:1337 password -u admin -p password` and `RUST_LOG=debug cargo run --example client -- --socks-server 127.0.0.1:1337 --username admin --password password -a perdu.com -p 80`.

## Coding Style & Naming Conventions
- Follow Rust defaults: `snake_case` for functions/vars, `CamelCase` for types, module-scoped `mod` files.
- Keep async functions focused and prefer small helpers in `util` for reusable IO/DNS logic.
- Use `log` crate levels consistently; avoid leaking credentials in debug logs.
- Favor explicit error propagation via `anyhow::Result` or typed errors in public APIs.

## Testing Guidelines
- Add integration tests under `tests/` with `tokio::test` for async scenarios; name files `*_test.rs` and functions `test_*`.
- Mock servers/clients with `TcpListener` and short timeouts (`tokio::time::timeout`) to keep runs fast and deterministic.
- When adding protocol cases, assert on exact byte exchanges so regressions are clear.

## Commit & Pull Request Guidelines
- Use concise, present-tense commit messages (the history favors short summaries, often in Chinese, e.g., “修复 udp 一直收包的情况下出现的无法清理情况”); group related edits per commit.
- In PRs, describe the behavior change, note flags/features touched (e.g., `socks4`), list commands/tests executed, and include example invocations if behavior is user-facing.
- Update README or examples when protocol behavior or CLI flags change to keep consumers aligned.

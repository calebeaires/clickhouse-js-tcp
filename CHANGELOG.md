# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-03-05

### Added

- ClickHouse TCP native protocol client for Node.js
- `createClient()` factory with `query()`, `insert()`, `command()`, `ping()`, `close()` API
- Column types: UInt8/16/32/64, Int8/16/32/64/128/256, Float32/64, String, FixedString, Bool, Date, Date32, DateTime, DateTime64, Enum8/16, Nullable, Array, Map, Tuple, LowCardinality, UUID, IPv4, IPv6, Decimal, Nothing
- LZ4 block compression support
- Connection pooling with health checks and idle connection management
- TLS/TCPS support
- Streaming inserts (batched, 10k rows/batch)
- Query cancellation via `AbortSignal`
- Automatic reconnection and retry on connection-level errors
- Query timeout support via `request_timeout`
- Keep-alive and idle connection sweeping
- TCP URL scheme (`tcp://` / `tcps://`) support

[0.1.0]: https://github.com/calebeaires/clickhouse-js-tcp/releases/tag/v0.1.0

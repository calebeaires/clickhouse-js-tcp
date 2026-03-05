# clickhouse-js-tcp

[![npm version](https://img.shields.io/npm/v/clickhouse-js-tcp)](https://www.npmjs.com/package/clickhouse-js-tcp)
[![CI](https://github.com/calebeaires/clickhouse-js-tcp/actions/workflows/ci.yml/badge.svg)](https://github.com/calebeaires/clickhouse-js-tcp/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Node.js](https://img.shields.io/badge/node-%3E%3D20-brightgreen)](https://nodejs.org)

ClickHouse TCP native protocol driver for Node.js/TypeScript.

Uses the ClickHouse **native binary protocol** (port 9000) for high-performance communication, unlike HTTP-based clients that use port 8123. The native protocol is more efficient for large data transfers with binary serialization and LZ4 block compression.

## Installation

```bash
npm install clickhouse-js-tcp
```

## Quick Start

```typescript
import { createClient } from 'clickhouse-js-tcp'

const client = createClient({
  host: 'localhost:9000',
  username: 'default',
  password: '',
  database: 'default',
})

// DDL
await client.command({
  query: `
    CREATE TABLE IF NOT EXISTS my_table
    (id UInt32, name String, created Date)
    ENGINE = MergeTree()
    ORDER BY id
  `,
})

// Insert
await client.insert({
  table: 'my_table',
  values: [
    { id: 1, name: 'Alice', created: '2026-03-05' },
    { id: 2, name: 'Bob', created: '2026-03-05' },
  ],
})

// Query
const result = await client.query({
  query: 'SELECT * FROM my_table ORDER BY id',
})
console.log(result.rows)
// [{ id: 1, name: 'Alice', created: '2026-03-05' }, ...]

// Ping
const alive = await client.ping()
console.log(alive) // true

await client.close()
```

> **Security note:** In production, always use environment variables for credentials and enable TLS (`tcps://`) to encrypt the connection. Never hardcode passwords in source code.

## Configuration

```typescript
const client = createClient({
  // Connection
  host: 'localhost:9000', // or use url: 'tcp://user:pass@host:9000/db'
  username: process.env.CLICKHOUSE_USER ?? 'default',
  password: process.env.CLICKHOUSE_PASSWORD ?? '',
  database: 'default',

  // Behavior
  request_timeout: 30_000, // Query timeout in ms (default: 30s)
  compression: true, // Enable LZ4 compression (default: false)

  // TLS (optional — enables TCPS)
  tls: {
    ca_cert: Buffer.from('...'),
    cert: Buffer.from('...'), // For mutual TLS
    key: Buffer.from('...'), // For mutual TLS
  },
})
```

| Option            | Type               | Default            | Description                                   |
| ----------------- | ------------------ | ------------------ | --------------------------------------------- |
| `host`            | `string`           | `'localhost:9000'` | ClickHouse native protocol address            |
| `url`             | `string`           | —                  | Connection URL (`tcp://` or `tcps://` scheme) |
| `username`        | `string`           | `'default'`        | ClickHouse username                           |
| `password`        | `string`           | `''`               | ClickHouse password                           |
| `database`        | `string`           | `'default'`        | Default database                              |
| `request_timeout` | `number`           | `30000`            | Query timeout in milliseconds                 |
| `compression`     | `boolean`          | `false`            | Enable LZ4 block compression                  |
| `tls.ca_cert`     | `Buffer \| string` | —                  | CA certificate for TLS                        |
| `tls.cert`        | `Buffer \| string` | —                  | Client certificate (mutual TLS)               |
| `tls.key`         | `Buffer \| string` | —                  | Client private key (mutual TLS)               |

## API Reference

### `createClient(config?)`

Creates a new ClickHouse client instance.

### `client.query({ query, query_params?, abort_signal? })`

Execute a SELECT query. Returns a result set with `.rows` containing the result as an array of objects.

```typescript
const result = await client.query({
  query: 'SELECT * FROM my_table WHERE id > {id:UInt32}',
  query_params: { id: 10 },
})
console.log(result.rows)
```

### `client.insert({ table, values, abort_signal? })`

Insert rows into a table. Values are streamed in batches of 10,000 rows.

```typescript
await client.insert({
  table: 'my_table',
  values: [{ id: 1, name: 'Alice' }],
})
```

### `client.command({ query, abort_signal? })`

Execute DDL/DML statements (CREATE, ALTER, DROP, etc.).

```typescript
await client.command({
  query: 'ALTER TABLE my_table DELETE WHERE id = 1',
})
```

### `client.ping()`

Check server connectivity. Returns `true` if the server is reachable.

### `client.close()`

Close all connections in the pool.

### Query cancellation

Pass an `AbortSignal` to cancel a running query:

```typescript
const controller = new AbortController()
setTimeout(() => controller.abort(), 5000) // cancel after 5s

const result = await client.query({
  query: 'SELECT * FROM large_table',
  abort_signal: controller.signal,
})
```

## TCP URL Scheme

You can use `tcp://` or `tcps://` URLs instead of individual config fields:

```typescript
const client = createClient({
  url: 'tcp://user:password@localhost:9000/my_database',
})

// With TLS
const secureClient = createClient({
  url: 'tcps://user:password@localhost:9440/my_database',
  tls: { ca_cert: fs.readFileSync('/path/to/ca.pem') },
})
```

## Supported Column Types

| Category            | Types                                                                    |
| ------------------- | ------------------------------------------------------------------------ |
| **Integers**        | UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Int128, Int256 |
| **Floats**          | Float32, Float64                                                         |
| **Decimals**        | Decimal32, Decimal64, Decimal128                                         |
| **Strings**         | String, FixedString(N)                                                   |
| **Date/Time**       | Date, Date32, DateTime, DateTime64                                       |
| **Boolean**         | Bool                                                                     |
| **UUID**            | UUID                                                                     |
| **Network**         | IPv4, IPv6                                                               |
| **Enums**           | Enum8, Enum16                                                            |
| **Nullable**        | Nullable(T)                                                              |
| **Arrays**          | Array(T)                                                                 |
| **Maps**            | Map(K, V)                                                                |
| **Tuples**          | Tuple(T1, T2, ...)                                                       |
| **Low Cardinality** | LowCardinality(T)                                                        |
| **Special**         | Nothing                                                                  |

## Benchmark: TCP vs HTTP

Performance comparison against the official HTTP-based `@clickhouse/client` driver. Run with `npm run benchmark`.

| Scenario         | Rows    | TCP (ms) | HTTP (ms) | Speedup   |
| ---------------- | ------- | -------- | --------- | --------- |
| Ping (avg x50)   | -       | 173.86   | 177.79    | **1.02x** |
| SELECT 1k rows   | 1,000   | 218.15   | 201.93    | 0.93x     |
| SELECT 100k rows | 100,000 | 1,098.24 | 2,823.47  | **2.57x** |
| INSERT 10k rows  | 10,000  | 753.10   | 1,229.40  | **1.63x** |
| INSERT 100k rows | 100,000 | 843.27   | 1,260.31  | **1.49x** |
| Stream 500k rows | 500,000 | 5,998.66 | 14,747.75 | **2.46x** |

The TCP native protocol excels across all scenarios — large streaming reads (2.5x faster), bulk inserts (1.5x faster), and matches HTTP for small operations. Binary serialization avoids JSON parsing overhead, and connection pooling with smart health-checks minimizes latency.

## Known Limitations

- **LowCardinality(Nullable(T))** — INSERT/SELECT not supported (null encoded in dictionary format)
- **Results are buffered** — query results are fully materialized in memory before returning

## Features

- Native TCP binary protocol (port 9000)
- LZ4 block compression
- Connection pooling with health checks
- TLS/TCPS support
- Streaming inserts (batched)
- Query cancellation via AbortSignal
- Automatic reconnection and retry
- Query timeout support
- Keep-alive and idle connection management

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, testing, and PR guidelines.

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for release history.

## License

[Apache-2.0](LICENSE)

# clickhouse-js-tcp

ClickHouse TCP native protocol driver for Node.js/TypeScript.

Uses the ClickHouse native binary protocol (port 9000) for high-performance communication, unlike HTTP-based clients.

## Install

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

// Query
const result = await client.query({ query: 'SELECT 1 AS val' })
console.log(result.rows) // [{ val: 1 }]

// Insert
await client.insert({
  table: 'my_table',
  values: [{ id: 1, name: 'Alice' }],
})

// Command (DDL)
await client.command({
  query: 'CREATE TABLE IF NOT EXISTS my_table (id UInt32, name String) ENGINE = MergeTree() ORDER BY id',
})

await client.close()
```

## Features

- Native TCP binary protocol (port 9000)
- LZ4 block compression
- Connection pooling with health checks
- TLS support
- Streaming inserts (batched)
- Query cancellation via AbortSignal
- Automatic reconnection and retry
- Query timeout support
- Keep-alive and idle connection management

### Supported Column Types

UInt8/16/32/64, Int8/16/32/64/128/256, Float32/64, String, FixedString, Bool, Date, Date32, DateTime, DateTime64, Enum8/16, Nullable, Array, Map, Tuple, LowCardinality, UUID, IPv4, IPv6, Decimal, Nothing

## Configuration

```typescript
const client = createClient({
  host: 'localhost:9000',       // ClickHouse native protocol address
  username: 'default',
  password: '',
  database: 'default',
  request_timeout: 30_000,      // Query timeout in ms
  compression: true,            // Enable LZ4 compression
  tls: {                        // Optional TLS config
    ca_cert: Buffer.from('...'),
  },
})
```

## API

- `client.query({ query })` — Execute a SELECT query, returns rows
- `client.insert({ table, values })` — Insert rows into a table
- `client.command({ query })` — Execute DDL/DML statements
- `client.ping()` — Check server connectivity
- `client.close()` — Close all connections

## License

Apache-2.0

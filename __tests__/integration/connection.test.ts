import { describe, it, expect } from 'vitest'
import { TcpConnection } from '../../src/connection/tcp_connection'
import type { ConnectionParams } from '@clickhouse/client-common'

const host = process.env.GAIO_CLICKHOUSE_HOST || 'localhost'
const password = process.env.GAIO_CLICKHOUSE_PASSWORD || ''

function makeParams(overrides?: Partial<ConnectionParams>): ConnectionParams {
  return {
    url: new URL(`http://${host}:9000`),
    request_timeout: 10000,
    max_open_connections: 10,
    compression: { decompress_response: false, compress_request: false },
    database: 'default',
    clickhouse_settings: {},
    log_writer: {
      trace: () => {},
      debug: () => {},
      info: () => {},
      warn: () => {},
      error: () => {},
    },
    log_level: 'OFF' as any,
    keep_alive: { enabled: true },
    auth: { username: 'default', password, type: 'Credentials' },
    ...overrides,
  }
}

async function withConnection(
  fn: (conn: TcpConnection) => Promise<void>,
): Promise<void> {
  const conn = new TcpConnection(makeParams())
  try {
    await conn.connect()
    await fn(conn)
  } finally {
    await conn.close()
  }
}

describe('TCP Connection Integration', () => {
  it('should connect and handshake', async () => {
    await withConnection(async (conn) => {
      expect(conn.serverInfo).toBeDefined()
      expect(conn.serverInfo!.serverName).toContain('ClickHouse')
    })
  })

  it('should ping', async () => {
    await withConnection(async (conn) => {
      const result = await conn.ping()
      expect(result.success).toBe(true)
    })
  })

  it('should execute SELECT 1', async () => {
    await withConnection(async (conn) => {
      const result = await conn.query({
        query: 'SELECT 1 as n',
        query_id: 'test-select-1',
        clickhouse_settings: {},
      } as any)

      expect(result.query_id).toBe('test-select-1')
      const rows: any[] = []
      for await (const row of result.stream) {
        rows.push(row)
      }
      expect(rows.length).toBe(1)
      expect(rows[0].n).toBe(1)
    })
  })

  it('should execute SELECT with multiple rows', async () => {
    await withConnection(async (conn) => {
      const result = await conn.query({
        query: 'SELECT number FROM system.numbers LIMIT 10',
        query_id: 'test-numbers',
        clickhouse_settings: {},
      } as any)

      const rows: any[] = []
      for await (const row of result.stream) {
        rows.push(row)
      }
      expect(rows.length).toBe(10)
      expect(rows[0].number).toBe(0n)
      expect(rows[9].number).toBe(9n)
    })
  })

  it('should execute SELECT with various types', async () => {
    await withConnection(async (conn) => {
      const result = await conn.query({
        query: `SELECT
          toUInt8(42) as u8,
          toInt32(-100) as i32,
          'hello' as str,
          toFloat64(3.14) as f64,
          true as b`,
        query_id: 'test-types',
        clickhouse_settings: {},
      } as any)

      const rows: any[] = []
      for await (const row of result.stream) {
        rows.push(row)
      }
      expect(rows.length).toBe(1)
      expect(rows[0].u8).toBe(42)
      expect(rows[0].i32).toBe(-100)
      expect(rows[0].str).toBe('hello')
      expect(rows[0].f64).toBeCloseTo(3.14)
      expect(rows[0].b).toBe(true)
    })
  })

  it('should execute command (CREATE/DROP TABLE)', async () => {
    await withConnection(async (conn) => {
      const tableName = `test_tcp_${Date.now()}`

      await conn.command({
        query: `CREATE TABLE IF NOT EXISTS ${tableName} (id UInt32, name String) ENGINE = Memory`,
        query_id: `test-create-${tableName}`,
        clickhouse_settings: {},
      } as any)

      await conn.command({
        query: `DROP TABLE IF EXISTS ${tableName}`,
        query_id: `test-drop-${tableName}`,
        clickhouse_settings: {},
      } as any)
    })
  })

  it('should handle query errors', async () => {
    await withConnection(async (conn) => {
      await expect(
        conn.query({
          query: 'SELECT * FROM nonexistent_table_xyz_12345',
          query_id: 'test-error',
          clickhouse_settings: {},
        } as any),
      ).rejects.toThrow()
    })
  })

  it('should insert and select data', async () => {
    await withConnection(async (conn) => {
      const tableName = `test_insert_${Date.now()}`

      await conn.command({
        query: `CREATE TABLE ${tableName} (id UInt32, name String) ENGINE = Memory`,
        query_id: `create-${tableName}`,
        clickhouse_settings: {},
      } as any)

      try {
        await conn.insert({
          query: `INSERT INTO ${tableName} FORMAT Native`,
          values: JSON.stringify([
            { id: 1, name: 'Alice' },
            { id: 2, name: 'Bob' },
          ]),
          query_id: `insert-${tableName}`,
          clickhouse_settings: {},
        } as any)

        const result = await conn.query({
          query: `SELECT id, name FROM ${tableName} ORDER BY id`,
          query_id: `select-${tableName}`,
          clickhouse_settings: {},
        } as any)

        const rows: any[] = []
        for await (const row of result.stream) {
          rows.push(row)
        }
        expect(rows.length).toBe(2)
        expect(rows[0].id).toBe(1)
        expect(rows[0].name).toBe('Alice')
        expect(rows[1].id).toBe(2)
        expect(rows[1].name).toBe('Bob')
      } finally {
        await conn.command({
          query: `DROP TABLE IF EXISTS ${tableName}`,
          query_id: `drop-${tableName}`,
          clickhouse_settings: {},
        } as any)
      }
    })
  })
})

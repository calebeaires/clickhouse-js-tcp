import { describe, it, expect, afterAll } from 'vitest'
import { createClient } from '../../src/index'

const host = process.env.GAIO_CLICKHOUSE_HOST || 'localhost'
const password = process.env.GAIO_CLICKHOUSE_PASSWORD || ''

const client = createClient({
  url: `http://${host}:9000`,
  username: 'default',
  password,
})

afterAll(async () => {
  await client.close()
})

describe('createClient() production smoke tests', () => {
  it('should ping', async () => {
    const result = await client.ping()
    expect(result.success).toBe(true)
  })

  it('should query with JSONEachRow', async () => {
    const rs = await client.query({
      query: "SELECT 1 as n, 'hello' as greeting",
      format: 'JSONEachRow',
    })
    const rows = await rs.json<{ n: number; greeting: string }>()
    expect(rows).toEqual([{ n: 1, greeting: 'hello' }])
  })

  it('should query multiple rows', async () => {
    const rs = await client.query({
      query: 'SELECT number FROM system.numbers LIMIT 100',
      format: 'JSONEachRow',
    })
    const rows = await rs.json<{ number: bigint }>()
    expect(rows.length).toBe(100)
    expect(rows[0].number).toBe(0n)
    expect(rows[99].number).toBe(99n)
  })

  it('should execute DDL commands', async () => {
    const table = `smoke_test_${Date.now()}`
    await client.command({
      query: `CREATE TABLE IF NOT EXISTS ${table} (id UInt32, name String, score Float64) ENGINE = Memory`,
    })
    await client.command({
      query: `DROP TABLE IF EXISTS ${table}`,
    })
  })

  it('should insert and query back data', async () => {
    const table = `smoke_insert_${Date.now()}`

    await client.command({
      query: `CREATE TABLE ${table} (id UInt32, name String) ENGINE = Memory`,
    })

    try {
      await client.insert({
        table,
        values: [
          { id: 1, name: 'Alice' },
          { id: 2, name: 'Bob' },
          { id: 3, name: 'Charlie' },
        ],
        format: 'JSONEachRow',
      })

      const rs = await client.query({
        query: `SELECT id, name FROM ${table} ORDER BY id`,
        format: 'JSONEachRow',
      })
      const rows = await rs.json<{ id: number; name: string }>()
      expect(rows).toEqual([
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
        { id: 3, name: 'Charlie' },
      ])
    } finally {
      await client.command({ query: `DROP TABLE IF EXISTS ${table}` })
    }
  })

  it('should handle Nullable types', async () => {
    const rs = await client.query({
      query: "SELECT toNullable(42) as val, CAST(NULL AS Nullable(String)) as nul",
      format: 'JSONEachRow',
    })
    const rows = await rs.json()
    expect(rows.length).toBe(1)
    expect(rows[0].val).toBe(42)
    expect(rows[0].nul).toBeNull()
  })

  it('should handle DateTime types', async () => {
    const rs = await client.query({
      query: "SELECT toDate('2024-01-15') as d, toDateTime('2024-01-15 10:30:00') as dt",
      format: 'JSONEachRow',
    })
    const rows = await rs.json()
    expect(rows.length).toBe(1)
    // Date returns days since epoch, DateTime returns epoch seconds
    expect(rows[0].d).toBeDefined()
    expect(rows[0].dt).toBeDefined()
  })

  it('should handle Array types', async () => {
    const rs = await client.query({
      query: "SELECT [1, 2, 3] as arr, ['a', 'b'] as strs",
      format: 'JSONEachRow',
    })
    const rows = await rs.json()
    expect(rows.length).toBe(1)
    expect(rows[0].arr).toEqual([1, 2, 3])
    expect(rows[0].strs).toEqual(['a', 'b'])
  })

  it('should query with clickhouse_settings', async () => {
    const rs = await client.query({
      query: 'SELECT 1 as n',
      format: 'JSONEachRow',
      clickhouse_settings: {
        max_result_rows: '100',
      },
    })
    const rows = await rs.json()
    expect(rows.length).toBe(1)
  })

  it('should handle errors gracefully', async () => {
    await expect(
      client.query({ query: 'INVALID SQL SYNTAX 12345' }),
    ).rejects.toThrow()
  })
})

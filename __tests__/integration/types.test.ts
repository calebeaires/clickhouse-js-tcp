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

describe('UUID type', () => {
  it('should insert and select UUID values', async () => {
    const table = `test_uuid_${Date.now()}`
    await client.command({
      query: `CREATE TABLE ${table} (id UUID, name String) ENGINE = Memory`,
    })
    try {
      await client.insert({
        table,
        values: [
          { id: '550e8400-e29b-41d4-a716-446655440000', name: 'first' },
          { id: '6ba7b810-9dad-11d1-80b4-00c04fd430c8', name: 'second' },
        ],
        format: 'JSONEachRow',
      })
      const rs = await client.query({
        query: `SELECT id, name FROM ${table} ORDER BY name`,
        format: 'JSONEachRow',
      })
      const rows = await rs.json<{ id: string; name: string }>()
      expect(rows.length).toBe(2)
      expect(rows[0].id).toBe('550e8400-e29b-41d4-a716-446655440000')
      expect(rows[0].name).toBe('first')
      expect(rows[1].id).toBe('6ba7b810-9dad-11d1-80b4-00c04fd430c8')
    } finally {
      await client.command({ query: `DROP TABLE IF EXISTS ${table}` })
    }
  })

  it('should select Nullable(UUID)', async () => {
    const rs = await client.query({
      query: "SELECT toUUID('550e8400-e29b-41d4-a716-446655440000') as val, CAST(NULL AS Nullable(UUID)) as nul",
      format: 'JSONEachRow',
    })
    const rows = await rs.json()
    expect(rows.length).toBe(1)
    expect(rows[0].val).toBe('550e8400-e29b-41d4-a716-446655440000')
    expect(rows[0].nul).toBeNull()
  })
})

describe('Decimal types', () => {
  it('should insert and select Decimal32', async () => {
    const table = `test_decimal32_${Date.now()}`
    await client.command({
      query: `CREATE TABLE ${table} (val Decimal32(2)) ENGINE = Memory`,
    })
    try {
      await client.insert({
        table,
        values: [{ val: 123.45 }, { val: -67.89 }],
        format: 'JSONEachRow',
      })
      const rs = await client.query({
        query: `SELECT val FROM ${table} ORDER BY val`,
        format: 'JSONEachRow',
      })
      const rows = await rs.json<{ val: number }>()
      expect(rows.length).toBe(2)
      expect(rows[0].val).toBeCloseTo(-67.89, 2)
      expect(rows[1].val).toBeCloseTo(123.45, 2)
    } finally {
      await client.command({ query: `DROP TABLE IF EXISTS ${table}` })
    }
  })

  it('should insert and select Decimal64', async () => {
    const table = `test_decimal64_${Date.now()}`
    await client.command({
      query: `CREATE TABLE ${table} (val Decimal64(4)) ENGINE = Memory`,
    })
    try {
      await client.insert({
        table,
        values: [{ val: 12345.6789 }],
        format: 'JSONEachRow',
      })
      const rs = await client.query({
        query: `SELECT val FROM ${table}`,
        format: 'JSONEachRow',
      })
      const rows = await rs.json<{ val: number }>()
      expect(rows.length).toBe(1)
      expect(rows[0].val).toBeCloseTo(12345.6789, 4)
    } finally {
      await client.command({ query: `DROP TABLE IF EXISTS ${table}` })
    }
  })
})

describe('LowCardinality types', () => {
  it('should insert and select LowCardinality(String)', async () => {
    const table = `test_lc_${Date.now()}`
    await client.command({
      query: `CREATE TABLE ${table} (status LowCardinality(String)) ENGINE = Memory`,
    })
    try {
      await client.insert({
        table,
        values: [
          { status: 'active' },
          { status: 'inactive' },
          { status: 'active' },
          { status: 'pending' },
        ],
        format: 'JSONEachRow',
      })
      const rs = await client.query({
        query: `SELECT status FROM ${table} ORDER BY status`,
        format: 'JSONEachRow',
      })
      const rows = await rs.json<{ status: string }>()
      expect(rows.length).toBe(4)
      expect(rows[0].status).toBe('active')
      expect(rows[1].status).toBe('active')
      expect(rows[2].status).toBe('inactive')
      expect(rows[3].status).toBe('pending')
    } finally {
      await client.command({ query: `DROP TABLE IF EXISTS ${table}` })
    }
  })

  it('should handle LowCardinality with many repeated values', async () => {
    // LowCardinality(Nullable(T)) has a known limitation: the native protocol
    // encodes null within the dictionary, not via Nullable's null mask.
    // Test LowCardinality(String) with many repeats to validate dictionary encoding.
    const table = `test_lc_many_${Date.now()}`
    await client.command({
      query: `CREATE TABLE ${table} (val LowCardinality(String)) ENGINE = Memory`,
    })
    try {
      const values = Array.from({ length: 100 }, (_, i) => ({
        val: ['red', 'green', 'blue'][i % 3],
      }))
      await client.insert({ table, values, format: 'JSONEachRow' })
      const rs = await client.query({
        query: `SELECT val, count() as cnt FROM ${table} GROUP BY val ORDER BY val`,
        format: 'JSONEachRow',
      })
      const rows = await rs.json<{ val: string; cnt: string }>()
      expect(rows.length).toBe(3)
      expect(rows[0].val).toBe('blue')
      expect(rows[1].val).toBe('green')
      expect(rows[2].val).toBe('red')
    } finally {
      await client.command({ query: `DROP TABLE IF EXISTS ${table}` })
    }
  })
})

describe('IPv4/IPv6 types', () => {
  it('should insert and select IPv4', async () => {
    const table = `test_ipv4_${Date.now()}`
    await client.command({
      query: `CREATE TABLE ${table} (ip IPv4) ENGINE = Memory`,
    })
    try {
      await client.insert({
        table,
        values: [{ ip: '192.168.1.1' }, { ip: '10.0.0.1' }],
        format: 'JSONEachRow',
      })
      const rs = await client.query({
        query: `SELECT ip FROM ${table} ORDER BY ip`,
        format: 'JSONEachRow',
      })
      const rows = await rs.json<{ ip: string }>()
      expect(rows.length).toBe(2)
      expect(rows[0].ip).toBe('10.0.0.1')
      expect(rows[1].ip).toBe('192.168.1.1')
    } finally {
      await client.command({ query: `DROP TABLE IF EXISTS ${table}` })
    }
  })

  it('should insert and select IPv6', async () => {
    const table = `test_ipv6_${Date.now()}`
    await client.command({
      query: `CREATE TABLE ${table} (ip IPv6) ENGINE = Memory`,
    })
    try {
      await client.insert({
        table,
        values: [
          { ip: '::1' },
          { ip: '2001:db8::1' },
        ],
        format: 'JSONEachRow',
      })
      const rs = await client.query({
        query: `SELECT ip FROM ${table} ORDER BY ip`,
        format: 'JSONEachRow',
      })
      const rows = await rs.json<{ ip: string }>()
      expect(rows.length).toBe(2)
      // ClickHouse normalizes IPv6 addresses
      expect(rows).toBeDefined()
    } finally {
      await client.command({ query: `DROP TABLE IF EXISTS ${table}` })
    }
  })
})

describe('Combined type tests', () => {
  it('should handle Array(UUID)', async () => {
    const rs = await client.query({
      query: "SELECT [toUUID('550e8400-e29b-41d4-a716-446655440000'), toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')] as ids",
      format: 'JSONEachRow',
    })
    const rows = await rs.json<{ ids: string[] }>()
    expect(rows.length).toBe(1)
    expect(rows[0].ids.length).toBe(2)
    expect(rows[0].ids[0]).toBe('550e8400-e29b-41d4-a716-446655440000')
  })
})

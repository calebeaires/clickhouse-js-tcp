import type { BenchmarkResult } from '../helpers'
import { measure } from '../helpers'

function generateRows(count: number): Array<{ id: number; value: string }> {
  const rows = []
  for (let i = 0; i < count; i++) {
    rows.push({ id: i, value: `value_${i}` })
  }
  return rows
}

async function ensureTable(client: any, table: string): Promise<void> {
  await client.command({
    query: `CREATE TABLE IF NOT EXISTS ${table} (id UInt32, value String) ENGINE = Memory`,
  })
}

async function truncateTable(client: any, table: string): Promise<void> {
  await client.command({ query: `TRUNCATE TABLE ${table}` })
}

export async function runInsert(tcpClient: any, httpClient: any): Promise<BenchmarkResult[]> {
  console.log('  Running: INSERT 10k rows (3 iterations)...')

  const table10k = 'benchmark_insert_10k'
  const table100k = 'benchmark_insert_100k'

  // Pre-generate data before timing
  const data10k = generateRows(10_000)
  const data100k = generateRows(100_000)

  // Create tables using TCP client
  await ensureTable(tcpClient, table10k)
  await ensureTable(tcpClient, table100k)

  // INSERT 10k
  const tcp10k = await measure(async () => {
    await truncateTable(tcpClient, table10k)
    await tcpClient.insert({ table: table10k, values: data10k, format: 'JSONEachRow' })
  }, 3)

  const http10k = await measure(async () => {
    await truncateTable(tcpClient, table10k)
    await httpClient.insert({ table: table10k, values: data10k, format: 'JSONEachRow' })
  }, 3)

  console.log('  Running: INSERT 100k rows (3 iterations)...')

  // INSERT 100k
  const tcp100k = await measure(async () => {
    await truncateTable(tcpClient, table100k)
    await tcpClient.insert({ table: table100k, values: data100k, format: 'JSONEachRow' })
  }, 3)

  const http100k = await measure(async () => {
    await truncateTable(tcpClient, table100k)
    await httpClient.insert({ table: table100k, values: data100k, format: 'JSONEachRow' })
  }, 3)

  // Cleanup
  await tcpClient.command({ query: `DROP TABLE IF EXISTS ${table10k}` })
  await tcpClient.command({ query: `DROP TABLE IF EXISTS ${table100k}` })

  return [
    { scenario: 'INSERT 10k rows', rows: '10,000', tcpMs: tcp10k, httpMs: http10k },
    { scenario: 'INSERT 100k rows', rows: '100,000', tcpMs: tcp100k, httpMs: http100k },
  ]
}

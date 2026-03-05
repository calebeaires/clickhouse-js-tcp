import type { BenchmarkResult } from '../helpers'
import { measure } from '../helpers'

export async function runSelectSmall(tcpClient: any, httpClient: any): Promise<BenchmarkResult> {
  console.log('  Running: SELECT 1,000 rows (10 iterations)...')

  const query = 'SELECT toUInt32(number) as n FROM system.numbers LIMIT 1000'

  const tcpMs = await measure(async () => {
    const rs = await tcpClient.query({ query, format: 'JSONEachRow' })
    await rs.json()
  }, 10)

  const httpMs = await measure(async () => {
    const rs = await httpClient.query({ query, format: 'JSONEachRow' })
    await rs.json()
  }, 10)

  return { scenario: 'SELECT 1k rows', rows: '1,000', tcpMs, httpMs }
}

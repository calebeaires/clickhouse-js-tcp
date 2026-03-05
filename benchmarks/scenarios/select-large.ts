import type { BenchmarkResult } from '../helpers'
import { measure } from '../helpers'

export async function runSelectLarge(
  tcpClient: any,
  httpClient: any,
): Promise<BenchmarkResult> {
  console.log('  Running: SELECT 100,000 rows (5 iterations)...')

  const query =
    'SELECT toUInt32(number) as n, toString(number) as str FROM system.numbers LIMIT 100000'

  const tcpMs = await measure(async () => {
    const rs = await tcpClient.query({ query, format: 'JSONEachRow' })
    await rs.json()
  }, 5)

  const httpMs = await measure(async () => {
    const rs = await httpClient.query({ query, format: 'JSONEachRow' })
    await rs.json()
  }, 5)

  return { scenario: 'SELECT 100k rows', rows: '100,000', tcpMs, httpMs }
}

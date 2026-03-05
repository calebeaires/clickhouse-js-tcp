import type { BenchmarkResult } from '../helpers'
import { measure } from '../helpers'

export async function runStreamSelect(tcpClient: any, httpClient: any): Promise<BenchmarkResult> {
  console.log('  Running: Stream SELECT 500k rows (3 iterations)...')

  const query = 'SELECT toUInt32(number) as n, toString(number) as str FROM system.numbers LIMIT 500000'

  const tcpMs = await measure(async () => {
    const rs = await tcpClient.query({ query, format: 'JSONEachRow' })
    const stream = rs.stream()
    let count = 0
    for await (const chunk of stream) {
      count++
      void chunk
    }
  }, 3)

  const httpMs = await measure(async () => {
    const rs = await httpClient.query({ query, format: 'JSONEachRow' })
    const stream = rs.stream()
    let count = 0
    for await (const rows of stream) {
      if (Array.isArray(rows)) {
        for (const row of rows) {
          count++
          void row
        }
      } else {
        count++
        void rows
      }
    }
  }, 3)

  return { scenario: 'Stream 500k rows', rows: '500,000', tcpMs, httpMs }
}

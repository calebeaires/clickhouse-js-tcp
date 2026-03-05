import type { BenchmarkResult } from '../helpers'
import { measure } from '../helpers'

export async function runPing(
  tcpClient: any,
  httpClient: any,
): Promise<BenchmarkResult> {
  console.log('  Running: Ping (50 iterations, 5 warmup)...')

  const tcpMs = await measure(() => tcpClient.ping(), 50, 5)
  const httpMs = await measure(() => httpClient.ping(), 50, 5)

  return { scenario: 'Ping (avg x50)', rows: '-', tcpMs, httpMs }
}

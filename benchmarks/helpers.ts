export interface BenchmarkResult {
  scenario: string
  rows: string
  tcpMs: number
  httpMs: number
}

export function loadEnv(): { host: string; password: string } {
  const fs = require('fs')
  const path = require('path')
  const envPath = path.resolve(__dirname, '..', '.env')
  if (fs.existsSync(envPath)) {
    const content = fs.readFileSync(envPath, 'utf-8')
    for (const line of content.split('\n')) {
      const match = line.match(/^([^#=]+)=(.*)$/)
      if (match) {
        process.env[match[1].trim()] = match[2].trim()
      }
    }
  }
  const host = process.env.GAIO_CLICKHOUSE_HOST
  const password = process.env.GAIO_CLICKHOUSE_PASSWORD ?? ''
  if (!host) {
    throw new Error('GAIO_CLICKHOUSE_HOST not set in .env or environment')
  }
  return { host, password }
}

export async function measure(
  fn: () => Promise<void>,
  iterations: number,
  warmup: number = 0,
): Promise<number> {
  for (let i = 0; i < warmup; i++) {
    await fn()
  }
  const times: number[] = []
  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    await fn()
    times.push(performance.now() - start)
  }
  return times.reduce((a, b) => a + b, 0) / times.length
}

export function printTable(results: BenchmarkResult[]): void {
  const col = { scenario: 21, rows: 9, time: 12, speedup: 10 }
  const pad = (s: string, w: number) => s.padEnd(w)
  const rpad = (s: string, w: number) => s.padStart(w)
  const hr = (c: string) =>
    `${c}${''.padEnd(col.scenario, '\u2500')}${c}${''.padEnd(col.rows, '\u2500')}${c}${''.padEnd(col.time, '\u2500')}${c}${''.padEnd(col.time, '\u2500')}${c}${''.padEnd(col.speedup, '\u2500')}${c}`

  console.log('\n=== ClickHouse Benchmark: TCP vs HTTP ===\n')
  console.log(hr('\u250c').replace(/\u250c/g, (_, i) => (i === 0 ? '\u250c' : '\u252c')).replace(/^./, '\u250c').replace(/.$/, '\u2510'))

  const header = `\u2502 ${pad('Scenario', col.scenario - 2)} \u2502 ${pad('Rows', col.rows - 2)} \u2502 ${rpad('TCP (ms)', col.time - 2)} \u2502 ${rpad('HTTP (ms)', col.time - 2)} \u2502 ${rpad('Speedup', col.speedup - 2)} \u2502`
  console.log(header)
  console.log(`\u251c${''.padEnd(col.scenario, '\u2500')}\u253c${''.padEnd(col.rows, '\u2500')}\u253c${''.padEnd(col.time, '\u2500')}\u253c${''.padEnd(col.time, '\u2500')}\u253c${''.padEnd(col.speedup, '\u2500')}\u2524`)

  for (const r of results) {
    const speedup = r.httpMs / r.tcpMs
    const row = `\u2502 ${pad(r.scenario, col.scenario - 2)} \u2502 ${pad(r.rows, col.rows - 2)} \u2502 ${rpad(r.tcpMs.toFixed(2), col.time - 2)} \u2502 ${rpad(r.httpMs.toFixed(2), col.time - 2)} \u2502 ${rpad(speedup.toFixed(2) + 'x', col.speedup - 2)} \u2502`
    console.log(row)
  }

  console.log(`\u2514${''.padEnd(col.scenario, '\u2500')}\u2534${''.padEnd(col.rows, '\u2500')}\u2534${''.padEnd(col.time, '\u2500')}\u2534${''.padEnd(col.time, '\u2500')}\u2534${''.padEnd(col.speedup, '\u2500')}\u2518`)
}

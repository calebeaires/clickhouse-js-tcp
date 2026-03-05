import { loadEnv, printTable, type BenchmarkResult } from './helpers'
import { runPing } from './scenarios/ping'
import { runSelectSmall } from './scenarios/select-small'
import { runSelectLarge } from './scenarios/select-large'
import { runInsert } from './scenarios/insert'
import { runStreamSelect } from './scenarios/stream-select'

async function main() {
  const { host, password } = loadEnv()

  console.log(`\nConnecting to ClickHouse at ${host}...`)
  console.log(`  TCP port: 9000 | HTTP port: 8123\n`)

  const { createClient: createTcpClient } = await import('../src/index')
  const { createClient: createHttpClient } = await import('@clickhouse/client')

  const makeTcp = () =>
    createTcpClient({ url: `http://${host}:9000`, username: 'default', password })
  const makeHttp = () =>
    createHttpClient({ url: `http://${host}:8123`, username: 'default', password })

  // Verify connectivity
  console.log('Verifying connections...')
  let t = makeTcp()
  await t.ping()
  console.log('  TCP: OK')
  await t.close()
  let h = makeHttp()
  await h.ping()
  console.log('  HTTP: OK')
  await h.close()
  console.log('')

  const results: BenchmarkResult[] = []

  // Each scenario gets fresh clients to avoid protocol state leakage
  const run = async (
    fn: (tcp: any, http: any) => Promise<BenchmarkResult | BenchmarkResult[]>,
  ) => {
    const tcp = makeTcp()
    const http = makeHttp()
    const r = await fn(tcp, http)
    await tcp.close()
    await http.close()
    return Array.isArray(r) ? r : [r]
  }

  results.push(...(await run(runPing)))
  results.push(...(await run(runSelectSmall)))
  results.push(...(await run(runSelectLarge)))
  results.push(...(await run(runInsert)))
  results.push(...(await run(runStreamSelect)))

  printTable(results)
}

main().catch((err) => {
  console.error('Benchmark failed:', err)
  process.exit(1)
})

import * as Stream from 'stream'
import { ClickHouseClient } from '@clickhouse/client-common'
import { TcpImpl, TcpClickHouseClientConfigOptions } from './config'
import type { TcpClickHouseClient } from './client'

export function createClient(
  config?: TcpClickHouseClientConfigOptions,
): TcpClickHouseClient {
  const defaultUrl = 'http://localhost:9000'
  let url = config?.url ?? defaultUrl
  let tls = config?.tls

  // Convert tcp:// and tcps:// schemes to http:// and https://
  if (typeof url === 'string') {
    if (url.startsWith('tcps://')) {
      url = 'https://' + url.slice(7)
      // Enable TLS if not explicitly configured
      if (!tls) tls = {}
    } else if (url.startsWith('tcp://')) {
      url = 'http://' + url.slice(6)
    }
  }

  return new ClickHouseClient<Stream.Readable>({
    impl: TcpImpl,
    ...(config || {}),
    url,
    tls,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } as any) as TcpClickHouseClient
}

// Re-export types
export type { TcpClickHouseClient } from './client'
export type { TcpClickHouseClientConfigOptions } from './config'
export { version } from './version'

// Re-export common types users might need
export type {
  DataFormat,
  ClickHouseSettings,
  BaseResultSet,
} from '@clickhouse/client-common'

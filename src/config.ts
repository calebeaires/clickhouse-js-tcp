import * as Stream from 'stream'
import type {
  BaseClickHouseClientConfigOptions,
  ImplementationDetails,
  ConnectionParams,
  ResponseHeaders,
} from '@clickhouse/client-common'
import { TcpConnectionPool } from './connection/pool'
import { PooledConnection } from './connection/pooled_connection'
import { TcpValuesEncoder } from './utils/encoder'
import { TcpResultSet } from './result_set'

export type TcpClickHouseClientConfigOptions = BaseClickHouseClientConfigOptions & {
  tls?: {
    ca_cert?: Buffer | string
    cert?: Buffer | string
    key?: Buffer | string
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const TcpImpl: ImplementationDetails<Stream.Readable>['impl'] = {
  make_connection: (
    _config: BaseClickHouseClientConfigOptions,
    params: ConnectionParams,
  ) => {
    const tcpConfig = _config as TcpClickHouseClientConfigOptions
    const tlsConfig = tcpConfig.tls
      ? {
          ca: tcpConfig.tls.ca_cert,
          cert: tcpConfig.tls.cert,
          key: tcpConfig.tls.key,
        }
      : undefined
    const extra = { tls: tlsConfig }
    const pool = new TcpConnectionPool(params, extra)
    return new PooledConnection(pool)
  },

  values_encoder: () => new TcpValuesEncoder(),

  make_result_set: ((
    stream: Stream.Readable,
    format: string,
    query_id: string,
    log_error: (err: Error) => void,
    response_headers: ResponseHeaders,
    json: unknown,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  ): any => {
    return TcpResultSet.instance({
      stream,
      format,
      query_id,
      log_error,
      response_headers,
      json,
    })
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  }) as any,
}

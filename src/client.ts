import type * as Stream from 'stream'
import type { ClickHouseClient } from '@clickhouse/client-common'

export type TcpClickHouseClient = ClickHouseClient<Stream.Readable>

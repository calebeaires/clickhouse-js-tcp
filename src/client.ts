import * as Stream from 'stream'
import { ClickHouseClient } from '@clickhouse/client-common'

export type TcpClickHouseClient = ClickHouseClient<Stream.Readable>

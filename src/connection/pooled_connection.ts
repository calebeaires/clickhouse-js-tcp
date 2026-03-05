import type * as Stream from 'stream'
import type {
  Connection,
  ConnPingParams,
  ConnPingResult,
  ConnBaseQueryParams,
  ConnQueryResult,
  ConnInsertParams,
  ConnInsertResult,
  ConnCommandResult,
  ConnExecParams,
  ConnExecResult,
} from '@clickhouse/client-common'
import type { TcpConnectionPool } from './pool'

/**
 * A Connection wrapper that acquires a connection from the pool for each
 * operation and releases it when done.
 */
export class PooledConnection implements Connection<Stream.Readable> {
  private pool: TcpConnectionPool

  constructor(pool: TcpConnectionPool) {
    this.pool = pool
  }

  async ping(params?: ConnPingParams): Promise<ConnPingResult> {
    const conn = await this.pool.acquire()
    try {
      return await conn.ping(params)
    } finally {
      this.pool.release(conn)
    }
  }

  async query(
    params: ConnBaseQueryParams,
  ): Promise<ConnQueryResult<Stream.Readable>> {
    const conn = await this.pool.acquire()
    try {
      const result = await conn.query(params)
      // We need to release the connection after the stream is consumed
      const originalStream = result.stream
      const pool = this.pool
      let released = false
      const release = () => {
        if (!released) {
          released = true
          pool.release(conn)
        }
      }
      originalStream.on('end', release)
      originalStream.on('error', release)
      originalStream.on('close', release)
      return result
    } catch (err) {
      this.pool.release(conn)
      throw err
    }
  }

  async insert(
    params: ConnInsertParams<Stream.Readable>,
  ): Promise<ConnInsertResult> {
    const conn = await this.pool.acquire()
    try {
      const result = await conn.insert(params)
      this.pool.release(conn)
      return result
    } catch (err) {
      this.pool.release(conn)
      throw err
    }
  }

  async command(
    params: ConnBaseQueryParams & { ignore_error_response?: boolean },
  ): Promise<ConnCommandResult> {
    const conn = await this.pool.acquire()
    try {
      const result = await conn.command(params)
      this.pool.release(conn)
      return result
    } catch (err) {
      this.pool.release(conn)
      throw err
    }
  }

  async exec(
    params: ConnExecParams<Stream.Readable>,
  ): Promise<ConnExecResult<Stream.Readable>> {
    const conn = await this.pool.acquire()
    try {
      const result = await conn.exec(params)
      const originalStream = result.stream
      const pool = this.pool
      let released = false
      const release = () => {
        if (!released) {
          released = true
          pool.release(conn)
        }
      }
      originalStream.on('end', release)
      originalStream.on('error', release)
      originalStream.on('close', release)
      return result
    } catch (err) {
      this.pool.release(conn)
      throw err
    }
  }

  async close(): Promise<void> {
    await this.pool.close()
  }
}

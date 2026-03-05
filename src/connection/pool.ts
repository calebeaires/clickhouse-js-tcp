import type { ConnectionParams } from '@clickhouse/client-common'
import { TcpConnection, TcpConnectionExtra } from './tcp_connection'

interface Waiter {
  resolve: (conn: TcpConnection) => void
  reject: (err: Error) => void
  timer: ReturnType<typeof setTimeout>
}

export class TcpConnectionPool {
  private idle: TcpConnection[] = []
  private active = new Set<TcpConnection>()
  private waitQueue: Waiter[] = []
  private maxConnections: number
  private params: ConnectionParams
  private extra: TcpConnectionExtra
  private closed = false
  private idleTimeout: number
  private sweepTimer: ReturnType<typeof setInterval> | null = null

  constructor(params: ConnectionParams, extra?: TcpConnectionExtra) {
    this.params = params
    this.extra = extra || {}
    this.maxConnections = params.max_open_connections || 10
    this.idleTimeout = 60000 // 60 seconds default

    // Start periodic sweep for idle connections
    this.sweepTimer = setInterval(() => this.sweepIdle(), 30000)
    // Unref so it doesn't prevent process exit
    if (this.sweepTimer.unref) this.sweepTimer.unref()
  }

  async acquire(timeout = 30000): Promise<TcpConnection> {
    if (this.closed) throw new Error('Connection pool is closed')

    // Try to get an idle connection with health check
    while (this.idle.length > 0) {
      const conn = this.idle.pop()!

      // Check if connection has been idle too long
      if (Date.now() - conn.lastUsed > this.idleTimeout) {
        conn.close().catch(() => {})
        continue
      }

      // Quick health check via ping
      try {
        const result = await conn.ping()
        if (result.success) {
          this.active.add(conn)
          return conn
        }
      } catch {
        // Ping failed, discard this connection
      }
      conn.close().catch(() => {})
    }

    // Try to create a new connection
    if (this.active.size < this.maxConnections) {
      const conn = new TcpConnection(this.params, this.extra)
      await conn.connect()
      this.active.add(conn)
      return conn
    }

    // Wait for a connection to become available
    return new Promise<TcpConnection>((resolve, reject) => {
      const timer = setTimeout(() => {
        const idx = this.waitQueue.findIndex((w) => w.resolve === resolve)
        if (idx >= 0) this.waitQueue.splice(idx, 1)
        reject(new Error('Connection pool acquire timeout'))
      }, timeout)

      this.waitQueue.push({ resolve, reject, timer })
    })
  }

  release(conn: TcpConnection): void {
    this.active.delete(conn)

    if (this.closed) {
      conn.close().catch(() => {})
      return
    }

    conn.lastUsed = Date.now()

    // If someone is waiting, give them this connection
    if (this.waitQueue.length > 0) {
      const waiter = this.waitQueue.shift()!
      clearTimeout(waiter.timer)
      this.active.add(conn)
      waiter.resolve(conn)
      return
    }

    this.idle.push(conn)
  }

  private sweepIdle(): void {
    const now = Date.now()
    const toClose: TcpConnection[] = []
    this.idle = this.idle.filter((conn) => {
      if (now - conn.lastUsed > this.idleTimeout) {
        toClose.push(conn)
        return false
      }
      return true
    })
    for (const conn of toClose) {
      conn.close().catch(() => {})
    }
  }

  async close(): Promise<void> {
    this.closed = true

    if (this.sweepTimer) {
      clearInterval(this.sweepTimer)
      this.sweepTimer = null
    }

    // Reject all waiters
    for (const waiter of this.waitQueue) {
      clearTimeout(waiter.timer)
      waiter.reject(new Error('Connection pool closed'))
    }
    this.waitQueue = []

    // Close all connections
    const allConns = [...this.idle, ...this.active]
    this.idle = []
    this.active.clear()

    await Promise.all(allConns.map((c) => c.close().catch(() => {})))
  }

  get stats() {
    return {
      idle: this.idle.length,
      active: this.active.size,
      waiting: this.waitQueue.length,
      total: this.idle.length + this.active.size,
    }
  }
}

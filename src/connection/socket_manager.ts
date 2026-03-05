import * as net from 'net'
import * as tls from 'tls'
import { EventEmitter } from 'events'

export interface SocketConfig {
  host: string
  port: number
  connectTimeout: number
  tls?: {
    ca?: Buffer | string
    cert?: Buffer | string
    key?: Buffer | string
    servername?: string
    rejectUnauthorized?: boolean
  }
}

export class SocketManager extends EventEmitter {
  private socket: net.Socket | null = null
  private config: SocketConfig
  private connected = false
  private destroyed = false

  constructor(config: SocketConfig) {
    super()
    this.config = config
  }

  async connect(): Promise<void> {
    if (this.connected) return

    return new Promise<void>((resolve, reject) => {
      const onError = (err: Error) => {
        cleanup()
        reject(err)
      }

      const onConnect = () => {
        cleanup()
        this.connected = true
        resolve()
      }

      const cleanup = () => {
        if (this.socket) {
          this.socket.removeListener('error', onError)
          this.socket.removeListener('connect', onConnect)
          this.socket.removeListener('secureConnect', onConnect)
        }
      }

      const timeout = this.config.connectTimeout

      if (this.config.tls) {
        this.socket = tls.connect(
          {
            host: this.config.host,
            port: this.config.port,
            ca: this.config.tls.ca,
            cert: this.config.tls.cert,
            key: this.config.tls.key,
            servername: this.config.tls.servername || this.config.host,
            rejectUnauthorized: this.config.tls.rejectUnauthorized ?? true,
            timeout,
          },
          onConnect,
        )
      } else {
        this.socket = net.connect(
          {
            host: this.config.host,
            port: this.config.port,
            timeout,
          },
          onConnect,
        )
      }

      this.socket.setNoDelay(true)
      this.socket.once('error', onError)

      this.socket.on('data', (data: Buffer) => {
        this.emit('data', data)
      })

      this.socket.on('close', () => {
        this.connected = false
        this.emit('close')
      })

      this.socket.on('error', (err: Error) => {
        this.emit('error', err)
      })

      this.socket.on('timeout', () => {
        this.emit('timeout')
      })
    })
  }

  write(data: Buffer): boolean {
    if (!this.socket || !this.connected) {
      throw new Error('Socket is not connected')
    }
    return this.socket.write(data)
  }

  isConnected(): boolean {
    return this.connected && !this.destroyed
  }

  destroySync(): void {
    this.destroyed = true
    this.connected = false
    if (this.socket) {
      this.socket.removeAllListeners()
      this.socket.destroy()
      this.socket = null
    }
  }

  async close(): Promise<void> {
    if (this.destroyed) return
    this.destroyed = true
    this.connected = false

    return new Promise<void>((resolve) => {
      if (!this.socket) {
        resolve()
        return
      }
      this.socket.once('close', () => resolve())
      this.socket.destroy()
      this.socket = null
    })
  }
}

import * as Stream from 'stream'
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
  ConnectionParams,
} from '@clickhouse/client-common'
import { SocketManager } from './socket_manager'
import { PacketReader, ServerPacket } from './packet_reader'
import { PacketWriter } from './packet_writer'
import { ServerPacketType } from '../protocol/packet_types'
import { ServerException } from '../protocol/handshake'
import { Block, ColumnData } from '../protocol/data_packet'
import { getCodec } from '../columns/registry'
import { BinaryReader } from '../protocol/binary_reader'
import { blockToRows } from '../utils/block_to_rows'
import { rowsToBlock } from '../utils/rows_to_block'

export interface TcpConnectionExtra {
  tls?: {
    ca?: Buffer | string
    cert?: Buffer | string
    key?: Buffer | string
  }
}

export class TcpConnection implements Connection<Stream.Readable> {
  private socketManager: SocketManager
  private packetReader: PacketReader
  private packetWriter!: PacketWriter
  serverInfo: {
    serverName: string
    revision: number
    timezone: string
    displayName: string
  } | null = null
  private params: ConnectionParams
  private extra: TcpConnectionExtra
  private connected = false
  private pendingResolves: Array<(packet: ServerPacket) => void> = []
  lastUsed: number = Date.now()

  constructor(config: ConnectionParams, extra?: TcpConnectionExtra) {
    this.params = config
    this.extra = extra || {}

    this.socketManager = this.createSocketManager()
    this.packetReader = new PacketReader()
  }

  async connect(): Promise<void> {
    if (this.connected) return

    await this.socketManager.connect()

    this.socketManager.on('data', (data: Buffer) => {
      this.packetReader.feed(data)
      this.drainPackets()
    })

    this.packetWriter = new PacketWriter(this.socketManager)

    // Send Hello
    const auth = this.params.auth
    const username = auth.type === 'Credentials' ? auth.username : 'default'
    const password = auth.type === 'Credentials' ? auth.password : ''

    this.packetWriter.sendHello({
      database: this.params.database,
      username,
      password,
    })

    // Wait for Hello response
    const packet = await this.waitForPacket()

    if (packet.type === ServerPacketType.Exception) {
      throw this.exceptionToError(packet.data)
    }

    if (packet.type !== ServerPacketType.Hello) {
      throw new Error(`Expected Hello response, got ${packet.type}`)
    }

    this.serverInfo = {
      serverName: packet.data.serverName,
      revision: packet.data.revision,
      timezone: packet.data.timezone,
      displayName: packet.data.displayName,
    }

    // Note: for protocol version >= 54456, server expects a client addendum.
    // We advertise 54423 so no addendum is needed.

    this.connected = true
  }

  async ping(_params?: ConnPingParams): Promise<ConnPingResult> {
    try {
      await this.ensureConnected()
      this.packetWriter.sendPing()
      const packet = await this.waitForPacket()
      if (packet.type === ServerPacketType.Pong) {
        return { success: true }
      }
      return { success: false, error: new Error('Unexpected response to ping') }
    } catch (e) {
      return { success: false, error: e as Error }
    }
  }

  async query(params: ConnBaseQueryParams): Promise<ConnQueryResult<Stream.Readable>> {
    return this.withRetry(() => this._query(params))
  }

  private async _query(params: ConnBaseQueryParams): Promise<ConnQueryResult<Stream.Readable>> {
    await this.ensureConnected()
    this.checkAborted(params.abort_signal)
    this.lastUsed = Date.now()

    const queryId = params.query_id || generateQueryId()
    const auth = this.params.auth
    const username = auth.type === 'Credentials' ? auth.username : 'default'

    const useCompression = this.params.compression.decompress_response
    this.packetReader.decompress = useCompression

    this.packetWriter.sendQuery({
      queryId,
      query: params.query,
      database: this.params.database,
      username,
      compression: useCompression,
      settings: params.clickhouse_settings as Record<string, string | number | boolean> | undefined,
    })

    const blocks: Block[] = []

    // Read all response packets
    while (true) {
      const packet = await this.waitForPacketWithAbort(params.abort_signal)

      switch (packet.type) {
        case ServerPacketType.Exception: {
          this.packetReader.decompress = false
          await this.resetConnection()
          throw this.exceptionToError(packet.data)
        }

        case ServerPacketType.Data:
        case ServerPacketType.Totals:
        case ServerPacketType.Extremes: {
          const { header, rawReader } = packet.data
          if (header.numRows > 0 && header.numColumns > 0) {
            const block = await this.readBlockColumns(rawReader, header.numColumns, header.numRows)
            blocks.push(block)
          } else if (header.numColumns > 0) {
            await this.skipBlockColumns(rawReader, header.numColumns, 0)
          } else {
            this.consumeFromReader(rawReader)
          }
          break
        }

        case ServerPacketType.Progress:
          break

        case ServerPacketType.ProfileInfo:
          break

        case ServerPacketType.ProfileEvents: {
          const { header: h, rawReader: r } = packet.data
          if (h.numRows > 0 && h.numColumns > 0) {
            await this.skipBlockColumns(r, h.numColumns, h.numRows)
          } else {
            this.consumeFromReader(r)
          }
          break
        }

        case ServerPacketType.EndOfStream: {
          this.packetReader.decompress = false
          const stream = this.blocksToStream(blocks)
          return {
            stream,
            query_id: queryId,
            response_headers: {},
          }
        }

        case ServerPacketType.TableColumns:
          break

        default:
          break
      }
    }
  }

  async insert(params: ConnInsertParams<Stream.Readable>): Promise<ConnInsertResult> {
    return this.withRetry(() => this._insert(params))
  }

  private async _insert(params: ConnInsertParams<Stream.Readable>): Promise<ConnInsertResult> {
    await this.ensureConnected()
    this.checkAborted(params.abort_signal)
    this.lastUsed = Date.now()

    const queryId = params.query_id || generateQueryId()
    const auth = this.params.auth
    const username = auth.type === 'Credentials' ? auth.username : 'default'

    const compressInsert = this.params.compression.compress_request

    this.packetWriter.sendQuery({
      queryId,
      query: params.query,
      database: this.params.database,
      username,
      compression: compressInsert,
    })

    // Read schema block from server
    let schemaColumns: Array<{ name: string; type: string }> = []

    while (true) {
      const packet = await this.waitForPacketWithAbort(params.abort_signal)

      if (packet.type === ServerPacketType.Exception) {
        await this.resetConnection()
        throw this.exceptionToError(packet.data)
      }

      if (packet.type === ServerPacketType.TableColumns) {
        continue
      }

      if (
        packet.type === ServerPacketType.Data
      ) {
        const { header, rawReader } = packet.data
        // This is the schema block — read column names and types
        for (let i = 0; i < header.numColumns; i++) {
          const name = rawReader.readString()
          const type = rawReader.readString()
          schemaColumns.push({ name, type })
        }
        // Feed back any remaining data
        this.consumeFromReader(rawReader)
        break
      }
    }

    const sendBlock = (block: Block) => {
      const columnWriters = block.columns.map((col) => getCodec(col.type))
      if (compressInsert) {
        this.packetWriter.sendCompressedDataBlock(block, columnWriters)
      } else {
        this.packetWriter.sendDataBlock(block, columnWriters)
      }
    }

    // Convert values to blocks and send
    if (typeof params.values === 'string') {
      const rows = JSON.parse(params.values) as Record<string, unknown>[]
      sendBlock(rowsToBlock(rows, schemaColumns))
    } else if (params.values instanceof Stream.Readable) {
      // Streaming insert — batched
      const BATCH_SIZE = 10000
      let batch: Record<string, unknown>[] = []
      for await (const chunk of params.values) {
        const rows = typeof chunk === 'string' ? JSON.parse(chunk) : chunk
        if (Array.isArray(rows)) {
          batch.push(...rows)
        } else {
          batch.push(rows)
        }
        if (batch.length >= BATCH_SIZE) {
          sendBlock(rowsToBlock(batch, schemaColumns))
          batch = []
        }
      }
      if (batch.length > 0) {
        sendBlock(rowsToBlock(batch, schemaColumns))
      }
    }

    // Send empty block to signal end
    this.packetWriter.sendEmptyBlock()

    // Wait for EndOfStream
    const summaryData: Record<string, string> = {}
    while (true) {
      const packet = await this.waitForPacketWithAbort(params.abort_signal)
      if (packet.type === ServerPacketType.Exception) {
        await this.resetConnection()
        throw this.exceptionToError(packet.data)
      }
      if (packet.type === ServerPacketType.EndOfStream) {
        break
      }
      if (packet.type === ServerPacketType.Progress) {
        const p = packet.data
        summaryData.written_rows = String(p.writtenRows)
        summaryData.written_bytes = String(p.writtenBytes)
      }
    }

    return {
      query_id: queryId,
      response_headers: {},
      summary: {
        read_rows: summaryData.read_rows ?? '0',
        read_bytes: summaryData.read_bytes ?? '0',
        written_rows: summaryData.written_rows ?? '0',
        written_bytes: summaryData.written_bytes ?? '0',
        total_rows_to_read: '0',
        result_rows: '0',
        result_bytes: '0',
        elapsed_ns: '0',
      },
    }
  }

  async command(params: ConnBaseQueryParams & { ignore_error_response?: boolean }): Promise<ConnCommandResult> {
    return this.withRetry(() => this._command(params))
  }

  private async _command(params: ConnBaseQueryParams & { ignore_error_response?: boolean }): Promise<ConnCommandResult> {
    await this.ensureConnected()
    this.checkAborted(params.abort_signal)
    this.lastUsed = Date.now()

    const queryId = params.query_id || generateQueryId()
    const auth = this.params.auth
    const username = auth.type === 'Credentials' ? auth.username : 'default'

    this.packetWriter.sendQuery({
      queryId,
      query: params.query,
      database: this.params.database,
      username,
      compression: false,
      settings: params.clickhouse_settings as Record<string, string | number | boolean> | undefined,
    })

    while (true) {
      const packet = await this.waitForPacketWithAbort(params.abort_signal)
      if (packet.type === ServerPacketType.Exception) {
        await this.resetConnection()
        throw this.exceptionToError(packet.data)
      }
      if (packet.type === ServerPacketType.EndOfStream) {
        return {
          query_id: queryId,
          response_headers: {},
          summary: {
            read_rows: '0',
            read_bytes: '0',
            written_rows: '0',
            written_bytes: '0',
            total_rows_to_read: '0',
            result_rows: '0',
            result_bytes: '0',
            elapsed_ns: '0',
          },
        }
      }
      // Skip data blocks, progress, etc.
      if (
        packet.type === ServerPacketType.Data ||
        packet.type === ServerPacketType.ProfileEvents
      ) {
        const { header, rawReader } = packet.data
        if (header.numRows > 0 && header.numColumns > 0) {
          await this.skipBlockColumns(rawReader, header.numColumns, header.numRows)
        } else if (header.numColumns > 0) {
          await this.skipBlockColumns(rawReader, header.numColumns, 0)
        } else {
          this.consumeFromReader(rawReader)
        }
      }
    }
  }

  async exec(params: ConnExecParams<Stream.Readable>): Promise<ConnExecResult<Stream.Readable>> {
    const result = await this.query(params)
    return {
      ...result,
      summary: {
        read_rows: '0',
        read_bytes: '0',
        written_rows: '0',
        written_bytes: '0',
        total_rows_to_read: '0',
        result_rows: '0',
        result_bytes: '0',
        elapsed_ns: '0',
      },
    }
  }

  async close(): Promise<void> {
    this.connected = false
    await this.socketManager.close()
  }

  // --- Private helpers ---

  private createSocketManager(): SocketManager {
    const url = this.params.url
    const host = url.hostname || 'localhost'
    const port = parseInt(url.port, 10) || 9000
    return new SocketManager({
      host,
      port,
      connectTimeout: this.params.request_timeout,
      tls: this.extra.tls
        ? {
            ca: this.extra.tls.ca,
            cert: this.extra.tls.cert,
            key: this.extra.tls.key,
          }
        : undefined,
    })
  }

  private async resetConnection(): Promise<void> {
    this.connected = false
    this.packetReader.reset()
    this.pendingResolves = []
    try {
      this.socketManager.destroySync()
    } catch {
      // ignore close errors
    }
    this.socketManager = this.createSocketManager()
    this.packetWriter = new PacketWriter(this.socketManager)
  }

  private async ensureConnected(): Promise<void> {
    if (!this.connected) {
      await this.connect()
    }
  }

  private checkAborted(signal?: AbortSignal): void {
    if (signal?.aborted) {
      throw new Error('The operation was aborted')
    }
  }

  private async waitForPacketWithAbort(signal?: AbortSignal): Promise<ServerPacket> {
    if (!signal) return this.waitForPacket()

    return new Promise<ServerPacket>((resolve, reject) => {
      if (signal.aborted) {
        reject(new Error('The operation was aborted'))
        return
      }

      const onAbort = () => {
        try {
          this.packetWriter.sendCancel()
        } catch {
          // ignore
        }
        this.resetConnection().catch(() => {})
        reject(new Error('The operation was aborted'))
      }

      signal.addEventListener('abort', onAbort, { once: true })

      this.waitForPacket().then(
        (packet) => {
          signal.removeEventListener('abort', onAbort)
          resolve(packet)
        },
        (err) => {
          signal.removeEventListener('abort', onAbort)
          reject(err)
        },
      )
    })
  }

  private waitForPacket(): Promise<ServerPacket> {
    // First try to read from buffered data
    const packet = this.packetReader.tryReadPacket()
    if (packet) return Promise.resolve(packet)

    const timeout = this.params.request_timeout

    // Otherwise wait for more data
    return new Promise<ServerPacket>((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined

      const wrappedResolve = (pkt: ServerPacket) => {
        if (timer) clearTimeout(timer)
        resolve(pkt)
      }

      this.pendingResolves.push(wrappedResolve)

      if (timeout > 0) {
        timer = setTimeout(() => {
          // Remove from pending resolves
          const idx = this.pendingResolves.indexOf(wrappedResolve)
          if (idx >= 0) this.pendingResolves.splice(idx, 1)
          // Try to cancel on server and reset connection
          try {
            this.packetWriter.sendCancel()
          } catch {
            // ignore - connection may already be broken
          }
          this.resetConnection().catch(() => {})
          reject(new Error(`Query timeout after ${timeout}ms`))
        }, timeout)
      }

      // Schedule a drain in case data arrived between check and registration
      queueMicrotask(() => this.drainPackets())
    })
  }

  private isConnectionError(err: unknown): boolean {
    if (!(err instanceof Error)) return false
    const msg = err.message.toLowerCase()
    return (
      msg.includes('econnreset') ||
      msg.includes('econnrefused') ||
      msg.includes('epipe') ||
      msg.includes('socket is not connected') ||
      msg.includes('socket hang up') ||
      msg.includes('connection reset') ||
      msg.includes('query timeout')
    )
  }

  private async withRetry<T>(fn: () => Promise<T>): Promise<T> {
    try {
      return await fn()
    } catch (err) {
      if (this.isConnectionError(err)) {
        await this.resetConnection()
        return await fn()
      }
      throw err
    }
  }

  private drainPackets(): void {
    while (this.pendingResolves.length > 0) {
      const packet = this.packetReader.tryReadPacket()
      if (!packet) break
      const resolve = this.pendingResolves.shift()!
      resolve(packet)
    }
  }

  private async readBlockColumns(
    reader: BinaryReader,
    numColumns: number,
    numRows: number,
  ): Promise<Block> {
    // Column data may span multiple TCP packets. If we don't have enough
    // data, feed the unread portion back to the packet reader, wait for
    // more socket data, and retry with a fresh reader.
    while (true) {
      try {
        const columns: ColumnData[] = []
        const startOffset = reader.offset
        for (let i = 0; i < numColumns; i++) {
          const name = reader.readString()
          const type = reader.readString()
          const codec = getCodec(type)
          const data = codec.read(reader, numRows)
          columns.push({ name, type, data })
        }
        this.consumeFromReader(reader)
        return {
          info: { isOverflows: false, bucketNum: -1 },
          columns,
          rows: numRows,
        }
      } catch (e) {
        if (e instanceof Error && e.message.startsWith('Not enough data')) {
          // Feed ALL unread data (from the start) back to the packet reader
          const unread = reader.readRawBytesFrom(0)
          this.packetReader.feed(unread)
          // Wait for more socket data
          await this.waitForMoreData()
          // Drain the packet reader buffer into a new reader
          const buf = this.packetReader.drainBuffer()
          reader = new BinaryReader(buf)
          continue
        }
        throw e
      }
    }
  }

  private async skipBlockColumns(
    reader: BinaryReader,
    numColumns: number,
    numRows: number,
  ): Promise<void> {
    while (true) {
      try {
        for (let i = 0; i < numColumns; i++) {
          reader.readString() // name
          const type = reader.readString()
          if (numRows > 0) {
            const codec = getCodec(type)
            codec.read(reader, numRows) // read and discard
          }
        }
        this.consumeFromReader(reader)
        return
      } catch (e) {
        if (e instanceof Error && e.message.startsWith('Not enough data')) {
          const unread = reader.readRawBytesFrom(0)
          this.packetReader.feed(unread)
          await this.waitForMoreData()
          const buf = this.packetReader.drainBuffer()
          reader = new BinaryReader(buf)
          continue
        }
        throw e
      }
    }
  }

  /** Wait until at least one more chunk of data arrives on the socket. */
  private waitForMoreData(): Promise<void> {
    return new Promise<void>((resolve) => {
      const onData = () => {
        this.socketManager.removeListener('data', onData)
        resolve()
      }
      this.socketManager.on('data', onData)
    })
  }

  private consumeFromReader(reader: BinaryReader): void {
    // The rawReader was created from remaining buffer after block header.
    // We need to feed back any unconsumed data to the packetReader.
    const remaining = reader.remaining()
    if (remaining > 0) {
      const leftover = reader.readRawBytes(remaining)
      this.packetReader.feed(leftover)
    }
  }

  private blocksToStream(blocks: Block[]): Stream.Readable {
    const allRows = blockToRows(blocks)
    let index = 0
    const stream = new Stream.Readable({
      objectMode: true,
      read() {
        while (index < allRows.length) {
          if (!this.push(allRows[index++])) return
        }
        this.push(null)
      },
    })
    return stream
  }

  private exceptionToError(ex: ServerException): Error {
    let message = `ClickHouse error ${ex.code}: ${ex.message}`
    if (ex.nested) {
      message += `\nCaused by: ${ex.nested.message}`
    }
    const err = new Error(message)
    ;(err as any).code = ex.code
    return err
  }
}

function generateQueryId(): string {
  return Math.random().toString(36).slice(2) + Date.now().toString(36)
}

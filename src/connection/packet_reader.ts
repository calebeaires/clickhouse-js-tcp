import { BinaryReader } from '../protocol/binary_reader'
import { ServerPacketType } from '../protocol/packet_types'
import type { ServerHello, ServerException } from '../protocol/handshake'
import { readServerHello, readServerException } from '../protocol/handshake'
import type { BlockHeader } from '../protocol/data_packet'
import { readBlockHeader } from '../protocol/data_packet'
import type {
  ProgressPacket,
  ProfileInfoPacket,
} from '../protocol/server_packets'
import { readProgress, readProfileInfo } from '../protocol/server_packets'
import { decompressBlock } from '../compression/lz4'

export type ServerPacket =
  | { type: ServerPacketType.Hello; data: ServerHello }
  | {
      type:
        | ServerPacketType.Data
        | ServerPacketType.Totals
        | ServerPacketType.Extremes
        | ServerPacketType.ProfileEvents
      data: { tempTable: string; header: BlockHeader; rawReader: BinaryReader }
    }
  | { type: ServerPacketType.Exception; data: ServerException }
  | { type: ServerPacketType.Progress; data: ProgressPacket }
  | { type: ServerPacketType.Pong }
  | { type: ServerPacketType.EndOfStream }
  | { type: ServerPacketType.ProfileInfo; data: ProfileInfoPacket }
  | {
      type: ServerPacketType.TableColumns
      data: { tempTable: string; columns: string }
    }

/**
 * Accumulates data from the socket and decodes server packets.
 * Note: This is a simplified synchronous parser that works on complete buffers.
 * For production streaming use, you'd add incremental buffering.
 */
const MAX_BUFFER_SIZE = 268_435_456 // 256MB — prevent unbounded buffer growth
const INITIAL_BUF_SIZE = 65_536 // 64KB — matches typical TCP chunk size

export class PacketReader {
  private buf: Buffer = Buffer.allocUnsafe(INITIAL_BUF_SIZE)
  private readPos = 0
  private writePos = 0
  private _decompress = false

  set decompress(value: boolean) {
    this._decompress = value
  }

  feed(data: Buffer): void {
    const needed = this.writePos + data.length
    if (needed - this.readPos > MAX_BUFFER_SIZE) {
      throw new Error(
        `PacketReader buffer size ${needed - this.readPos} exceeds maximum ${MAX_BUFFER_SIZE}`,
      )
    }
    if (needed > this.buf.length) {
      // Try compacting first (shift unread data to front)
      if (this.readPos > 0) {
        this.buf.copyWithin(0, this.readPos, this.writePos)
        this.writePos -= this.readPos
        this.readPos = 0
      }
      // If still not enough, grow the buffer
      if (this.writePos + data.length > this.buf.length) {
        let newSize = this.buf.length * 2
        while (newSize < this.writePos + data.length) newSize *= 2
        const next = Buffer.allocUnsafe(newSize)
        this.buf.copy(next, 0, 0, this.writePos)
        this.buf = next
      }
    }
    data.copy(this.buf, this.writePos)
    this.writePos += data.length
  }

  get bufferedLength(): number {
    return this.writePos - this.readPos
  }

  private clearBuffer(): void {
    this.readPos = 0
    this.writePos = 0
  }

  /**
   * Try to read a single packet from the accumulated buffer.
   * Returns null if not enough data yet.
   */
  tryReadPacket(): ServerPacket | null {
    if (this.writePos === this.readPos) return null

    // Snapshot the active buffer region once
    const buffer = this.buf.subarray(this.readPos, this.writePos)
    const reader = new BinaryReader(buffer)

    try {
      const packetType = reader.readVarUInt() as ServerPacketType

      switch (packetType) {
        case ServerPacketType.Hello: {
          const data = readServerHello(reader)
          this.consume(reader.offset)
          return { type: ServerPacketType.Hello, data }
        }

        case ServerPacketType.Exception: {
          const data = readServerException(reader)
          this.consume(reader.offset)
          return { type: ServerPacketType.Exception, data }
        }

        case ServerPacketType.Progress: {
          const data = readProgress(reader)
          this.consume(reader.offset)
          return { type: ServerPacketType.Progress, data }
        }

        case ServerPacketType.Pong: {
          this.consume(reader.offset)
          return { type: ServerPacketType.Pong }
        }

        case ServerPacketType.EndOfStream: {
          this.consume(reader.offset)
          return { type: ServerPacketType.EndOfStream }
        }

        case ServerPacketType.ProfileInfo: {
          const data = readProfileInfo(reader)
          this.consume(reader.offset)
          return { type: ServerPacketType.ProfileInfo, data }
        }

        case ServerPacketType.Data:
        case ServerPacketType.Totals:
        case ServerPacketType.Extremes:
        case ServerPacketType.ProfileEvents: {
          const tempTable = reader.readString()

          if (this._decompress) {
            // Compressed block: checksum(16) + header(9) + compressed_data
            // The entire block header + column data is inside the compressed payload
            const compressedBuf = buffer.subarray(reader.offset)
            if (compressedBuf.length < 25) {
              // Need at least checksum(16) + method(1) + sizes(8)
              return null
            }
            const compressedSize = compressedBuf.readUInt32LE(17) // offset 16+1
            if (compressedSize > Number.MAX_SAFE_INTEGER - 16) {
              throw new Error(
                `Compressed block size ${compressedSize} would overflow`,
              )
            }
            const totalBlockSize = 16 + compressedSize
            if (compressedBuf.length < totalBlockSize) {
              return null // not enough data yet
            }
            const { data: decompressed, bytesRead } = decompressBlock(
              compressedBuf,
              0,
            )
            const afterBlock = Buffer.from(compressedBuf.subarray(bytesRead))
            this.clearBuffer()

            const blockReader = new BinaryReader(decompressed)
            const header = readBlockHeader(blockReader)
            // Remaining decompressed data = column data
            const columnData = Buffer.from(
              decompressed.subarray(blockReader.offset),
            )
            // Combine column data remainder with afterBlock
            const combined = Buffer.concat([columnData, afterBlock])
            return {
              type: packetType,
              data: {
                tempTable,
                header,
                rawReader: new BinaryReader(combined),
              },
            }
          }

          const header = readBlockHeader(reader)
          // Copy remaining bytes into a new buffer for the rawReader.
          // The caller reads column data from rawReader, then feeds back
          // any unconsumed bytes via consumeFromReader/feed.
          const remaining = Buffer.from(buffer.subarray(reader.offset))
          // Consume entire buffer — leftover will be fed back by the caller.
          this.clearBuffer()
          return {
            type: packetType,
            data: {
              tempTable,
              header,
              rawReader: new BinaryReader(remaining),
            },
          }
        }

        case ServerPacketType.TableColumns: {
          const tempTable = reader.readString()
          const columns = reader.readString()
          this.consume(reader.offset)
          return {
            type: ServerPacketType.TableColumns,
            data: { tempTable, columns },
          }
        }

        case ServerPacketType.Log: {
          // Log packets contain a data block with log entries
          const tempTable = reader.readString()

          if (this._decompress) {
            const compressedBuf = buffer.subarray(reader.offset)
            if (compressedBuf.length < 25) return null
            const compressedSize = compressedBuf.readUInt32LE(17)
            if (compressedSize > Number.MAX_SAFE_INTEGER - 16) {
              throw new Error(
                `Compressed block size ${compressedSize} would overflow`,
              )
            }
            const totalBlockSize = 16 + compressedSize
            if (compressedBuf.length < totalBlockSize) return null
            const { data: decompressed, bytesRead } = decompressBlock(
              compressedBuf,
              0,
            )
            const afterBlock = Buffer.from(compressedBuf.subarray(bytesRead))
            this.clearBuffer()
            const blockReader = new BinaryReader(decompressed)
            const header = readBlockHeader(blockReader)
            const columnData = Buffer.from(
              decompressed.subarray(blockReader.offset),
            )
            const combined = Buffer.concat([columnData, afterBlock])
            return {
              type: ServerPacketType.ProfileEvents,
              data: {
                tempTable,
                header,
                rawReader: new BinaryReader(combined),
              },
            }
          }

          const header = readBlockHeader(reader)
          const remaining = Buffer.from(buffer.subarray(reader.offset))
          this.clearBuffer()
          return {
            type: ServerPacketType.ProfileEvents,
            data: {
              tempTable,
              header,
              rawReader: new BinaryReader(remaining),
            },
          }
        }

        default:
          throw new Error(`Unknown server packet type: ${packetType}`)
      }
    } catch (e) {
      if (e instanceof Error && e.message.startsWith('Not enough data')) {
        // Not enough data accumulated yet — wait for more
        return null
      }
      throw e
    }
  }

  private consume(bytes: number): void {
    this.readPos += bytes
    if (this.readPos === this.writePos) {
      // Buffer fully consumed — reset positions
      this.readPos = 0
      this.writePos = 0
    }
  }

  /** Take the accumulated buffer and reset it. */
  drainBuffer(): Buffer {
    const result = Buffer.from(this.buf.subarray(this.readPos, this.writePos))
    this.readPos = 0
    this.writePos = 0
    return result
  }

  reset(): void {
    this.readPos = 0
    this.writePos = 0
  }
}

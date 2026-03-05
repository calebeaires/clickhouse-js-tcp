import { BinaryReader } from '../protocol/binary_reader'
import { ServerPacketType } from '../protocol/packet_types'
import {
  readServerHello,
  readServerException,
  ServerHello,
  ServerException,
} from '../protocol/handshake'
import { readBlockHeader, BlockHeader } from '../protocol/data_packet'
import {
  readProgress,
  readProfileInfo,
  ProgressPacket,
  ProfileInfoPacket,
} from '../protocol/server_packets'
import { decompressBlock } from '../compression/lz4'

export type ServerPacket =
  | { type: ServerPacketType.Hello; data: ServerHello }
  | {
      type: ServerPacketType.Data | ServerPacketType.Totals | ServerPacketType.Extremes | ServerPacketType.ProfileEvents
      data: { tempTable: string; header: BlockHeader; rawReader: BinaryReader }
    }
  | { type: ServerPacketType.Exception; data: ServerException }
  | { type: ServerPacketType.Progress; data: ProgressPacket }
  | { type: ServerPacketType.Pong }
  | { type: ServerPacketType.EndOfStream }
  | { type: ServerPacketType.ProfileInfo; data: ProfileInfoPacket }
  | { type: ServerPacketType.TableColumns; data: { tempTable: string; columns: string } }

/**
 * Accumulates data from the socket and decodes server packets.
 * Note: This is a simplified synchronous parser that works on complete buffers.
 * For production streaming use, you'd add incremental buffering.
 */
export class PacketReader {
  private buffer: Buffer = Buffer.alloc(0)
  private _decompress = false

  set decompress(value: boolean) {
    this._decompress = value
  }

  feed(data: Buffer): void {
    this.buffer = Buffer.concat([this.buffer, data])
  }

  get bufferedLength(): number {
    return this.buffer.length
  }

  /**
   * Try to read a single packet from the accumulated buffer.
   * Returns null if not enough data yet.
   */
  tryReadPacket(): ServerPacket | null {
    if (this.buffer.length === 0) return null

    const reader = new BinaryReader(this.buffer)

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
            const compressedBuf = this.buffer.subarray(reader.offset)
            if (compressedBuf.length < 25) {
              // Need at least checksum(16) + method(1) + sizes(8)
              return null
            }
            const compressedSize = compressedBuf.readUInt32LE(17) // offset 16+1
            const totalBlockSize = 16 + compressedSize
            if (compressedBuf.length < totalBlockSize) {
              return null // not enough data yet
            }
            const { data: decompressed, bytesRead } = decompressBlock(compressedBuf, 0)
            const afterBlock = Buffer.from(compressedBuf.subarray(bytesRead))
            this.buffer = Buffer.alloc(0)

            const blockReader = new BinaryReader(decompressed)
            const header = readBlockHeader(blockReader)
            // Remaining decompressed data = column data
            const columnData = Buffer.from(decompressed.subarray(blockReader.offset))
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
          const remaining = Buffer.from(this.buffer.subarray(reader.offset))
          // Consume entire buffer — leftover will be fed back by the caller.
          this.buffer = Buffer.alloc(0)
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
            const compressedBuf = this.buffer.subarray(reader.offset)
            if (compressedBuf.length < 25) return null
            const compressedSize = compressedBuf.readUInt32LE(17)
            const totalBlockSize = 16 + compressedSize
            if (compressedBuf.length < totalBlockSize) return null
            const { data: decompressed, bytesRead } = decompressBlock(compressedBuf, 0)
            const afterBlock = Buffer.from(compressedBuf.subarray(bytesRead))
            this.buffer = Buffer.alloc(0)
            const blockReader = new BinaryReader(decompressed)
            const header = readBlockHeader(blockReader)
            const columnData = Buffer.from(decompressed.subarray(blockReader.offset))
            const combined = Buffer.concat([columnData, afterBlock])
            return {
              type: ServerPacketType.ProfileEvents,
              data: { tempTable, header, rawReader: new BinaryReader(combined) },
            }
          }

          const header = readBlockHeader(reader)
          const remaining = Buffer.from(this.buffer.subarray(reader.offset))
          this.buffer = Buffer.alloc(0)
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
      if (
        e instanceof Error &&
        e.message.startsWith('Not enough data')
      ) {
        // Not enough data accumulated yet — wait for more
        return null
      }
      throw e
    }
  }

  private consume(bytes: number): void {
    this.buffer = this.buffer.subarray(bytes)
  }

  /** Take the accumulated buffer and reset it. */
  drainBuffer(): Buffer {
    const buf = this.buffer
    this.buffer = Buffer.alloc(0)
    return buf
  }

  reset(): void {
    this.buffer = Buffer.alloc(0)
  }
}

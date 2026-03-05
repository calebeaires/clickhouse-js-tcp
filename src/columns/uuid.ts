import type { ColumnCodec } from './column'
import type { BinaryReader } from '../protocol/binary_reader'
import type { BinaryWriter } from '../protocol/binary_writer'

/**
 * UUID in ClickHouse native protocol:
 * Stored as two UInt64 LE values, but in a specific byte order:
 *   - First 8 bytes: high part of UUID (bytes 8-15 of the UUID)
 *   - Second 8 bytes: low part of UUID (bytes 0-7 of the UUID)
 * Both are stored in little-endian.
 */
export class UuidCodec implements ColumnCodec {
  read(reader: BinaryReader, rows: number): string[] {
    const result = new Array<string>(rows)
    for (let i = 0; i < rows; i++) {
      // Read high then low (ClickHouse stores high first)
      const hi = reader.readUInt64()
      const lo = reader.readUInt64()

      result[i] = bigintToUuid(hi, lo)
    }
    return result
  }

  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) {
      const [hi, lo] = uuidToBigint(v as string)
      writer.writeUInt64(hi)
      writer.writeUInt64(lo)
    }
  }
}

function bigintToUuid(hi: bigint, lo: bigint): string {
  const hiHex = hi.toString(16).padStart(16, '0')
  const loHex = lo.toString(16).padStart(16, '0')

  // UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
  // hi = first 16 hex chars, lo = last 16 hex chars
  return [
    hiHex.slice(0, 8),
    hiHex.slice(8, 12),
    hiHex.slice(12, 16),
    loHex.slice(0, 4),
    loHex.slice(4, 16),
  ].join('-')
}

function uuidToBigint(uuid: string): [bigint, bigint] {
  const hex = uuid.replace(/-/g, '')
  const hi = BigInt('0x' + hex.slice(0, 16))
  const lo = BigInt('0x' + hex.slice(16, 32))
  return [hi, lo]
}

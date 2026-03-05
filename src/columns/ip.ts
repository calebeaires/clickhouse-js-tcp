import type { ColumnCodec } from './column'
import type { BinaryReader } from '../protocol/binary_reader'
import type { BinaryWriter } from '../protocol/binary_writer'

/** IPv4 — stored as UInt32 */
export class IPv4Codec implements ColumnCodec {
  read(reader: BinaryReader, rows: number): string[] {
    const result = new Array<string>(rows)
    for (let i = 0; i < rows; i++) {
      const val = reader.readUInt32()
      result[i] = [
        (val >>> 24) & 0xff,
        (val >>> 16) & 0xff,
        (val >>> 8) & 0xff,
        val & 0xff,
      ].join('.')
    }
    return result
  }

  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) {
      const parts = (v as string).split('.').map(Number)
      const val =
        ((parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3]) >>> 0
      writer.writeUInt32(val)
    }
  }
}

/** IPv6 — stored as FixedString(16) in network byte order */
export class IPv6Codec implements ColumnCodec {
  read(reader: BinaryReader, rows: number): string[] {
    const result = new Array<string>(rows)
    for (let i = 0; i < rows; i++) {
      const bytes = reader.readFixedString(16)
      result[i] = formatIPv6(bytes)
    }
    return result
  }

  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) {
      const buf = parseIPv6(v as string)
      writer.writeFixedString(buf, 16)
    }
  }
}

function formatIPv6(bytes: Buffer): string {
  const groups: string[] = []
  for (let i = 0; i < 16; i += 2) {
    groups.push(((bytes[i] << 8) | bytes[i + 1]).toString(16))
  }

  // Check for IPv4-mapped IPv6
  if (
    groups[0] === '0' &&
    groups[1] === '0' &&
    groups[2] === '0' &&
    groups[3] === '0' &&
    groups[4] === '0' &&
    groups[5] === 'ffff'
  ) {
    return `::ffff:${bytes[12]}.${bytes[13]}.${bytes[14]}.${bytes[15]}`
  }

  // Compress consecutive zero groups
  let result = groups.join(':')
  result = result.replace(/(^|:)0(:0)+(:|$)/, '::')
  return result
}

function parseIPv6(str: string): Buffer {
  const buf = Buffer.alloc(16)

  // Handle IPv4-mapped
  const v4Match = str.match(/::ffff:(\d+\.\d+\.\d+\.\d+)$/)
  if (v4Match) {
    buf[10] = 0xff
    buf[11] = 0xff
    const parts = v4Match[1].split('.').map(Number)
    buf[12] = parts[0]
    buf[13] = parts[1]
    buf[14] = parts[2]
    buf[15] = parts[3]
    return buf
  }

  // Expand :: shorthand
  let expanded = str
  if (str.includes('::')) {
    const [left, right] = str.split('::')
    const leftParts = left ? left.split(':') : []
    const rightParts = right ? right.split(':') : []
    const missing = 8 - leftParts.length - rightParts.length
    const middle = new Array(missing).fill('0')
    expanded = [...leftParts, ...(middle as string[]), ...rightParts].join(':')
  }

  const groups = expanded.split(':')
  for (let i = 0; i < 8; i++) {
    const val = parseInt(groups[i] || '0', 16)
    buf[i * 2] = (val >> 8) & 0xff
    buf[i * 2 + 1] = val & 0xff
  }

  return buf
}

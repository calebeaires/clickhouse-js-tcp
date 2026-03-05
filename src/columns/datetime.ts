import type { ColumnCodec } from './column'
import type { BinaryReader } from '../protocol/binary_reader'
import type { BinaryWriter } from '../protocol/binary_writer'

/** Date — UInt16 days since 1970-01-01 */
export class DateCodec implements ColumnCodec {
  read(reader: BinaryReader, rows: number): string[] {
    const result = new Array<string>(rows)
    for (let i = 0; i < rows; i++) {
      const days = reader.readUInt16()
      const date = new Date(days * 86400000)
      result[i] = date.toISOString().slice(0, 10)
    }
    return result
  }
  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) {
      const d = typeof v === 'string' ? new Date(v) : (v as Date)
      const days = Math.floor(d.getTime() / 86400000)
      writer.writeUInt16(days)
    }
  }
}

/** Date32 — Int32 days since 1970-01-01 (supports wider range) */
export class Date32Codec implements ColumnCodec {
  read(reader: BinaryReader, rows: number): string[] {
    const result = new Array<string>(rows)
    for (let i = 0; i < rows; i++) {
      const days = reader.readInt32()
      const date = new Date(days * 86400000)
      result[i] = date.toISOString().slice(0, 10)
    }
    return result
  }
  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) {
      const d = typeof v === 'string' ? new Date(v) : (v as Date)
      const days = Math.floor(d.getTime() / 86400000)
      writer.writeInt32(days)
    }
  }
}

/** DateTime — UInt32 epoch seconds */
export class DateTimeCodec implements ColumnCodec {
  readonly timezone?: string
  constructor(timezone?: string) {
    this.timezone = timezone
  }

  read(reader: BinaryReader, rows: number): string[] {
    const result = new Array<string>(rows)
    for (let i = 0; i < rows; i++) {
      const epoch = reader.readUInt32()
      const date = new Date(epoch * 1000)
      result[i] = date.toISOString().replace('T', ' ').slice(0, 19)
    }
    return result
  }
  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) {
      let epoch: number
      if (typeof v === 'string') {
        epoch = Math.floor(new Date(v).getTime() / 1000)
      } else if (v instanceof Date) {
        epoch = Math.floor(v.getTime() / 1000)
      } else {
        epoch = v as number
      }
      writer.writeUInt32(epoch)
    }
  }
}

/** DateTime64(precision) — Int64 with sub-second precision */
export class DateTime64Codec implements ColumnCodec {
  private readonly scale: bigint

  readonly precision: number
  readonly timezone?: string
  constructor(precision: number, timezone?: string) {
    this.precision = precision
    this.timezone = timezone
    this.scale = 10n ** BigInt(precision)
  }

  read(reader: BinaryReader, rows: number): string[] {
    const result = new Array<string>(rows)
    for (let i = 0; i < rows; i++) {
      const ticks = reader.readInt64()
      const ms = Number((ticks * 1000n) / this.scale)
      const date = new Date(ms)
      result[i] = date.toISOString().replace('T', ' ').replace('Z', '')
    }
    return result
  }

  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) {
      let ms: number
      if (typeof v === 'string') {
        ms = new Date(v).getTime()
      } else if (v instanceof Date) {
        ms = v.getTime()
      } else {
        ms = v as number
      }
      const ticks = (BigInt(ms) * this.scale) / 1000n
      writer.writeInt64(ticks)
    }
  }
}

import type { ColumnCodec } from './column'
import type { BinaryReader } from '../protocol/binary_reader'
import type { BinaryWriter } from '../protocol/binary_writer'

export class Enum8Codec implements ColumnCodec {
  private readonly valueToName: Map<number, string>
  private readonly nameToValue: Map<string, number>

  constructor(entries: Array<[string, number]>) {
    this.valueToName = new Map(entries.map(([name, val]) => [val, name]))
    this.nameToValue = new Map(entries)
  }

  read(reader: BinaryReader, rows: number): string[] {
    const result = new Array<string>(rows)
    for (let i = 0; i < rows; i++) {
      const val = reader.readInt8()
      result[i] = this.valueToName.get(val) ?? String(val)
    }
    return result
  }

  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) {
      if (typeof v === 'number') {
        writer.writeInt8(v)
      } else {
        const num = this.nameToValue.get(v as string)
        if (num === undefined)
          throw new Error(`Unknown enum value: ${String(v)}`)
        writer.writeInt8(num)
      }
    }
  }
}

export class Enum16Codec implements ColumnCodec {
  private readonly valueToName: Map<number, string>
  private readonly nameToValue: Map<string, number>

  constructor(entries: Array<[string, number]>) {
    this.valueToName = new Map(entries.map(([name, val]) => [val, name]))
    this.nameToValue = new Map(entries)
  }

  read(reader: BinaryReader, rows: number): string[] {
    const result = new Array<string>(rows)
    for (let i = 0; i < rows; i++) {
      const val = reader.readInt16()
      result[i] = this.valueToName.get(val) ?? String(val)
    }
    return result
  }

  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) {
      if (typeof v === 'number') {
        writer.writeInt16(v)
      } else {
        const num = this.nameToValue.get(v as string)
        if (num === undefined)
          throw new Error(`Unknown enum value: ${String(v)}`)
        writer.writeInt16(num)
      }
    }
  }
}

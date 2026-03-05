import type { ColumnCodec } from './column'
import type { BinaryReader } from '../protocol/binary_reader'
import type { BinaryWriter } from '../protocol/binary_writer'

export class StringCodec implements ColumnCodec {
  read(reader: BinaryReader, rows: number): string[] {
    const result = new Array<string>(rows)
    for (let i = 0; i < rows; i++) result[i] = reader.readString()
    return result
  }
  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) writer.writeString(v as string)
  }
}

export class FixedStringCodec implements ColumnCodec {
  constructor(private readonly length: number) {}

  read(reader: BinaryReader, rows: number): string[] {
    const result = new Array<string>(rows)
    for (let i = 0; i < rows; i++) {
      const buf = reader.readFixedString(this.length)
      // Trim trailing null bytes for display
      let end = this.length
      while (end > 0 && buf[end - 1] === 0) end--
      result[i] = buf.toString('utf-8', 0, end)
    }
    return result
  }

  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) {
      const buf = Buffer.from(v as string, 'utf-8')
      writer.writeFixedString(buf, this.length)
    }
  }
}

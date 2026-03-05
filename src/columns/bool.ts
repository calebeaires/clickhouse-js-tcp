import type { ColumnCodec } from './column'
import type { BinaryReader } from '../protocol/binary_reader'
import type { BinaryWriter } from '../protocol/binary_writer'

export class BoolCodec implements ColumnCodec {
  read(reader: BinaryReader, rows: number): boolean[] {
    const result = new Array<boolean>(rows)
    for (let i = 0; i < rows; i++) result[i] = reader.readUInt8() !== 0
    return result
  }
  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) writer.writeUInt8(v ? 1 : 0)
  }
}

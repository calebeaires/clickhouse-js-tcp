import type { ColumnCodec } from './column'
import type { BinaryReader } from '../protocol/binary_reader'
import type { BinaryWriter } from '../protocol/binary_writer'

export class Float32Codec implements ColumnCodec {
  read(reader: BinaryReader, rows: number): number[] {
    const result = new Array<number>(rows)
    for (let i = 0; i < rows; i++) result[i] = reader.readFloat32()
    return result
  }
  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) writer.writeFloat32(v as number)
  }
}

export class Float64Codec implements ColumnCodec {
  read(reader: BinaryReader, rows: number): number[] {
    const result = new Array<number>(rows)
    for (let i = 0; i < rows; i++) result[i] = reader.readFloat64()
    return result
  }
  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) writer.writeFloat64(v as number)
  }
}

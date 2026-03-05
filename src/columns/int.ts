import type { ColumnCodec } from './column'
import type { BinaryReader } from '../protocol/binary_reader'
import type { BinaryWriter } from '../protocol/binary_writer'

export class UInt8Codec implements ColumnCodec {
  read(reader: BinaryReader, rows: number): number[] {
    const result = new Array<number>(rows)
    for (let i = 0; i < rows; i++) result[i] = reader.readUInt8()
    return result
  }
  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) writer.writeUInt8(v as number)
  }
}

export class UInt16Codec implements ColumnCodec {
  read(reader: BinaryReader, rows: number): number[] {
    const result = new Array<number>(rows)
    for (let i = 0; i < rows; i++) result[i] = reader.readUInt16()
    return result
  }
  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) writer.writeUInt16(v as number)
  }
}

export class UInt32Codec implements ColumnCodec {
  read(reader: BinaryReader, rows: number): number[] {
    const result = new Array<number>(rows)
    for (let i = 0; i < rows; i++) result[i] = reader.readUInt32()
    return result
  }
  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) writer.writeUInt32(v as number)
  }
}

export class UInt64Codec implements ColumnCodec {
  read(reader: BinaryReader, rows: number): bigint[] {
    const result = new Array<bigint>(rows)
    for (let i = 0; i < rows; i++) result[i] = reader.readUInt64()
    return result
  }
  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) writer.writeUInt64(BigInt(v as bigint))
  }
}

export class Int8Codec implements ColumnCodec {
  read(reader: BinaryReader, rows: number): number[] {
    const result = new Array<number>(rows)
    for (let i = 0; i < rows; i++) result[i] = reader.readInt8()
    return result
  }
  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) writer.writeInt8(v as number)
  }
}

export class Int16Codec implements ColumnCodec {
  read(reader: BinaryReader, rows: number): number[] {
    const result = new Array<number>(rows)
    for (let i = 0; i < rows; i++) result[i] = reader.readInt16()
    return result
  }
  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) writer.writeInt16(v as number)
  }
}

export class Int32Codec implements ColumnCodec {
  read(reader: BinaryReader, rows: number): number[] {
    const result = new Array<number>(rows)
    for (let i = 0; i < rows; i++) result[i] = reader.readInt32()
    return result
  }
  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) writer.writeInt32(v as number)
  }
}

export class Int64Codec implements ColumnCodec {
  read(reader: BinaryReader, rows: number): bigint[] {
    const result = new Array<bigint>(rows)
    for (let i = 0; i < rows; i++) result[i] = reader.readInt64()
    return result
  }
  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) writer.writeInt64(BigInt(v as bigint))
  }
}

export class Int128Codec implements ColumnCodec {
  read(reader: BinaryReader, rows: number): bigint[] {
    const result = new Array<bigint>(rows)
    for (let i = 0; i < rows; i++) result[i] = reader.readInt128()
    return result
  }
  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) writer.writeInt128(BigInt(v as bigint))
  }
}

export class UInt128Codec implements ColumnCodec {
  read(reader: BinaryReader, rows: number): bigint[] {
    const result = new Array<bigint>(rows)
    for (let i = 0; i < rows; i++) result[i] = reader.readUInt128()
    return result
  }
  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) writer.writeInt128(BigInt(v as bigint))
  }
}

export class Int256Codec implements ColumnCodec {
  read(reader: BinaryReader, rows: number): bigint[] {
    const result = new Array<bigint>(rows)
    for (let i = 0; i < rows; i++) result[i] = reader.readInt256()
    return result
  }
  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) writer.writeInt256(BigInt(v as bigint))
  }
}

export class UInt256Codec implements ColumnCodec {
  read(reader: BinaryReader, rows: number): bigint[] {
    const result = new Array<bigint>(rows)
    for (let i = 0; i < rows; i++) result[i] = reader.readUInt256()
    return result
  }
  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) writer.writeInt256(BigInt(v as bigint))
  }
}

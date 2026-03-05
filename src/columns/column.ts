import type { BinaryReader } from '../protocol/binary_reader'
import type { BinaryWriter } from '../protocol/binary_writer'

export interface ColumnReader {
  read(reader: BinaryReader, rows: number): unknown[]
}

export interface ColumnWriter {
  write(writer: BinaryWriter, values: unknown[]): void
}

export interface ColumnCodec extends ColumnReader, ColumnWriter {}

export interface ParsedType {
  name: string
  params: string[]
  innerTypes: ParsedType[]
}

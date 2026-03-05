import type { ColumnCodec } from './column'
import type { BinaryReader } from '../protocol/binary_reader'
import type { BinaryWriter } from '../protocol/binary_writer'

export class NothingCodec implements ColumnCodec {
  read(_reader: BinaryReader, rows: number): null[] {
    const result = new Array<null>(rows)
    for (let i = 0; i < rows; i++) result[i] = null
    return result
  }
  write(_writer: BinaryWriter, _values: unknown[]): void {
    // Nothing type has no data
  }
}

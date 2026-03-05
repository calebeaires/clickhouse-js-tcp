import type { ColumnCodec } from './column'
import type { BinaryReader } from '../protocol/binary_reader'
import type { BinaryWriter } from '../protocol/binary_writer'

export class TupleCodec implements ColumnCodec {
  constructor(private readonly elements: ColumnCodec[]) {}

  read(reader: BinaryReader, rows: number): unknown[][] {
    // Read each element column sequentially
    const columnData: unknown[][] = []
    for (const codec of this.elements) {
      columnData.push(codec.read(reader, rows))
    }

    // Transpose: columns → rows
    const result = new Array<unknown[]>(rows)
    for (let i = 0; i < rows; i++) {
      result[i] = this.elements.map((_, j) => columnData[j][i])
    }
    return result
  }

  write(writer: BinaryWriter, values: unknown[]): void {
    const tuples = values as unknown[][]

    // Transpose: rows → columns, then write each element column
    for (let j = 0; j < this.elements.length; j++) {
      const colValues = tuples.map((t) => t[j])
      this.elements[j].write(writer, colValues)
    }
  }
}

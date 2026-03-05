import type { ColumnCodec } from './column'
import type { BinaryReader } from '../protocol/binary_reader'
import type { BinaryWriter } from '../protocol/binary_writer'

const MAX_ARRAY_ELEMENTS = 100_000_000 // 100M — prevent OOM from malicious offsets

export class ArrayCodec implements ColumnCodec {
  constructor(private readonly inner: ColumnCodec) {}

  read(reader: BinaryReader, rows: number): unknown[][] {
    // Read offsets (UInt64 per row)
    const offsets = new Array<number>(rows)
    for (let i = 0; i < rows; i++) {
      const raw = reader.readUInt64()
      if (raw > BigInt(Number.MAX_SAFE_INTEGER)) {
        throw new Error(`Array offset ${raw} exceeds Number.MAX_SAFE_INTEGER`)
      }
      offsets[i] = Number(raw)
    }

    // Total elements = last offset (or 0)
    const totalElements = rows > 0 ? offsets[rows - 1] : 0
    if (totalElements > MAX_ARRAY_ELEMENTS) {
      throw new Error(
        `Array total elements ${totalElements} exceeds maximum ${MAX_ARRAY_ELEMENTS}`,
      )
    }

    // Read all element data
    const allData = this.inner.read(reader, totalElements)

    // Reconstruct arrays from offsets
    const result = new Array<unknown[]>(rows)
    let prev = 0
    for (let i = 0; i < rows; i++) {
      result[i] = allData.slice(prev, offsets[i])
      prev = offsets[i]
    }
    return result
  }

  write(writer: BinaryWriter, values: unknown[]): void {
    const arrays = values as unknown[][]

    // Write offsets
    let offset = 0
    for (const arr of arrays) {
      offset += arr.length
      writer.writeUInt64(BigInt(offset))
    }

    // Write all elements flat
    const flat: unknown[] = []
    for (const arr of arrays) {
      for (const v of arr) flat.push(v)
    }
    this.inner.write(writer, flat)
  }
}

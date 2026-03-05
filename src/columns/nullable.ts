import type { ColumnCodec } from './column'
import type { BinaryReader } from '../protocol/binary_reader'
import type { BinaryWriter } from '../protocol/binary_writer'

export class NullableCodec implements ColumnCodec {
  constructor(private readonly inner: ColumnCodec) {}

  read(reader: BinaryReader, rows: number): (unknown | null)[] {
    // Read nulls mask (UInt8 per row)
    const nulls = new Array<boolean>(rows)
    for (let i = 0; i < rows; i++) nulls[i] = reader.readUInt8() !== 0

    // Read inner values
    const values = this.inner.read(reader, rows)

    // Apply nulls
    const result = new Array<unknown | null>(rows)
    for (let i = 0; i < rows; i++) {
      result[i] = nulls[i] ? null : values[i]
    }
    return result
  }

  write(writer: BinaryWriter, values: unknown[]): void {
    // Write nulls mask
    for (const v of values) writer.writeUInt8(v === null || v === undefined ? 1 : 0)

    // Write inner values — for null slots, we need to read first non-null value
    // to determine the type-appropriate default, or use the first value's type
    const firstNonNull = values.find((v) => v !== null && v !== undefined)
    const defaultVal = getDefaultForValue(firstNonNull)
    const innerValues = values.map((v) =>
      v === null || v === undefined ? defaultVal : v,
    )
    this.inner.write(writer, innerValues)
  }
}

function getDefaultForValue(sample: unknown): unknown {
  if (typeof sample === 'string') return ''
  if (typeof sample === 'bigint') return 0n
  if (typeof sample === 'boolean') return false
  return 0
}

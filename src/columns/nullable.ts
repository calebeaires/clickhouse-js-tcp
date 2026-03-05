import type { ColumnCodec } from './column'
import type { BinaryReader } from '../protocol/binary_reader'
import type { BinaryWriter } from '../protocol/binary_writer'

export class NullableCodec implements ColumnCodec {
  constructor(private readonly inner: ColumnCodec) {}

  read(reader: BinaryReader, rows: number): unknown[] {
    // Read nulls mask (UInt8 per row)
    const nulls = new Array<boolean>(rows)
    for (let i = 0; i < rows; i++) nulls[i] = reader.readUInt8() !== 0

    // Read inner values
    const values = this.inner.read(reader, rows)

    // Apply nulls
    const result = new Array<unknown>(rows)
    for (let i = 0; i < rows; i++) {
      result[i] = nulls[i] ? null : values[i]
    }
    return result
  }

  write(writer: BinaryWriter, values: unknown[]): void {
    const innerValues = new Array(values.length)
    let defaultVal: unknown = 0
    // Find a non-null sample for the default value
    for (let i = 0; i < values.length; i++) {
      if (values[i] !== null && values[i] !== undefined) {
        defaultVal = getDefaultForValue(values[i])
        break
      }
    }
    // Single pass: write null mask and build inner values
    for (let i = 0; i < values.length; i++) {
      const isNull = values[i] === null || values[i] === undefined
      writer.writeUInt8(isNull ? 1 : 0)
      innerValues[i] = isNull ? defaultVal : values[i]
    }
    this.inner.write(writer, innerValues)
  }
}

function getDefaultForValue(sample: unknown): unknown {
  if (typeof sample === 'string') return ''
  if (typeof sample === 'bigint') return 0n
  if (typeof sample === 'boolean') return false
  return 0
}

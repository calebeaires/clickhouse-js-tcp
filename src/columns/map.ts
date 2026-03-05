import type { ColumnCodec } from './column'
import type { BinaryReader } from '../protocol/binary_reader'
import type { BinaryWriter } from '../protocol/binary_writer'

export class MapCodec implements ColumnCodec {
  constructor(
    private readonly keyCodec: ColumnCodec,
    private readonly valueCodec: ColumnCodec,
  ) {}

  read(reader: BinaryReader, rows: number): Record<string, unknown>[] {
    // Read offsets (UInt64 per row)
    const offsets = new Array<number>(rows)
    for (let i = 0; i < rows; i++) offsets[i] = Number(reader.readUInt64())

    const totalElements = rows > 0 ? offsets[rows - 1] : 0

    // Read keys and values flat
    const allKeys = this.keyCodec.read(reader, totalElements)
    const allValues = this.valueCodec.read(reader, totalElements)

    // Reconstruct maps
    const result = new Array<Record<string, unknown>>(rows)
    let prev = 0
    for (let i = 0; i < rows; i++) {
      const map: Record<string, unknown> = {}
      for (let j = prev; j < offsets[i]; j++) {
        map[String(allKeys[j])] = allValues[j]
      }
      result[i] = map
      prev = offsets[i]
    }
    return result
  }

  write(writer: BinaryWriter, values: unknown[]): void {
    const maps = values as Record<string, unknown>[]

    // Compute offsets and collect keys/values flat
    const allKeys: unknown[] = []
    const allValues: unknown[] = []
    let offset = 0

    for (const map of maps) {
      const entries = Object.entries(map)
      offset += entries.length
      writer.writeUInt64(BigInt(offset))
      for (const [k, v] of entries) {
        allKeys.push(k)
        allValues.push(v)
      }
    }

    this.keyCodec.write(writer, allKeys)
    this.valueCodec.write(writer, allValues)
  }
}

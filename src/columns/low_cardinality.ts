import type { ColumnCodec } from './column'
import type { BinaryReader } from '../protocol/binary_reader'
import type { BinaryWriter } from '../protocol/binary_writer'

/**
 * LowCardinality serialization flags
 */
const HasAdditionalKeysBit = 1 << 9
// NeedGlobalDictionaryBit = 1 << 8 (reserved for future use)
const NeedUpdateDictionary = 1 << 10

// Index type sizes
const KeyUInt8 = 0
const KeyUInt16 = 1
const KeyUInt32 = 2
const KeyUInt64 = 3

export class LowCardinalityCodec implements ColumnCodec {
  constructor(private readonly inner: ColumnCodec) {}

  read(reader: BinaryReader, rows: number): unknown[] {
    if (rows === 0) return []

    // Read version (SharedDictionariesWithAdditionalKeys = 1)
    reader.readInt64() // version (SharedDictionariesWithAdditionalKeys = 1)

    // Read serialization type (Int64)
    const serializationType = Number(reader.readInt64())

    // Read index (dictionary) size
    const indexSize = Number(reader.readInt64())

    // Read index values (the dictionary)
    const index = this.inner.read(reader, indexSize)

    // Read number of keys
    const numKeys = Number(reader.readInt64())

    // Determine key size from serialization type
    const keyType = serializationType & 0xf

    // Read keys (indices into the dictionary)
    const keys = new Array<number>(numKeys)
    for (let i = 0; i < numKeys; i++) {
      switch (keyType) {
        case KeyUInt8:
          keys[i] = reader.readUInt8()
          break
        case KeyUInt16:
          keys[i] = reader.readUInt16()
          break
        case KeyUInt32:
          keys[i] = reader.readUInt32()
          break
        case KeyUInt64:
          keys[i] = Number(reader.readUInt64())
          break
        default:
          throw new Error(`Unknown LowCardinality key type: ${keyType}`)
      }
    }

    // Reconstruct values from dictionary
    const result = new Array<unknown>(rows)
    for (let i = 0; i < rows; i++) {
      result[i] = index[keys[i]]
    }
    return result
  }

  write(writer: BinaryWriter, values: unknown[]): void {
    if (values.length === 0) return

    // Version: SharedDictionariesWithAdditionalKeys = 1
    writer.writeInt64(1n)

    // Build dictionary
    const seen = new Map<string, number>()
    const index: unknown[] = []
    const keys: number[] = []

    for (const v of values) {
      const key = String(v)
      let idx = seen.get(key)
      if (idx === undefined) {
        idx = index.length
        seen.set(key, idx)
        index.push(v)
      }
      keys.push(idx)
    }

    // Determine key type based on dictionary size
    let keyType: number
    if (index.length <= 256) keyType = KeyUInt8
    else if (index.length <= 65536) keyType = KeyUInt16
    else if (index.length <= 4294967296) keyType = KeyUInt32
    else keyType = KeyUInt64

    const serializationType = keyType | HasAdditionalKeysBit | NeedUpdateDictionary

    // Write serialization type
    writer.writeInt64(BigInt(serializationType))

    // Write index (dictionary)
    writer.writeInt64(BigInt(index.length))
    this.inner.write(writer, index)

    // Write keys
    writer.writeInt64(BigInt(keys.length))
    for (const k of keys) {
      switch (keyType) {
        case KeyUInt8:
          writer.writeUInt8(k)
          break
        case KeyUInt16:
          writer.writeUInt16(k)
          break
        case KeyUInt32:
          writer.writeUInt32(k)
          break
        case KeyUInt64:
          writer.writeUInt64(BigInt(k))
          break
      }
    }
  }
}

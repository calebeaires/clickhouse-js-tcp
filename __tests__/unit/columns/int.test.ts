import { describe, it, expect } from 'vitest'
import { BinaryWriter } from '../../../src/protocol/binary_writer'
import { BinaryReader } from '../../../src/protocol/binary_reader'
import {
  UInt8Codec, UInt16Codec, UInt32Codec, UInt64Codec,
  Int8Codec, Int16Codec, Int32Codec, Int64Codec,
  Int128Codec, UInt128Codec, Int256Codec, UInt256Codec,
} from '../../../src/columns/int'

describe('Integer Column Codecs', () => {
  function roundtrip<T>(codec: { read: (r: BinaryReader, n: number) => T[]; write: (w: BinaryWriter, v: unknown[]) => void }, values: T[]): T[] {
    const w = new BinaryWriter()
    codec.write(w, values as unknown[])
    const r = new BinaryReader(w.getBuffer())
    return codec.read(r, values.length)
  }

  it('UInt8 roundtrip', () => {
    const codec = new UInt8Codec()
    expect(roundtrip(codec, [0, 127, 255])).toEqual([0, 127, 255])
  })

  it('UInt16 roundtrip', () => {
    const codec = new UInt16Codec()
    expect(roundtrip(codec, [0, 1000, 65535])).toEqual([0, 1000, 65535])
  })

  it('UInt32 roundtrip', () => {
    const codec = new UInt32Codec()
    expect(roundtrip(codec, [0, 123456, 4294967295])).toEqual([0, 123456, 4294967295])
  })

  it('UInt64 roundtrip', () => {
    const codec = new UInt64Codec()
    expect(roundtrip(codec, [0n, 123456789012345n, 18446744073709551615n]))
      .toEqual([0n, 123456789012345n, 18446744073709551615n])
  })

  it('Int8 roundtrip', () => {
    const codec = new Int8Codec()
    expect(roundtrip(codec, [-128, 0, 127])).toEqual([-128, 0, 127])
  })

  it('Int16 roundtrip', () => {
    const codec = new Int16Codec()
    expect(roundtrip(codec, [-32768, 0, 32767])).toEqual([-32768, 0, 32767])
  })

  it('Int32 roundtrip', () => {
    const codec = new Int32Codec()
    expect(roundtrip(codec, [-2147483648, 0, 2147483647])).toEqual([-2147483648, 0, 2147483647])
  })

  it('Int64 roundtrip', () => {
    const codec = new Int64Codec()
    expect(roundtrip(codec, [-9223372036854775808n, 0n, 9223372036854775807n]))
      .toEqual([-9223372036854775808n, 0n, 9223372036854775807n])
  })

  it('Int128 roundtrip', () => {
    const codec = new Int128Codec()
    const values = [0n, 123456789012345678901234567890n, -123456789012345678901234567890n]
    expect(roundtrip(codec, values)).toEqual(values)
  })

  it('UInt128 roundtrip', () => {
    const codec = new UInt128Codec()
    const values = [0n, 123456789012345678901234567890n]
    expect(roundtrip(codec, values)).toEqual(values)
  })

  it('Int256 roundtrip', () => {
    const codec = new Int256Codec()
    const values = [0n, 12345678901234567890123456789012345678901234567890n]
    expect(roundtrip(codec, values)).toEqual(values)
  })

  it('UInt256 roundtrip', () => {
    const codec = new UInt256Codec()
    const values = [0n, 12345678901234567890123456789012345678901234567890n]
    expect(roundtrip(codec, values)).toEqual(values)
  })
})

import { describe, it, expect } from 'vitest'
import { BinaryWriter } from '../../../src/protocol/binary_writer'
import { BinaryReader } from '../../../src/protocol/binary_reader'
import { LowCardinalityCodec } from '../../../src/columns/low_cardinality'
import { StringCodec } from '../../../src/columns/string'

describe('LowCardinality Column Codec', () => {
  it('LowCardinality(String) roundtrip', () => {
    const codec = new LowCardinalityCodec(new StringCodec())
    const values = ['a', 'b', 'a', 'c', 'b', 'a']
    const w = new BinaryWriter()
    codec.write(w, values)
    const r = new BinaryReader(w.getBuffer())
    const result = codec.read(r, values.length)
    expect(result).toEqual(values)
  })

  it('LowCardinality(String) with many unique values', () => {
    const codec = new LowCardinalityCodec(new StringCodec())
    const values = Array.from({ length: 300 }, (_, i) => `value_${i}`)
    const w = new BinaryWriter()
    codec.write(w, values)
    const r = new BinaryReader(w.getBuffer())
    const result = codec.read(r, values.length)
    expect(result).toEqual(values)
  })

  it('empty LowCardinality', () => {
    const codec = new LowCardinalityCodec(new StringCodec())
    const w = new BinaryWriter()
    codec.write(w, [])
    // No data should be written for empty
    expect(w.length).toBe(0)
  })
})

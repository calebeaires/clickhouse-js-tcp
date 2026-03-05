import { describe, it, expect } from 'vitest'
import { BinaryWriter } from '../../../src/protocol/binary_writer'
import { BinaryReader } from '../../../src/protocol/binary_reader'
import { NullableCodec } from '../../../src/columns/nullable'
import { UInt32Codec } from '../../../src/columns/int'
import { StringCodec } from '../../../src/columns/string'

describe('Nullable Column Codec', () => {
  it('Nullable(UInt32) roundtrip', () => {
    const codec = new NullableCodec(new UInt32Codec())
    const values = [42, null, 100, null, 0]
    const w = new BinaryWriter()
    codec.write(w, values)
    const r = new BinaryReader(w.getBuffer())
    const result = codec.read(r, values.length)
    expect(result).toEqual([42, null, 100, null, 0])
  })

  it('Nullable(String) roundtrip', () => {
    const codec = new NullableCodec(new StringCodec())
    const values = ['hello', null, 'world']
    const w = new BinaryWriter()
    codec.write(w, values)
    const r = new BinaryReader(w.getBuffer())
    const result = codec.read(r, values.length)
    expect(result[0]).toBe('hello')
    expect(result[1]).toBe(null)
    expect(result[2]).toBe('world')
  })

  it('all nulls', () => {
    const codec = new NullableCodec(new UInt32Codec())
    const values = [null, null, null]
    const w = new BinaryWriter()
    codec.write(w, values)
    const r = new BinaryReader(w.getBuffer())
    const result = codec.read(r, values.length)
    expect(result).toEqual([null, null, null])
  })

  it('no nulls', () => {
    const codec = new NullableCodec(new UInt32Codec())
    const values = [1, 2, 3]
    const w = new BinaryWriter()
    codec.write(w, values)
    const r = new BinaryReader(w.getBuffer())
    const result = codec.read(r, values.length)
    expect(result).toEqual([1, 2, 3])
  })
})

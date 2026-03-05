import { describe, it, expect } from 'vitest'
import { BinaryWriter } from '../../../src/protocol/binary_writer'
import { BinaryReader } from '../../../src/protocol/binary_reader'
import { ArrayCodec } from '../../../src/columns/array'
import { UInt32Codec } from '../../../src/columns/int'
import { StringCodec } from '../../../src/columns/string'

describe('Array Column Codec', () => {
  it('Array(UInt32) roundtrip', () => {
    const codec = new ArrayCodec(new UInt32Codec())
    const values = [[1, 2, 3], [], [42], [10, 20]]
    const w = new BinaryWriter()
    codec.write(w, values)
    const r = new BinaryReader(w.getBuffer())
    const result = codec.read(r, values.length)
    expect(result).toEqual([[1, 2, 3], [], [42], [10, 20]])
  })

  it('Array(String) roundtrip', () => {
    const codec = new ArrayCodec(new StringCodec())
    const values = [['a', 'b'], ['hello', 'world', '!'], []]
    const w = new BinaryWriter()
    codec.write(w, values)
    const r = new BinaryReader(w.getBuffer())
    const result = codec.read(r, values.length)
    expect(result).toEqual([['a', 'b'], ['hello', 'world', '!'], []])
  })

  it('empty arrays', () => {
    const codec = new ArrayCodec(new UInt32Codec())
    const values = [[], [], []]
    const w = new BinaryWriter()
    codec.write(w, values)
    const r = new BinaryReader(w.getBuffer())
    const result = codec.read(r, values.length)
    expect(result).toEqual([[], [], []])
  })
})

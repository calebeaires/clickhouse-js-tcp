import { describe, it, expect } from 'vitest'
import { BinaryWriter } from '../../../src/protocol/binary_writer'
import { BinaryReader } from '../../../src/protocol/binary_reader'
import { MapCodec } from '../../../src/columns/map'
import { StringCodec } from '../../../src/columns/string'
import { UInt32Codec } from '../../../src/columns/int'

describe('Map Column Codec', () => {
  it('Map(String, UInt32) roundtrip', () => {
    const codec = new MapCodec(new StringCodec(), new UInt32Codec())
    const values = [{ a: 1, b: 2 }, { x: 42 }, {}]
    const w = new BinaryWriter()
    codec.write(w, values)
    const r = new BinaryReader(w.getBuffer())
    const result = codec.read(r, values.length)
    expect(result[0]).toEqual({ a: 1, b: 2 })
    expect(result[1]).toEqual({ x: 42 })
    expect(result[2]).toEqual({})
  })
})

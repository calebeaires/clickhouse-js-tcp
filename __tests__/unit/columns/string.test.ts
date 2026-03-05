import { describe, it, expect } from 'vitest'
import { BinaryWriter } from '../../../src/protocol/binary_writer'
import { BinaryReader } from '../../../src/protocol/binary_reader'
import { StringCodec, FixedStringCodec } from '../../../src/columns/string'

describe('String Column Codecs', () => {
  it('String roundtrip', () => {
    const codec = new StringCodec()
    const values = [
      '',
      'hello',
      'unicode: \u00e9\u00e8\u00ea \u{1f600}',
      'a'.repeat(1000),
    ]
    const w = new BinaryWriter()
    codec.write(w, values)
    const r = new BinaryReader(w.getBuffer())
    expect(codec.read(r, values.length)).toEqual(values)
  })

  it('FixedString roundtrip', () => {
    const codec = new FixedStringCodec(5)
    const values = ['abc', 'hello', 'xy']
    const w = new BinaryWriter()
    codec.write(w, values)
    const r = new BinaryReader(w.getBuffer())
    const result = codec.read(r, values.length)
    expect(result[0]).toBe('abc')
    expect(result[1]).toBe('hello')
    expect(result[2]).toBe('xy')
  })

  it('FixedString pads with nulls', () => {
    const codec = new FixedStringCodec(10)
    const w = new BinaryWriter()
    codec.write(w, ['hi'])
    const r = new BinaryReader(w.getBuffer())
    const result = codec.read(r, 1)
    expect(result[0]).toBe('hi')
  })
})

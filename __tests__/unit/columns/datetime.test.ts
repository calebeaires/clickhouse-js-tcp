import { describe, it, expect } from 'vitest'
import { BinaryWriter } from '../../../src/protocol/binary_writer'
import { BinaryReader } from '../../../src/protocol/binary_reader'
import { DateCodec, Date32Codec, DateTimeCodec, DateTime64Codec } from '../../../src/columns/datetime'

describe('DateTime Column Codecs', () => {
  it('Date roundtrip', () => {
    const codec = new DateCodec()
    const w = new BinaryWriter()
    codec.write(w, ['1970-01-01', '2024-01-15'])
    const r = new BinaryReader(w.getBuffer())
    const result = codec.read(r, 2)
    expect(result[0]).toBe('1970-01-01')
    expect(result[1]).toBe('2024-01-15')
  })

  it('Date32 roundtrip', () => {
    const codec = new Date32Codec()
    const w = new BinaryWriter()
    codec.write(w, ['2024-06-15'])
    const r = new BinaryReader(w.getBuffer())
    const result = codec.read(r, 1)
    expect(result[0]).toBe('2024-06-15')
  })

  it('DateTime roundtrip', () => {
    const codec = new DateTimeCodec()
    const w = new BinaryWriter()
    // Write epoch seconds directly
    codec.write(w, [0, 1704067200]) // 2024-01-01 00:00:00 UTC
    const r = new BinaryReader(w.getBuffer())
    const result = codec.read(r, 2)
    expect(result[0]).toBe('1970-01-01 00:00:00')
    expect(result[1]).toBe('2024-01-01 00:00:00')
  })

  it('DateTime64 roundtrip', () => {
    const codec = new DateTime64Codec(3)
    const w = new BinaryWriter()
    codec.write(w, ['2024-01-01T00:00:00.123Z'])
    const r = new BinaryReader(w.getBuffer())
    const result = codec.read(r, 1)
    expect(result[0]).toContain('2024-01-01')
  })
})

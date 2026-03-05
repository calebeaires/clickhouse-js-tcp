import { describe, it, expect } from 'vitest'
import { BinaryWriter } from '../../../src/protocol/binary_writer'
import { BinaryReader } from '../../../src/protocol/binary_reader'

describe('BinaryWriter', () => {
  it('should write and read UInt8', () => {
    const w = new BinaryWriter()
    w.writeUInt8(0)
    w.writeUInt8(127)
    w.writeUInt8(255)
    const r = new BinaryReader(w.getBuffer())
    expect(r.readUInt8()).toBe(0)
    expect(r.readUInt8()).toBe(127)
    expect(r.readUInt8()).toBe(255)
  })

  it('should write and read UInt16', () => {
    const w = new BinaryWriter()
    w.writeUInt16(0)
    w.writeUInt16(65535)
    w.writeUInt16(12345)
    const r = new BinaryReader(w.getBuffer())
    expect(r.readUInt16()).toBe(0)
    expect(r.readUInt16()).toBe(65535)
    expect(r.readUInt16()).toBe(12345)
  })

  it('should write and read UInt32', () => {
    const w = new BinaryWriter()
    w.writeUInt32(0)
    w.writeUInt32(4294967295)
    w.writeUInt32(123456789)
    const r = new BinaryReader(w.getBuffer())
    expect(r.readUInt32()).toBe(0)
    expect(r.readUInt32()).toBe(4294967295)
    expect(r.readUInt32()).toBe(123456789)
  })

  it('should write and read UInt64', () => {
    const w = new BinaryWriter()
    w.writeUInt64(0n)
    w.writeUInt64(18446744073709551615n)
    w.writeUInt64(123456789012345n)
    const r = new BinaryReader(w.getBuffer())
    expect(r.readUInt64()).toBe(0n)
    expect(r.readUInt64()).toBe(18446744073709551615n)
    expect(r.readUInt64()).toBe(123456789012345n)
  })

  it('should write and read Int8', () => {
    const w = new BinaryWriter()
    w.writeInt8(-128)
    w.writeInt8(0)
    w.writeInt8(127)
    const r = new BinaryReader(w.getBuffer())
    expect(r.readInt8()).toBe(-128)
    expect(r.readInt8()).toBe(0)
    expect(r.readInt8()).toBe(127)
  })

  it('should write and read Int16', () => {
    const w = new BinaryWriter()
    w.writeInt16(-32768)
    w.writeInt16(0)
    w.writeInt16(32767)
    const r = new BinaryReader(w.getBuffer())
    expect(r.readInt16()).toBe(-32768)
    expect(r.readInt16()).toBe(0)
    expect(r.readInt16()).toBe(32767)
  })

  it('should write and read Int32', () => {
    const w = new BinaryWriter()
    w.writeInt32(-2147483648)
    w.writeInt32(0)
    w.writeInt32(2147483647)
    const r = new BinaryReader(w.getBuffer())
    expect(r.readInt32()).toBe(-2147483648)
    expect(r.readInt32()).toBe(0)
    expect(r.readInt32()).toBe(2147483647)
  })

  it('should write and read Int64', () => {
    const w = new BinaryWriter()
    w.writeInt64(-9223372036854775808n)
    w.writeInt64(0n)
    w.writeInt64(9223372036854775807n)
    const r = new BinaryReader(w.getBuffer())
    expect(r.readInt64()).toBe(-9223372036854775808n)
    expect(r.readInt64()).toBe(0n)
    expect(r.readInt64()).toBe(9223372036854775807n)
  })

  it('should write and read Int128', () => {
    const w = new BinaryWriter()
    w.writeInt128(0n)
    w.writeInt128(123456789012345678901234567890n)
    w.writeInt128(-123456789012345678901234567890n)
    const r = new BinaryReader(w.getBuffer())
    expect(r.readInt128()).toBe(0n)
    expect(r.readInt128()).toBe(123456789012345678901234567890n)
    expect(r.readInt128()).toBe(-123456789012345678901234567890n)
  })

  it('should write and read Int256', () => {
    const w = new BinaryWriter()
    w.writeInt256(0n)
    w.writeInt256(12345678901234567890123456789012345678901234567890n)
    w.writeInt256(-12345678901234567890123456789012345678901234567890n)
    const r = new BinaryReader(w.getBuffer())
    expect(r.readInt256()).toBe(0n)
    expect(r.readInt256()).toBe(12345678901234567890123456789012345678901234567890n)
    expect(r.readInt256()).toBe(-12345678901234567890123456789012345678901234567890n)
  })

  it('should write and read Float32', () => {
    const w = new BinaryWriter()
    w.writeFloat32(0)
    w.writeFloat32(3.14)
    w.writeFloat32(-1.5)
    const r = new BinaryReader(w.getBuffer())
    expect(r.readFloat32()).toBeCloseTo(0, 5)
    expect(r.readFloat32()).toBeCloseTo(3.14, 2)
    expect(r.readFloat32()).toBeCloseTo(-1.5, 5)
  })

  it('should write and read Float64', () => {
    const w = new BinaryWriter()
    w.writeFloat64(0)
    w.writeFloat64(3.141592653589793)
    w.writeFloat64(-1e100)
    const r = new BinaryReader(w.getBuffer())
    expect(r.readFloat64()).toBe(0)
    expect(r.readFloat64()).toBe(3.141592653589793)
    expect(r.readFloat64()).toBe(-1e100)
  })

  it('should write and read Bool', () => {
    const w = new BinaryWriter()
    w.writeBool(true)
    w.writeBool(false)
    const r = new BinaryReader(w.getBuffer())
    expect(r.readBool()).toBe(true)
    expect(r.readBool()).toBe(false)
  })

  it('should write and read VarUInt', () => {
    const w = new BinaryWriter()
    w.writeVarUInt(0)
    w.writeVarUInt(1)
    w.writeVarUInt(127)
    w.writeVarUInt(128)
    w.writeVarUInt(16383)
    w.writeVarUInt(16384)
    w.writeVarUInt(2097151)
    w.writeVarUInt(4294967295) // max uint32
    const r = new BinaryReader(w.getBuffer())
    expect(r.readVarUInt()).toBe(0)
    expect(r.readVarUInt()).toBe(1)
    expect(r.readVarUInt()).toBe(127)
    expect(r.readVarUInt()).toBe(128)
    expect(r.readVarUInt()).toBe(16383)
    expect(r.readVarUInt()).toBe(16384)
    expect(r.readVarUInt()).toBe(2097151)
    expect(r.readVarUInt()).toBe(4294967295)
  })

  it('should write and read VarUInt with bigint', () => {
    const w = new BinaryWriter()
    w.writeVarUInt(0n)
    w.writeVarUInt(128n)
    w.writeVarUInt(9999999999n)
    const r = new BinaryReader(w.getBuffer())
    expect(r.readVarUIntBig()).toBe(0n)
    expect(r.readVarUIntBig()).toBe(128n)
    expect(r.readVarUIntBig()).toBe(9999999999n)
  })

  it('should write and read String', () => {
    const w = new BinaryWriter()
    w.writeString('')
    w.writeString('hello')
    w.writeString('unicode: \u00e9\u00e8\u00ea \u{1f600}')
    const r = new BinaryReader(w.getBuffer())
    expect(r.readString()).toBe('')
    expect(r.readString()).toBe('hello')
    expect(r.readString()).toBe('unicode: \u00e9\u00e8\u00ea \u{1f600}')
  })

  it('should write and read FixedString', () => {
    const w = new BinaryWriter()
    const data = Buffer.from('abc')
    w.writeFixedString(data, 5)
    const r = new BinaryReader(w.getBuffer())
    const result = r.readFixedString(5)
    expect(result.length).toBe(5)
    expect(result[0]).toBe(97) // 'a'
    expect(result[1]).toBe(98) // 'b'
    expect(result[2]).toBe(99) // 'c'
    expect(result[3]).toBe(0)
    expect(result[4]).toBe(0)
  })

  it('should expand buffer when needed', () => {
    const w = new BinaryWriter(4) // tiny initial capacity
    for (let i = 0; i < 1000; i++) {
      w.writeUInt32(i)
    }
    const r = new BinaryReader(w.getBuffer())
    for (let i = 0; i < 1000; i++) {
      expect(r.readUInt32()).toBe(i)
    }
  })

  it('should reset and reuse', () => {
    const w = new BinaryWriter()
    w.writeUInt32(42)
    expect(w.length).toBe(4)
    w.reset()
    expect(w.length).toBe(0)
    w.writeUInt8(1)
    expect(w.length).toBe(1)
    const r = new BinaryReader(w.getBuffer())
    expect(r.readUInt8()).toBe(1)
  })
})

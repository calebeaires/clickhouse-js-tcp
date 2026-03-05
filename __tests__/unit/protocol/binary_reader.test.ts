import { describe, it, expect } from 'vitest'
import { BinaryReader } from '../../../src/protocol/binary_reader'

describe('BinaryReader', () => {
  it('should throw when not enough data', () => {
    const r = new BinaryReader(Buffer.alloc(2))
    expect(() => r.readUInt32()).toThrow('Not enough data')
  })

  it('should report remaining bytes', () => {
    const r = new BinaryReader(Buffer.alloc(10))
    expect(r.remaining()).toBe(10)
    r.readUInt32()
    expect(r.remaining()).toBe(6)
  })

  it('should track offset', () => {
    const r = new BinaryReader(Buffer.alloc(10))
    expect(r.offset).toBe(0)
    r.readUInt8()
    expect(r.offset).toBe(1)
    r.readUInt32()
    expect(r.offset).toBe(5)
  })

  it('should skip bytes', () => {
    const buf = Buffer.from([1, 2, 3, 4, 5])
    const r = new BinaryReader(buf)
    r.skip(3)
    expect(r.readUInt8()).toBe(4)
  })

  it('should throw on skip past end', () => {
    const r = new BinaryReader(Buffer.alloc(2))
    expect(() => r.skip(3)).toThrow('Not enough data')
  })

  it('should read UInt128', () => {
    const buf = Buffer.alloc(16)
    // Write 1 as UInt128 LE (lo=1, hi=0)
    buf.writeBigUInt64LE(1n, 0)
    buf.writeBigUInt64LE(0n, 8)
    const r = new BinaryReader(buf)
    expect(r.readUInt128()).toBe(1n)
  })

  it('should read UInt256', () => {
    const buf = Buffer.alloc(32)
    buf.writeBigUInt64LE(42n, 0)
    buf.writeBigUInt64LE(0n, 8)
    buf.writeBigUInt64LE(0n, 16)
    buf.writeBigUInt64LE(0n, 24)
    const r = new BinaryReader(buf)
    expect(r.readUInt256()).toBe(42n)
  })

  it('should read VarUInt encoded bytes correctly', () => {
    // VarUInt encoding of 300: 0xAC 0x02
    const buf = Buffer.from([0xac, 0x02])
    const r = new BinaryReader(buf)
    expect(r.readVarUInt()).toBe(300)
  })

  it('should throw on VarUInt too long', () => {
    // 11 bytes all with continuation bit set
    const buf = Buffer.alloc(11, 0x80)
    const r = new BinaryReader(buf)
    expect(() => r.readVarUInt()).toThrow('VarUInt too long')
  })

  it('should read raw bytes', () => {
    const buf = Buffer.from([10, 20, 30, 40, 50])
    const r = new BinaryReader(buf)
    const raw = r.readRawBytes(3)
    expect(raw).toEqual(Buffer.from([10, 20, 30]))
    expect(r.remaining()).toBe(2)
  })

  it('should start from offset', () => {
    const buf = Buffer.from([0, 0, 42, 0, 0, 0])
    const r = new BinaryReader(buf, 2)
    expect(r.readUInt32()).toBe(42)
  })
})

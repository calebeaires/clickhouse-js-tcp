export class BinaryReader {
  private buf: Buffer
  private pos: number

  constructor(buf: Buffer, offset = 0) {
    this.buf = buf
    this.pos = offset
  }

  get offset(): number {
    return this.pos
  }

  remaining(): number {
    return this.buf.length - this.pos
  }

  readUInt8(): number {
    this.assertAvailable(1)
    return this.buf[this.pos++]
  }

  readUInt16(): number {
    this.assertAvailable(2)
    const v = this.buf.readUInt16LE(this.pos)
    this.pos += 2
    return v
  }

  readUInt32(): number {
    this.assertAvailable(4)
    const v = this.buf.readUInt32LE(this.pos)
    this.pos += 4
    return v
  }

  readUInt64(): bigint {
    this.assertAvailable(8)
    const v = this.buf.readBigUInt64LE(this.pos)
    this.pos += 8
    return v
  }

  readInt8(): number {
    this.assertAvailable(1)
    const v = this.buf.readInt8(this.pos)
    this.pos += 1
    return v
  }

  readInt16(): number {
    this.assertAvailable(2)
    const v = this.buf.readInt16LE(this.pos)
    this.pos += 2
    return v
  }

  readInt32(): number {
    this.assertAvailable(4)
    const v = this.buf.readInt32LE(this.pos)
    this.pos += 4
    return v
  }

  readInt64(): bigint {
    this.assertAvailable(8)
    const v = this.buf.readBigInt64LE(this.pos)
    this.pos += 8
    return v
  }

  readInt128(): bigint {
    this.assertAvailable(16)
    const lo = this.buf.readBigUInt64LE(this.pos)
    const hi = this.buf.readBigInt64LE(this.pos + 8)
    this.pos += 16
    return (hi << 64n) | lo
  }

  readUInt128(): bigint {
    this.assertAvailable(16)
    const lo = this.buf.readBigUInt64LE(this.pos)
    const hi = this.buf.readBigUInt64LE(this.pos + 8)
    this.pos += 16
    return (hi << 64n) | lo
  }

  readInt256(): bigint {
    this.assertAvailable(32)
    let result = 0n
    for (let i = 0; i < 3; i++) {
      const word = this.buf.readBigUInt64LE(this.pos + i * 8)
      result |= word << BigInt(i * 64)
    }
    // Last word is signed
    const hi = this.buf.readBigInt64LE(this.pos + 24)
    result |= hi << 192n
    this.pos += 32
    return result
  }

  readUInt256(): bigint {
    this.assertAvailable(32)
    let result = 0n
    for (let i = 0; i < 4; i++) {
      const word = this.buf.readBigUInt64LE(this.pos + i * 8)
      result |= word << BigInt(i * 64)
    }
    this.pos += 32
    return result
  }

  readFloat32(): number {
    this.assertAvailable(4)
    const v = this.buf.readFloatLE(this.pos)
    this.pos += 4
    return v
  }

  readFloat64(): number {
    this.assertAvailable(8)
    const v = this.buf.readDoubleLE(this.pos)
    this.pos += 8
    return v
  }

  readBool(): boolean {
    return this.readUInt8() !== 0
  }

  readVarUInt(): number {
    let result = 0
    let shift = 0
    for (let i = 0; i < 10; i++) {
      this.assertAvailable(1)
      const byte = this.buf[this.pos++]
      result |= (byte & 0x7f) << shift
      if ((byte & 0x80) === 0) {
        return result >>> 0 // ensure unsigned
      }
      shift += 7
    }
    throw new Error('VarUInt too long')
  }

  readVarUIntBig(): bigint {
    let result = 0n
    let shift = 0n
    for (let i = 0; i < 10; i++) {
      this.assertAvailable(1)
      const byte = this.buf[this.pos++]
      result |= BigInt(byte & 0x7f) << shift
      if ((byte & 0x80) === 0) {
        return result
      }
      shift += 7n
    }
    throw new Error('VarUInt too long')
  }

  readString(): string {
    const len = this.readVarUInt()
    if (len === 0) return ''
    this.assertAvailable(len)
    const s = this.buf.toString('utf-8', this.pos, this.pos + len)
    this.pos += len
    return s
  }

  readFixedString(length: number): Buffer {
    this.assertAvailable(length)
    const result = Buffer.allocUnsafe(length)
    this.buf.copy(result, 0, this.pos, this.pos + length)
    this.pos += length
    return result
  }

  readRawBytes(length: number): Buffer {
    return this.readFixedString(length)
  }

  /** Return a copy of the internal buffer starting from `from` offset. */
  readRawBytesFrom(from: number): Buffer {
    return Buffer.from(this.buf.subarray(from))
  }

  skip(length: number): void {
    this.assertAvailable(length)
    this.pos += length
  }

  private assertAvailable(bytes: number): void {
    if (this.pos + bytes > this.buf.length) {
      throw new Error(
        `Not enough data: need ${bytes} bytes, have ${this.buf.length - this.pos}`,
      )
    }
  }
}

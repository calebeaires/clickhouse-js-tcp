const INITIAL_CAPACITY = 256

export class BinaryWriter {
  private buf: Buffer
  private pos: number

  constructor(capacity = INITIAL_CAPACITY) {
    this.buf = Buffer.allocUnsafe(capacity)
    this.pos = 0
  }

  private ensure(bytes: number): void {
    if (this.pos + bytes > this.buf.length) {
      let newSize = this.buf.length * 2
      while (newSize < this.pos + bytes) {
        newSize *= 2
      }
      const next = Buffer.allocUnsafe(newSize)
      this.buf.copy(next, 0, 0, this.pos)
      this.buf = next
    }
  }

  writeUInt8(value: number): void {
    this.ensure(1)
    this.buf[this.pos++] = value & 0xff
  }

  writeUInt16(value: number): void {
    this.ensure(2)
    this.buf.writeUInt16LE(value, this.pos)
    this.pos += 2
  }

  writeUInt32(value: number): void {
    this.ensure(4)
    this.buf.writeUInt32LE(value, this.pos)
    this.pos += 4
  }

  writeUInt64(value: bigint): void {
    this.ensure(8)
    this.buf.writeBigUInt64LE(value, this.pos)
    this.pos += 8
  }

  writeInt8(value: number): void {
    this.ensure(1)
    this.buf.writeInt8(value, this.pos)
    this.pos += 1
  }

  writeInt16(value: number): void {
    this.ensure(2)
    this.buf.writeInt16LE(value, this.pos)
    this.pos += 2
  }

  writeInt32(value: number): void {
    this.ensure(4)
    this.buf.writeInt32LE(value, this.pos)
    this.pos += 4
  }

  writeInt64(value: bigint): void {
    this.ensure(8)
    this.buf.writeBigInt64LE(value, this.pos)
    this.pos += 8
  }

  writeInt128(value: bigint): void {
    this.ensure(16)
    // Little-endian: write low 8 bytes first, then high 8 bytes
    const mask64 = (1n << 64n) - 1n
    const v = value < 0n ? (1n << 128n) + value : value
    this.buf.writeBigUInt64LE(v & mask64, this.pos)
    this.buf.writeBigUInt64LE((v >> 64n) & mask64, this.pos + 8)
    this.pos += 16
  }

  writeInt256(value: bigint): void {
    this.ensure(32)
    const mask64 = (1n << 64n) - 1n
    let v = value < 0n ? (1n << 256n) + value : value
    for (let i = 0; i < 4; i++) {
      this.buf.writeBigUInt64LE(v & mask64, this.pos + i * 8)
      v >>= 64n
    }
    this.pos += 32
  }

  writeFloat32(value: number): void {
    this.ensure(4)
    this.buf.writeFloatLE(value, this.pos)
    this.pos += 4
  }

  writeFloat64(value: number): void {
    this.ensure(8)
    this.buf.writeDoubleLE(value, this.pos)
    this.pos += 8
  }

  writeBool(value: boolean): void {
    this.writeUInt8(value ? 1 : 0)
  }

  writeVarUInt(value: number | bigint): void {
    if (typeof value === 'number') {
      // Fast path for number values (99%+ of calls)
      this.ensure(5)
      let v = value
      while (v >= 0x80) {
        this.buf[this.pos++] = (v & 0x7f) | 0x80
        v >>>= 7
      }
      this.buf[this.pos++] = v
      return
    }
    // BigInt fallback (rare: Int128/Int256)
    this.ensure(10)
    let bv = value
    while (bv >= 0x80n) {
      this.buf[this.pos++] = Number(bv & 0x7fn) | 0x80
      bv >>= 7n
    }
    this.buf[this.pos++] = Number(bv)
  }

  writeString(value: string): void {
    const encoded = Buffer.from(value, 'utf-8')
    this.writeVarUInt(encoded.length)
    this.writeRawBytes(encoded)
  }

  writeFixedString(value: Buffer, length: number): void {
    this.ensure(length)
    if (value.length >= length) {
      value.copy(this.buf, this.pos, 0, length)
    } else {
      value.copy(this.buf, this.pos, 0, value.length)
      this.buf.fill(0, this.pos + value.length, this.pos + length)
    }
    this.pos += length
  }

  writeRawBytes(data: Buffer): void {
    this.ensure(data.length)
    data.copy(this.buf, this.pos, 0, data.length)
    this.pos += data.length
  }

  getBuffer(): Buffer {
    return this.buf.subarray(0, this.pos)
  }

  get length(): number {
    return this.pos
  }

  reset(): void {
    this.pos = 0
  }
}

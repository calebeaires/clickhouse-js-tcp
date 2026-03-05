import type { ColumnCodec } from './column'
import type { BinaryReader } from '../protocol/binary_reader'
import type { BinaryWriter } from '../protocol/binary_writer'

export class DecimalCodec implements ColumnCodec {
  private readonly scaleFactor: bigint

  constructor(
    private readonly precision: number,
    private readonly scale: number,
  ) {
    this.scaleFactor = 10n ** BigInt(scale)
  }

  private get byteSize(): number {
    if (this.precision <= 9) return 4
    if (this.precision <= 18) return 8
    if (this.precision <= 38) return 16
    return 32
  }

  read(reader: BinaryReader, rows: number): string[] {
    const result = new Array<string>(rows)
    for (let i = 0; i < rows; i++) {
      let raw: bigint
      switch (this.byteSize) {
        case 4:
          raw = BigInt(reader.readInt32())
          break
        case 8:
          raw = reader.readInt64()
          break
        case 16:
          raw = reader.readInt128()
          break
        case 32:
          raw = reader.readInt256()
          break
        default:
          throw new Error(`Unsupported decimal size: ${this.byteSize}`)
      }
      result[i] = this.formatDecimal(raw)
    }
    return result
  }

  write(writer: BinaryWriter, values: unknown[]): void {
    for (const v of values) {
      const raw = this.parseDecimal(v)
      switch (this.byteSize) {
        case 4:
          writer.writeInt32(Number(raw))
          break
        case 8:
          writer.writeInt64(raw)
          break
        case 16:
          writer.writeInt128(raw)
          break
        case 32:
          writer.writeInt256(raw)
          break
      }
    }
  }

  private formatDecimal(raw: bigint): string {
    if (this.scale === 0) return raw.toString()

    const negative = raw < 0n
    const abs = negative ? -raw : raw
    const intPart = abs / this.scaleFactor
    const fracPart = abs % this.scaleFactor

    const fracStr = fracPart.toString().padStart(this.scale, '0')
    const sign = negative ? '-' : ''
    return `${sign}${intPart}.${fracStr}`
  }

  private parseDecimal(v: unknown): bigint {
    if (typeof v === 'bigint') return v
    if (typeof v === 'number') {
      return BigInt(Math.round(v * Number(this.scaleFactor)))
    }
    const str = String(v)
    const dotIdx = str.indexOf('.')
    if (dotIdx === -1) {
      return BigInt(str) * this.scaleFactor
    }

    const intStr = str.slice(0, dotIdx)
    let fracStr = str.slice(dotIdx + 1)

    // Pad or truncate fractional part
    if (fracStr.length > this.scale) {
      fracStr = fracStr.slice(0, this.scale)
    } else {
      fracStr = fracStr.padEnd(this.scale, '0')
    }

    const negative = intStr.startsWith('-')
    const absInt = negative ? intStr.slice(1) : intStr
    const combined = BigInt(absInt) * this.scaleFactor + BigInt(fracStr)
    return negative ? -combined : combined
  }
}

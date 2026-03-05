import type { ColumnCodec, ParsedType } from './column'
import { parseType, parseEnumEntries } from './type_parser'
import {
  UInt8Codec, UInt16Codec, UInt32Codec, UInt64Codec,
  Int8Codec, Int16Codec, Int32Codec, Int64Codec,
  Int128Codec, UInt128Codec, Int256Codec, UInt256Codec,
} from './int'
import { Float32Codec, Float64Codec } from './float'
import { BoolCodec } from './bool'
import { StringCodec, FixedStringCodec } from './string'
import { DateCodec, Date32Codec, DateTimeCodec, DateTime64Codec } from './datetime'
import { Enum8Codec, Enum16Codec } from './enum'
import { NullableCodec } from './nullable'
import { ArrayCodec } from './array'
import { MapCodec } from './map'
import { TupleCodec } from './tuple'
import { LowCardinalityCodec } from './low_cardinality'
import { UuidCodec } from './uuid'
import { IPv4Codec, IPv6Codec } from './ip'
import { DecimalCodec } from './decimal'
import { NothingCodec } from './nothing'

const simpleCodecs: Record<string, () => ColumnCodec> = {
  UInt8: () => new UInt8Codec(),
  UInt16: () => new UInt16Codec(),
  UInt32: () => new UInt32Codec(),
  UInt64: () => new UInt64Codec(),
  UInt128: () => new UInt128Codec(),
  UInt256: () => new UInt256Codec(),
  Int8: () => new Int8Codec(),
  Int16: () => new Int16Codec(),
  Int32: () => new Int32Codec(),
  Int64: () => new Int64Codec(),
  Int128: () => new Int128Codec(),
  Int256: () => new Int256Codec(),
  Float32: () => new Float32Codec(),
  Float64: () => new Float64Codec(),
  Bool: () => new BoolCodec(),
  String: () => new StringCodec(),
  Date: () => new DateCodec(),
  Date32: () => new Date32Codec(),
  UUID: () => new UuidCodec(),
  IPv4: () => new IPv4Codec(),
  IPv6: () => new IPv6Codec(),
  Nothing: () => new NothingCodec(),
}

export function getCodec(typeStr: string): ColumnCodec {
  const parsed = parseType(typeStr)
  return buildCodec(parsed)
}

function buildCodec(parsed: ParsedType): ColumnCodec {
  // Check simple types first
  const simpleFactory = simpleCodecs[parsed.name]
  if (simpleFactory && parsed.params.length === 0 && parsed.innerTypes.length === 0) {
    return simpleFactory()
  }

  switch (parsed.name) {
    case 'FixedString':
      return new FixedStringCodec(parseInt(parsed.params[0], 10))

    case 'DateTime':
      if (parsed.params.length > 0) {
        const tz = parsed.params[0].replace(/'/g, '')
        return new DateTimeCodec(tz)
      }
      return new DateTimeCodec()

    case 'DateTime64': {
      const precision = parseInt(parsed.params[0], 10)
      const tz = parsed.params.length > 1
        ? parsed.params[1].replace(/'/g, '').trim()
        : undefined
      return new DateTime64Codec(precision, tz)
    }

    case 'Decimal': {
      const p = parseInt(parsed.params[0], 10)
      const s = parseInt(parsed.params[1], 10)
      return new DecimalCodec(p, s)
    }

    case 'Decimal32': {
      const s = parseInt(parsed.params[0], 10)
      return new DecimalCodec(9, s)
    }

    case 'Decimal64': {
      const s = parseInt(parsed.params[0], 10)
      return new DecimalCodec(18, s)
    }

    case 'Decimal128': {
      const s = parseInt(parsed.params[0], 10)
      return new DecimalCodec(38, s)
    }

    case 'Decimal256': {
      const s = parseInt(parsed.params[0], 10)
      return new DecimalCodec(76, s)
    }

    case 'Enum8': {
      const entries = parseEnumEntries(parsed.params)
      return new Enum8Codec(entries)
    }

    case 'Enum16': {
      const entries = parseEnumEntries(parsed.params)
      return new Enum16Codec(entries)
    }

    case 'Nullable':
      return new NullableCodec(buildCodec(parsed.innerTypes[0]))

    case 'Array':
      return new ArrayCodec(buildCodec(parsed.innerTypes[0]))

    case 'Map':
      return new MapCodec(
        buildCodec(parsed.innerTypes[0]),
        buildCodec(parsed.innerTypes[1]),
      )

    case 'Tuple':
      return new TupleCodec(parsed.innerTypes.map(buildCodec))

    case 'LowCardinality':
      return new LowCardinalityCodec(buildCodec(parsed.innerTypes[0]))

    default:
      throw new Error(`Unsupported ClickHouse type: ${parsed.name}`)
  }
}

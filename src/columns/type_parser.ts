import type { ParsedType } from './column'

/**
 * Parses ClickHouse type strings like:
 *   "UInt32"
 *   "Nullable(String)"
 *   "Array(Nullable(UInt32))"
 *   "Map(String, UInt64)"
 *   "DateTime64(9, 'UTC')"
 *   "Enum8('a' = 1, 'b' = 2)"
 *   "Decimal(18, 4)"
 *   "FixedString(16)"
 *   "Tuple(UInt32, String, Nullable(Float64))"
 *   "LowCardinality(String)"
 */
export function parseType(typeStr: string): ParsedType {
  const trimmed = typeStr.trim()

  const parenIdx = trimmed.indexOf('(')
  if (parenIdx === -1) {
    return { name: trimmed, params: [], innerTypes: [] }
  }

  const name = trimmed.slice(0, parenIdx)
  const argsStr = trimmed.slice(parenIdx + 1, trimmed.length - 1)

  // For types that have type parameters (Nullable, Array, Map, Tuple, LowCardinality)
  // we need to split by commas respecting parenthesis depth
  const args = splitArgs(argsStr)

  const innerTypes: ParsedType[] = []
  const params: string[] = []

  switch (name) {
    case 'Nullable':
    case 'Array':
    case 'LowCardinality':
      innerTypes.push(parseType(args[0]))
      break

    case 'Map':
      innerTypes.push(parseType(args[0]))
      innerTypes.push(parseType(args[1]))
      break

    case 'Tuple':
      for (const arg of args) {
        innerTypes.push(parseType(arg))
      }
      break

    case 'Enum8':
    case 'Enum16':
      // Params are 'name' = value pairs
      for (const arg of args) {
        params.push(arg.trim())
      }
      break

    case 'DateTime64':
      // First arg is precision, optional second is timezone
      for (const arg of args) {
        params.push(arg.trim())
      }
      break

    case 'DateTime':
      // Optional timezone param
      for (const arg of args) {
        params.push(arg.trim())
      }
      break

    case 'Decimal':
    case 'Decimal32':
    case 'Decimal64':
    case 'Decimal128':
    case 'Decimal256':
      for (const arg of args) {
        params.push(arg.trim())
      }
      break

    case 'FixedString':
      params.push(args[0].trim())
      break

    default:
      // Unknown parameterized type — store raw params
      for (const arg of args) {
        params.push(arg.trim())
      }
      break
  }

  return { name, params, innerTypes }
}

function splitArgs(str: string): string[] {
  const args: string[] = []
  let depth = 0
  let start = 0

  for (let i = 0; i < str.length; i++) {
    const ch = str[i]
    if (ch === '(') {
      depth++
    } else if (ch === ')') {
      depth--
    } else if (ch === ',' && depth === 0) {
      args.push(str.slice(start, i))
      start = i + 1
    }
  }
  args.push(str.slice(start))
  return args
}

export function parseEnumEntries(
  params: string[],
): Array<[string, number]> {
  const entries: Array<[string, number]> = []
  for (const param of params) {
    // Format: 'name' = value
    const match = param.match(/^\s*'([^']+)'\s*=\s*(-?\d+)\s*$/)
    if (match) {
      entries.push([match[1], parseInt(match[2], 10)])
    }
  }
  return entries
}

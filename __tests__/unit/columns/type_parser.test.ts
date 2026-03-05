import { describe, it, expect } from 'vitest'
import { parseType, parseEnumEntries } from '../../../src/columns/type_parser'

describe('Type Parser', () => {
  it('simple types', () => {
    expect(parseType('UInt32')).toEqual({ name: 'UInt32', params: [], innerTypes: [] })
    expect(parseType('String')).toEqual({ name: 'String', params: [], innerTypes: [] })
    expect(parseType('Float64')).toEqual({ name: 'Float64', params: [], innerTypes: [] })
  })

  it('Nullable', () => {
    const parsed = parseType('Nullable(UInt32)')
    expect(parsed.name).toBe('Nullable')
    expect(parsed.innerTypes).toHaveLength(1)
    expect(parsed.innerTypes[0].name).toBe('UInt32')
  })

  it('Array', () => {
    const parsed = parseType('Array(String)')
    expect(parsed.name).toBe('Array')
    expect(parsed.innerTypes[0].name).toBe('String')
  })

  it('Map', () => {
    const parsed = parseType('Map(String, UInt64)')
    expect(parsed.name).toBe('Map')
    expect(parsed.innerTypes).toHaveLength(2)
    expect(parsed.innerTypes[0].name).toBe('String')
    expect(parsed.innerTypes[1].name).toBe('UInt64')
  })

  it('Tuple', () => {
    const parsed = parseType('Tuple(UInt32, String, Float64)')
    expect(parsed.name).toBe('Tuple')
    expect(parsed.innerTypes).toHaveLength(3)
    expect(parsed.innerTypes[0].name).toBe('UInt32')
    expect(parsed.innerTypes[1].name).toBe('String')
    expect(parsed.innerTypes[2].name).toBe('Float64')
  })

  it('nested types', () => {
    const parsed = parseType('Array(Nullable(String))')
    expect(parsed.name).toBe('Array')
    expect(parsed.innerTypes[0].name).toBe('Nullable')
    expect(parsed.innerTypes[0].innerTypes[0].name).toBe('String')
  })

  it('DateTime64 with params', () => {
    const parsed = parseType("DateTime64(9, 'UTC')")
    expect(parsed.name).toBe('DateTime64')
    expect(parsed.params).toEqual(['9', "'UTC'"])
  })

  it('FixedString', () => {
    const parsed = parseType('FixedString(16)')
    expect(parsed.name).toBe('FixedString')
    expect(parsed.params).toEqual(['16'])
  })

  it('Enum8', () => {
    const parsed = parseType("Enum8('a' = 1, 'b' = 2)")
    expect(parsed.name).toBe('Enum8')
    expect(parsed.params).toHaveLength(2)
  })

  it('Decimal', () => {
    const parsed = parseType('Decimal(18, 4)')
    expect(parsed.name).toBe('Decimal')
    expect(parsed.params).toEqual(['18', '4'])
  })

  it('LowCardinality', () => {
    const parsed = parseType('LowCardinality(String)')
    expect(parsed.name).toBe('LowCardinality')
    expect(parsed.innerTypes[0].name).toBe('String')
  })

  it('deeply nested', () => {
    const parsed = parseType('Map(String, Array(Nullable(UInt32)))')
    expect(parsed.name).toBe('Map')
    expect(parsed.innerTypes[0].name).toBe('String')
    expect(parsed.innerTypes[1].name).toBe('Array')
    expect(parsed.innerTypes[1].innerTypes[0].name).toBe('Nullable')
    expect(parsed.innerTypes[1].innerTypes[0].innerTypes[0].name).toBe('UInt32')
  })
})

describe('parseEnumEntries', () => {
  it('should parse enum entries', () => {
    const entries = parseEnumEntries(["'hello' = 1", "'world' = 2"])
    expect(entries).toEqual([['hello', 1], ['world', 2]])
  })

  it('should handle negative values', () => {
    const entries = parseEnumEntries(["'a' = -1", "'b' = 0", "'c' = 1"])
    expect(entries).toEqual([['a', -1], ['b', 0], ['c', 1]])
  })
})

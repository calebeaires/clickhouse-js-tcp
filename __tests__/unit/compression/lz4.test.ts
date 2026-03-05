import { describe, it, expect } from 'vitest'
import { lz4CompressBlock, lz4DecompressBlock, compressBlock, decompressBlock } from '../../../src/compression/lz4'

describe('LZ4', () => {
  it('should compress and decompress empty buffer', () => {
    const input = Buffer.alloc(0)
    const compressed = lz4CompressBlock(input)
    const decompressed = lz4DecompressBlock(compressed, 0)
    expect(decompressed.length).toBe(0)
  })

  it('should compress and decompress small data', () => {
    const input = Buffer.from('Hello, World!')
    const compressed = lz4CompressBlock(input)
    const decompressed = lz4DecompressBlock(compressed, input.length)
    expect(decompressed.toString()).toBe('Hello, World!')
  })

  it('should compress and decompress repetitive data', () => {
    const input = Buffer.from('ABCABCABCABCABCABCABCABCABCABCABCABCABCABCABCABCABCABCABCABC')
    const compressed = lz4CompressBlock(input)
    // Repetitive data should compress
    expect(compressed.length).toBeLessThan(input.length)
    const decompressed = lz4DecompressBlock(compressed, input.length)
    expect(decompressed.toString()).toBe(input.toString())
  })

  it('should compress and decompress large data', () => {
    const size = 10000
    const input = Buffer.alloc(size)
    for (let i = 0; i < size; i++) {
      input[i] = i % 256
    }
    const compressed = lz4CompressBlock(input)
    const decompressed = lz4DecompressBlock(compressed, input.length)
    expect(Buffer.compare(decompressed, input)).toBe(0)
  })

  it('should handle full block with checksum', () => {
    const input = Buffer.from('Hello ClickHouse TCP Protocol! '.repeat(10))
    const block = compressBlock(input)
    // Block should have checksum (16 bytes) + header (9 bytes) + data
    expect(block.length).toBeGreaterThan(25)
    const { data, bytesRead } = decompressBlock(block)
    expect(bytesRead).toBe(block.length)
    expect(data.toString()).toBe(input.toString())
  })

  it('should detect checksum mismatch', () => {
    const input = Buffer.from('test data for checksum')
    const block = compressBlock(input)
    // Corrupt the checksum
    block[0] ^= 0xff
    expect(() => decompressBlock(block)).toThrow('checksum mismatch')
  })
})

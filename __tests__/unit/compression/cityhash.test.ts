import { describe, it, expect } from 'vitest'
import { cityHash128 } from '../../../src/compression/cityhash'

describe('CityHash128', () => {
  it('should return consistent hash for same input', () => {
    const buf = Buffer.from('Hello, World!')
    const [lo1, hi1] = cityHash128(buf)
    const [lo2, hi2] = cityHash128(buf)
    expect(lo1).toBe(lo2)
    expect(hi1).toBe(hi2)
  })

  it('should return different hash for different input', () => {
    const [lo1, hi1] = cityHash128(Buffer.from('Hello'))
    const [lo2, hi2] = cityHash128(Buffer.from('World'))
    expect(lo1 === lo2 && hi1 === hi2).toBe(false)
  })

  it('should hash empty buffer', () => {
    const [lo, hi] = cityHash128(Buffer.alloc(0))
    expect(typeof lo).toBe('bigint')
    expect(typeof hi).toBe('bigint')
  })

  it('should hash various sizes', () => {
    for (const size of [1, 4, 8, 16, 32, 64, 128, 256, 1024]) {
      const buf = Buffer.alloc(size, 0x42)
      const [lo, hi] = cityHash128(buf)
      expect(typeof lo).toBe('bigint')
      expect(typeof hi).toBe('bigint')
    }
  })

  it('should handle offset and length', () => {
    const buf = Buffer.from('XXXX Hello XXXX')
    const [lo1, hi1] = cityHash128(buf, 5, 5)
    const [lo2, hi2] = cityHash128(Buffer.from('Hello'))
    expect(lo1).toBe(lo2)
    expect(hi1).toBe(hi2)
  })
})

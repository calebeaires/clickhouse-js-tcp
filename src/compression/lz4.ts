import { cityHash128 } from './cityhash'
import { CompressionMethod } from '../protocol/packet_types'

const LZ4_HEADER_SIZE = 9 // method(1) + compressed_size(4) + uncompressed_size(4)
const CHECKSUM_SIZE = 16
const MAX_DECOMPRESSED_BLOCK_SIZE = 1_073_741_824 // 1GB — prevent decompression bombs

/**
 * Compress a block of data using LZ4 block compression.
 * Returns the full ClickHouse compressed block including checksum.
 *
 * Format:
 *   checksum (16 bytes, CityHash128 of the rest)
 *   compression_method (1 byte, 0x82 for LZ4)
 *   compressed_size (4 bytes LE, includes header)
 *   uncompressed_size (4 bytes LE)
 *   compressed_data (bytes)
 */
export function compressBlock(data: Buffer): Buffer {
  const compressed = lz4CompressBlock(data)
  const compressedSize = LZ4_HEADER_SIZE + compressed.length
  const uncompressedSize = data.length

  // Build the block (without checksum first)
  const block = Buffer.allocUnsafe(LZ4_HEADER_SIZE + compressed.length)
  block[0] = CompressionMethod.LZ4
  block.writeUInt32LE(compressedSize, 1)
  block.writeUInt32LE(uncompressedSize, 5)
  compressed.copy(block, LZ4_HEADER_SIZE)

  // Compute checksum over the block
  const [lo, hi] = cityHash128(block)

  // Final output: checksum + block
  const result = Buffer.allocUnsafe(CHECKSUM_SIZE + block.length)
  result.writeBigUInt64LE(lo, 0)
  result.writeBigUInt64LE(hi, 8)
  block.copy(result, CHECKSUM_SIZE)

  return result
}

/**
 * Decompress a ClickHouse compressed block.
 * Expects the full block including checksum.
 * Returns the decompressed data.
 */
export function decompressBlock(buf: Buffer, offset = 0): {
  data: Buffer
  bytesRead: number
} {
  // Read checksum
  const checksumLo = buf.readBigUInt64LE(offset)
  const checksumHi = buf.readBigUInt64LE(offset + 8)

  const blockStart = offset + CHECKSUM_SIZE
  const method = buf[blockStart]

  if (method !== CompressionMethod.LZ4) {
    throw new Error(`Unsupported compression method: 0x${method.toString(16)}`)
  }

  const compressedSize = buf.readUInt32LE(blockStart + 1)
  const uncompressedSize = buf.readUInt32LE(blockStart + 5)

  // Verify checksum
  const blockData = buf.subarray(blockStart, blockStart + compressedSize)
  const [lo, hi] = cityHash128(blockData)
  if (lo !== checksumLo || hi !== checksumHi) {
    throw new Error('LZ4 block checksum mismatch')
  }

  // Decompress
  const compressedData = buf.subarray(
    blockStart + LZ4_HEADER_SIZE,
    blockStart + compressedSize,
  )
  const decompressed = lz4DecompressBlock(compressedData, uncompressedSize)

  return {
    data: decompressed,
    bytesRead: CHECKSUM_SIZE + compressedSize,
  }
}

/**
 * Pure JS LZ4 block compression (simplified).
 * Uses a hash table for match finding.
 */
export function lz4CompressBlock(input: Buffer): Buffer {
  const len = input.length
  if (len === 0) return Buffer.alloc(0)

  // Worst case output size
  const maxOut = len + Math.ceil(len / 255) + 16
  const output = Buffer.allocUnsafe(maxOut)
  let op = 0
  let anchor = 0
  let ip = 0

  const hashTable = new Int32Array(1 << 14).fill(-1)

  const hashFn = (pos: number): number => {
    const val =
      input[pos] | (input[pos + 1] << 8) | (input[pos + 2] << 16) | (input[pos + 3] << 24)
    return ((val * 2654435761) >>> 0) >> 18
  }

  if (len < 13) {
    // Too short — store as literal only
    op = writeLiterals(input, 0, len, output, op)
    // End mark
    return output.subarray(0, op)
  }

  while (ip < len - 12) {
    const hash = hashFn(ip)
    const ref = hashTable[hash]
    hashTable[hash] = ip

    // Check for match
    if (
      ref >= 0 &&
      ip - ref <= 65535 &&
      input[ref] === input[ip] &&
      input[ref + 1] === input[ip + 1] &&
      input[ref + 2] === input[ip + 2] &&
      input[ref + 3] === input[ip + 3]
    ) {
      // Write literals before match
      const litLen = ip - anchor
      const offset = ip - ref

      // Find match length
      let matchLen = 4
      while (ip + matchLen < len && input[ref + matchLen] === input[ip + matchLen]) {
        matchLen++
      }

      // Write token
      const tokenLit = Math.min(litLen, 15)
      const tokenMatch = Math.min(matchLen - 4, 15)
      output[op++] = (tokenLit << 4) | tokenMatch

      // Extra literal length
      if (litLen >= 15) {
        let remaining = litLen - 15
        while (remaining >= 255) {
          output[op++] = 255
          remaining -= 255
        }
        output[op++] = remaining
      }

      // Copy literals
      input.copy(output, op, anchor, anchor + litLen)
      op += litLen

      // Write offset (LE 16-bit)
      output[op++] = offset & 0xff
      output[op++] = (offset >> 8) & 0xff

      // Extra match length
      if (matchLen - 4 >= 15) {
        let remaining = matchLen - 4 - 15
        while (remaining >= 255) {
          output[op++] = 255
          remaining -= 255
        }
        output[op++] = remaining
      }

      ip += matchLen
      anchor = ip
    } else {
      ip++
    }
  }

  // Write remaining literals (last 5+ bytes must be literals in LZ4)
  const remaining = len - anchor
  op = writeLiterals(input, anchor, remaining, output, op)

  return output.subarray(0, op)
}

function writeLiterals(
  input: Buffer,
  start: number,
  litLen: number,
  output: Buffer,
  op: number,
): number {
  const tokenLit = Math.min(litLen, 15)
  output[op++] = tokenLit << 4

  if (litLen >= 15) {
    let remaining = litLen - 15
    while (remaining >= 255) {
      output[op++] = 255
      remaining -= 255
    }
    output[op++] = remaining
  }

  input.copy(output, op, start, start + litLen)
  op += litLen
  return op
}

/**
 * Pure JS LZ4 block decompression.
 */
export function lz4DecompressBlock(
  input: Buffer,
  uncompressedSize: number,
): Buffer {
  if (uncompressedSize > MAX_DECOMPRESSED_BLOCK_SIZE) {
    throw new Error(
      `LZ4: decompressed size ${uncompressedSize} exceeds maximum allowed ${MAX_DECOMPRESSED_BLOCK_SIZE}`,
    )
  }
  const output = Buffer.allocUnsafe(uncompressedSize)
  let ip = 0
  let op = 0

  while (ip < input.length) {
    if (ip >= input.length) throw new Error('LZ4: unexpected end of input')
    const token = input[ip++]

    // Literal length
    let litLen = token >> 4
    if (litLen === 15) {
      let b: number
      do {
        if (ip >= input.length) throw new Error('LZ4: unexpected end of input reading literal length')
        b = input[ip++]
        litLen += b
      } while (b === 255)
    }

    // Bounds check before copying literals
    if (ip + litLen > input.length) {
      throw new Error('LZ4: literal length exceeds input bounds')
    }
    if (op + litLen > uncompressedSize) {
      throw new Error('LZ4: literal would overflow output buffer')
    }

    // Copy literals
    input.copy(output, op, ip, ip + litLen)
    ip += litLen
    op += litLen

    if (op >= uncompressedSize) break
    if (ip >= input.length) break

    // Match offset — bounds check
    if (ip + 1 >= input.length) {
      throw new Error('LZ4: unexpected end of input reading match offset')
    }
    const offset = input[ip] | (input[ip + 1] << 8)
    ip += 2

    if (offset === 0) throw new Error('LZ4: invalid offset 0')

    // Match length
    let matchLen = (token & 0x0f) + 4
    if ((token & 0x0f) === 15) {
      let b: number
      do {
        if (ip >= input.length) throw new Error('LZ4: unexpected end of input reading match length')
        b = input[ip++]
        matchLen += b
      } while (b === 255)
    }

    if (op - offset < 0) {
      throw new Error('LZ4: match offset points before output start')
    }
    if (op + matchLen > uncompressedSize) {
      throw new Error('LZ4: match would overflow output buffer')
    }

    // Copy match — use fast copyWithin for non-overlapping cases
    let matchPos = op - offset
    if (offset >= matchLen) {
      output.copyWithin(op, matchPos, matchPos + matchLen)
      op += matchLen
    } else {
      for (let i = 0; i < matchLen; i++) {
        output[op++] = output[matchPos++]
      }
    }
  }

  return output.subarray(0, op)
}

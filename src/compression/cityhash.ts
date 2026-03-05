/**
 * CityHash128 implementation for ClickHouse checksums.
 * Based on Google's CityHash and adapted for ClickHouse's specific usage.
 *
 * ClickHouse uses CityHash128 to checksum compressed blocks.
 */

const k0 = 0xc3a5c85c97cb3127n
const k1 = 0xb492b66fbe98f273n
const k2 = 0x9ae16a3b2f90404fn
// k3 = 0xc949d7c7509e6557n (reserved for future hash variants)

function toU64(n: bigint): bigint {
  return n & 0xffffffffffffffffn
}

function mul64(a: bigint, b: bigint): bigint {
  return toU64(a * b)
}

function rotate(val: bigint, shift: number): bigint {
  if (shift === 0) return val
  return toU64((val >> BigInt(shift)) | (val << BigInt(64 - shift)))
}

function shiftMix(val: bigint): bigint {
  return toU64(val ^ (val >> 47n))
}

function hash128to64(lo: bigint, hi: bigint): bigint {
  const kMul = 0x9ddfea08eb382d69n
  let a = toU64(mul64(lo ^ hi, kMul))
  a = toU64(a ^ (a >> 47n))
  let b = toU64(mul64(hi ^ a, kMul))
  b = toU64(b ^ (b >> 47n))
  b = mul64(b, kMul)
  return b
}

function fetch64(buf: Buffer, pos: number): bigint {
  return buf.readBigUInt64LE(pos)
}

function fetch32(buf: Buffer, pos: number): bigint {
  return BigInt(buf.readUInt32LE(pos))
}

function weakHashLen32WithSeeds(
  w: bigint,
  x: bigint,
  y: bigint,
  z: bigint,
  a: bigint,
  b: bigint,
): [bigint, bigint] {
  a = toU64(a + w)
  b = toU64(rotate(toU64(b + a + z), 21))
  const c = a
  a = toU64(a + x)
  a = toU64(a + y)
  b = toU64(b + rotate(a, 44))
  return [toU64(a + z), toU64(b + c)]
}

function weakHashLen32WithSeedsBuf(
  buf: Buffer,
  pos: number,
  a: bigint,
  b: bigint,
): [bigint, bigint] {
  return weakHashLen32WithSeeds(
    fetch64(buf, pos),
    fetch64(buf, pos + 8),
    fetch64(buf, pos + 16),
    fetch64(buf, pos + 24),
    a,
    b,
  )
}

function cityHash128WithSeed(
  buf: Buffer,
  offset: number,
  len: number,
  seedLo: bigint,
  seedHi: bigint,
): [bigint, bigint] {
  if (len < 128) {
    return cityMurmur(buf, offset, len, seedLo, seedHi)
  }

  let x = seedLo
  let y = seedHi
  let z = toU64(mul64(BigInt(len), k1))
  let v0 = toU64(rotate(toU64(y ^ k1), 49) + fetch64(buf, offset))
  let v1: bigint
  {
    const tmp = toU64(v0 + fetch64(buf, offset + 8))
    v1 = tmp
  }
  let w0 = toU64(rotate(toU64(x + fetch64(buf, offset + 88)), 53))
  let w1 = toU64(x)

  let s = offset
  let remaining = len

  do {
    x = toU64(rotate(toU64(x + y + v0 + fetch64(buf, s + 8)), 37) * k1)
    y = toU64(rotate(toU64(y + v1 + fetch64(buf, s + 48)), 42) * k1)
    x = toU64(x ^ w1)
    y = toU64(y + v0 + fetch64(buf, s + 40))
    z = toU64(rotate(toU64(z + w0), 33) * k1)
    ;[v0, v1] = weakHashLen32WithSeedsBuf(buf, s, toU64(v1 * k1), x + w0)
    ;[w0, w1] = weakHashLen32WithSeedsBuf(
      buf,
      s + 32,
      toU64(z + w1),
      toU64(y + fetch64(buf, s + 16)),
    )
    ;[z, x] = [x, z]
    s += 64

    x = toU64(rotate(toU64(x + y + v0 + fetch64(buf, s + 8)), 37) * k1)
    y = toU64(rotate(toU64(y + v1 + fetch64(buf, s + 48)), 42) * k1)
    x = toU64(x ^ w1)
    y = toU64(y + v0 + fetch64(buf, s + 40))
    z = toU64(rotate(toU64(z + w0), 33) * k1)
    ;[v0, v1] = weakHashLen32WithSeedsBuf(buf, s, toU64(v1 * k1), x + w0)
    ;[w0, w1] = weakHashLen32WithSeedsBuf(
      buf,
      s + 32,
      toU64(z + w1),
      toU64(y + fetch64(buf, s + 16)),
    )
    ;[z, x] = [x, z]
    s += 64
    remaining -= 128
  } while (remaining >= 128)

  x = toU64(x + rotate(v0 + z, 49) * k0)
  y = toU64(y * k0 + rotate(w1, 37))
  z = toU64(z * k0 + rotate(w0, 27))
  w0 = mul64(w0, 9n)
  v0 = mul64(v0, 5n)

  for (let tail = 0; tail < remaining; tail += 32) {
    const idx = s + tail
    if (idx + 32 > offset + len) break
    y = toU64(mul64(toU64(x + y), k0) + v0)
    const w = toU64(y + fetch64(buf, idx))
    x = toU64(mul64(toU64(x + v1), k1) + w0)
    z = toU64(z + fetch64(buf, idx + 8))
    w0 = toU64(w0 + fetch64(buf, idx + 16))
    ;[v0, v1] = weakHashLen32WithSeedsBuf(
      buf,
      idx,
      toU64(v0 + z),
      toU64(v1 + w),
    )
    v0 = mul64(v0, 5n)
  }

  // Final hash uses the last 32 bytes of input
  const end = offset + len
  x = toU64(shiftMix(toU64(x + y + v0 + fetch64(buf, end - 32))) * k0)
  y = toU64(shiftMix(toU64(y + v1 + fetch64(buf, end - 24))) * k0)

  const lo = toU64(hash128to64(x, y) + toU64(shiftMix(w0) * 9n) + z)
  const hi = toU64(
    hash128to64(
      toU64(x + fetch64(buf, end - 16)),
      toU64(y + fetch64(buf, end - 8)),
    ) + toU64(shiftMix(toU64(w0 + z)) * 9n),
  )

  return [lo, hi]
}

function cityMurmur(
  buf: Buffer,
  offset: number,
  len: number,
  seedLo: bigint,
  seedHi: bigint,
): [bigint, bigint] {
  let a = seedLo
  let b = seedHi
  let c: bigint
  let d: bigint

  if (len <= 16) {
    a = toU64(shiftMix(toU64(a * k1)) * k1)
    c = toU64(b * k1 + hashLen0to16(buf, offset, len))
    d = toU64(shiftMix(toU64(a + (len >= 8 ? fetch64(buf, offset) : c))))
  } else {
    c = hash128to64(toU64(fetch64(buf, offset + len - 8) + k1), a)
    d = hash128to64(
      toU64(b + BigInt(len)),
      toU64(c + fetch64(buf, offset + len - 16)),
    )
    a = toU64(a + d)
    let pos = offset
    let remaining = len - 16
    while (remaining > 0) {
      a = toU64(a ^ toU64(shiftMix(toU64(fetch64(buf, pos) * k1)) * k1))
      a = mul64(a, k1)
      b = toU64(b ^ a)
      c = toU64(c ^ toU64(shiftMix(toU64(fetch64(buf, pos + 8) * k1)) * k1))
      c = mul64(c, k1)
      d = toU64(d ^ c)
      pos += 16
      remaining -= 16
    }
  }

  a = hash128to64(a, c)
  b = hash128to64(d, b)
  return [toU64(a ^ b), hash128to64(b, a)]
}

function hashLen0to16(buf: Buffer, offset: number, len: number): bigint {
  if (len >= 8) {
    const mul = toU64(k2 + BigInt(len) * 2n)
    const a = toU64(fetch64(buf, offset) + k2)
    const b = fetch64(buf, offset + len - 8)
    const c = toU64(rotate(b, 37) * mul + a)
    const d = toU64(toU64(rotate(a, 25) + b) * mul)
    return hashLen16(c, d, mul)
  }
  if (len >= 4) {
    const mul = toU64(k2 + BigInt(len) * 2n)
    const a = fetch32(buf, offset)
    return hashLen16(
      toU64(BigInt(len) + (a << 3n)),
      fetch32(buf, offset + len - 4),
      mul,
    )
  }
  if (len > 0) {
    const a = buf[offset]
    const b = buf[offset + (len >> 1)]
    const c = buf[offset + len - 1]
    const y = BigInt(a) + (BigInt(b) << 8n)
    const z = BigInt(len) + (BigInt(c) << 2n)
    return toU64(shiftMix(toU64((y * k2) ^ (z * k0))) * k2)
  }
  return k2
}

function hashLen16(u: bigint, v: bigint, mul: bigint): bigint {
  let a = toU64(mul64(toU64(u ^ v), mul))
  a = toU64(a ^ (a >> 47n))
  let b = toU64(mul64(toU64(v ^ a), mul))
  b = toU64(b ^ (b >> 47n))
  b = mul64(b, mul)
  return b
}

/**
 * Compute CityHash128 of the given buffer (or sub-range).
 * Returns [lo, hi] as two bigints (UInt64 each).
 */
export function cityHash128(
  buf: Buffer,
  offset = 0,
  length?: number,
): [bigint, bigint] {
  const len = length ?? buf.length - offset

  if (len <= 16) {
    if (len <= 8) {
      const lo = hashLen0to16(buf, offset, len)
      const hi = hash128to64(lo, k0)
      return [lo, hi]
    }
    return cityMurmur(buf, offset, len, k0, k1)
  }

  const seedLo = fetch64(buf, offset)
  const seedHi = toU64(fetch64(buf, offset + 8) + k0)

  return cityHash128WithSeed(buf, offset, len, seedLo, seedHi)
}

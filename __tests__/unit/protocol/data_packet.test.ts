import { describe, it, expect } from 'vitest'
import { BinaryWriter } from '../../../src/protocol/binary_writer'
import { BinaryReader } from '../../../src/protocol/binary_reader'
import { writeBlockInfo, readBlockInfo } from '../../../src/protocol/block_info'
import { readBlockHeader, writeBlockHeader } from '../../../src/protocol/data_packet'

describe('BlockInfo', () => {
  it('should roundtrip block info', () => {
    const w = new BinaryWriter()
    writeBlockInfo(w, { isOverflows: false, bucketNum: -1 })
    const r = new BinaryReader(w.getBuffer())
    const info = readBlockInfo(r)
    expect(info.isOverflows).toBe(false)
    expect(info.bucketNum).toBe(-1)
  })

  it('should roundtrip block info with overflow', () => {
    const w = new BinaryWriter()
    writeBlockInfo(w, { isOverflows: true, bucketNum: 42 })
    const r = new BinaryReader(w.getBuffer())
    const info = readBlockInfo(r)
    expect(info.isOverflows).toBe(true)
    expect(info.bucketNum).toBe(42)
  })

  it('should roundtrip default block info', () => {
    const w = new BinaryWriter()
    writeBlockInfo(w)
    const r = new BinaryReader(w.getBuffer())
    const info = readBlockInfo(r)
    expect(info.isOverflows).toBe(false)
    expect(info.bucketNum).toBe(-1)
  })
})

describe('BlockHeader', () => {
  it('should roundtrip block header', () => {
    const w = new BinaryWriter()
    writeBlockHeader(w, {
      info: { isOverflows: false, bucketNum: -1 },
      columns: [
        { name: 'id', type: 'UInt32', data: [] },
        { name: 'name', type: 'String', data: [] },
      ],
      rows: 100,
    })
    const r = new BinaryReader(w.getBuffer())
    const header = readBlockHeader(r)
    expect(header.numColumns).toBe(2)
    expect(header.numRows).toBe(100)
    expect(header.info.isOverflows).toBe(false)
  })
})

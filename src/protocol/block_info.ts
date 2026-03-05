import { BinaryReader } from './binary_reader'
import { BinaryWriter } from './binary_writer'

export interface BlockInfo {
  isOverflows: boolean
  bucketNum: number
}

export function writeBlockInfo(writer: BinaryWriter, info?: BlockInfo): void {
  // field 1: is_overflows
  writer.writeVarUInt(1)
  writer.writeUInt8(info?.isOverflows ? 1 : 0)
  // field 2: bucket_num
  writer.writeVarUInt(2)
  writer.writeInt32(info?.bucketNum ?? -1)
  // end marker
  writer.writeVarUInt(0)
}

export function readBlockInfo(reader: BinaryReader): BlockInfo {
  let isOverflows = false
  let bucketNum = -1

  while (true) {
    const fieldNum = reader.readVarUInt()
    if (fieldNum === 0) break
    switch (fieldNum) {
      case 1:
        isOverflows = reader.readUInt8() !== 0
        break
      case 2:
        bucketNum = reader.readInt32()
        break
      default:
        throw new Error(`Unknown BlockInfo field: ${fieldNum}`)
    }
  }

  return { isOverflows, bucketNum }
}

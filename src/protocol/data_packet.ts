import type { BinaryReader } from './binary_reader'
import type { BinaryWriter } from './binary_writer'
import type { BlockInfo } from './block_info'
import { readBlockInfo, writeBlockInfo } from './block_info'
import { ClientPacketType } from './packet_types'

export interface ColumnData {
  name: string
  type: string
  data: unknown[]
}

export interface Block {
  info: BlockInfo
  columns: ColumnData[]
  rows: number
}

export function emptyBlock(): Block {
  return {
    info: { isOverflows: false, bucketNum: -1 },
    columns: [],
    rows: 0,
  }
}

export function writeDataPacketHeader(writer: BinaryWriter): void {
  writer.writeVarUInt(ClientPacketType.Data)
  // temporary table name (empty for normal usage)
  writer.writeString('')
}

export function writeEmptyBlock(writer: BinaryWriter): void {
  writeDataPacketHeader(writer)
  writeBlockInfo(writer)
  // 0 columns, 0 rows
  writer.writeVarUInt(0)
  writer.writeVarUInt(0)
}

export function writeBlockHeader(writer: BinaryWriter, block: Block): void {
  writeBlockInfo(writer, block.info)
  writer.writeVarUInt(block.columns.length)
  writer.writeVarUInt(block.rows)
}

export interface BlockHeader {
  info: BlockInfo
  numColumns: number
  numRows: number
}

const MAX_COLUMNS = 10_000
const MAX_ROWS_PER_BLOCK = 10_000_000

export function readBlockHeader(reader: BinaryReader): BlockHeader {
  const info = readBlockInfo(reader)
  const numColumns = reader.readVarUInt()
  const numRows = reader.readVarUInt()
  if (numColumns > MAX_COLUMNS) {
    throw new Error(
      `Block column count ${numColumns} exceeds maximum ${MAX_COLUMNS}`,
    )
  }
  if (numRows > MAX_ROWS_PER_BLOCK) {
    throw new Error(
      `Block row count ${numRows} exceeds maximum ${MAX_ROWS_PER_BLOCK}`,
    )
  }
  return { info, numColumns, numRows }
}

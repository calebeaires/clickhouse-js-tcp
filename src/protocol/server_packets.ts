import type { BinaryReader } from './binary_reader'

export interface ProgressPacket {
  rows: number
  bytes: number
  totalRows: number
  writtenRows: number
  writtenBytes: number
  elapsedNs: bigint
}

export interface ProfileInfoPacket {
  rows: number
  blocks: number
  bytes: number
  appliedLimit: boolean
  rowsBeforeLimit: number
  calculatedRowsBeforeLimit: boolean
}

export interface LogEntry {
  time: number
  timeMicroseconds: number
  hostName: string
  queryId: string
  threadId: bigint
  priority: number
  source: string
  text: string
}

export function readProgress(reader: BinaryReader): ProgressPacket {
  return {
    rows: reader.readVarUInt(),
    bytes: reader.readVarUInt(),
    totalRows: reader.readVarUInt(),
    writtenRows: reader.readVarUInt(),
    writtenBytes: reader.readVarUInt(),
    // elapsedNs is only sent for revision >= 54460 (DBMS_MIN_PROTOCOL_VERSION_WITH_PROGRESS_ELAPSED_NS)
    // We advertise 54423, so the server does NOT send this field.
    elapsedNs: 0n,
  }
}

export function readProfileInfo(reader: BinaryReader): ProfileInfoPacket {
  return {
    rows: reader.readVarUInt(),
    blocks: reader.readVarUInt(),
    bytes: reader.readVarUInt(),
    appliedLimit: reader.readBool(),
    rowsBeforeLimit: reader.readVarUInt(),
    calculatedRowsBeforeLimit: reader.readBool(),
  }
}

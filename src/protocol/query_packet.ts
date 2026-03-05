import { BinaryWriter } from './binary_writer'
import {
  ClientPacketType,
  PROTOCOL_VERSION,
  CLIENT_NAME,
  CLIENT_VERSION_MAJOR,
  CLIENT_VERSION_MINOR,
  CLIENT_VERSION_PATCH,
  QueryProcessingStage,
} from './packet_types'
import * as os from 'os'

export interface QueryParams {
  queryId: string
  query: string
  database: string
  username: string
  compression: boolean
  settings?: Record<string, string | number | boolean>
  stage?: QueryProcessingStage
}

export function writeQueryPacket(
  writer: BinaryWriter,
  params: QueryParams,
): void {
  writer.writeVarUInt(ClientPacketType.Query)

  // query_id
  writer.writeString(params.queryId)

  // client_info
  writeClientInfo(writer, params)

  // Settings serialized as strings with flags (revision >= 54423)
  // Format: name(String) + flags(VarUInt) + value(String), terminated by empty name
  if (params.settings) {
    for (const [key, value] of Object.entries(params.settings)) {
      writer.writeString(key)
      writer.writeVarUInt(1) // flags: important=1
      writer.writeString(String(value))
    }
  }
  // End of settings
  writer.writeString('')

  // interserver_secret (revision >= 54441, we advertise 54453)
  writer.writeString('')

  // query processing stage
  writer.writeVarUInt(params.stage ?? QueryProcessingStage.Complete)

  // compression
  writer.writeVarUInt(params.compression ? 1 : 0)

  // query body
  writer.writeString(params.query)
}

function writeClientInfo(writer: BinaryWriter, params: QueryParams): void {
  // query_kind = InitialQuery
  writer.writeUInt8(1)
  // initial_user
  writer.writeString(params.username)
  // initial_query_id
  writer.writeString(params.queryId)
  // initial_address
  writer.writeString('0.0.0.0:0')

  // initial_query_start_time (revision >= 54449, we advertise 54453)
  writer.writeInt64(0n)

  // interface = TCP
  writer.writeUInt8(1)

  // os_user
  writer.writeString(os.userInfo().username || '')
  // client_hostname
  writer.writeString(os.hostname())
  // client_name
  writer.writeString(CLIENT_NAME)
  // version
  writer.writeVarUInt(CLIENT_VERSION_MAJOR)
  writer.writeVarUInt(CLIENT_VERSION_MINOR)
  writer.writeVarUInt(PROTOCOL_VERSION)

  // quota_key
  writer.writeString('')

  // distributed_depth (revision >= 54448, we advertise 54453)
  writer.writeVarUInt(0)

  // client patch version (revision >= 54423)
  writer.writeVarUInt(CLIENT_VERSION_PATCH)

  // OpenTelemetry (revision >= 54442, we advertise 54453)
  // flag = 0 means no tracing
  writer.writeUInt8(0)

  // parallel replicas (revision >= 54453, we advertise 54453)
  writer.writeVarUInt(0) // collaborate_with_initiator
  writer.writeVarUInt(0) // count_participating_replicas
  writer.writeVarUInt(0) // number_of_current_replica
}

export function writePingPacket(writer: BinaryWriter): void {
  writer.writeVarUInt(ClientPacketType.Ping)
}

export function writeCancelPacket(writer: BinaryWriter): void {
  writer.writeVarUInt(ClientPacketType.Cancel)
}

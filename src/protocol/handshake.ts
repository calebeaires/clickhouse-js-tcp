import type { BinaryReader } from './binary_reader'
import type { BinaryWriter } from './binary_writer'
import {
  CLIENT_NAME,
  CLIENT_VERSION_MAJOR,
  CLIENT_VERSION_MINOR,
  PROTOCOL_VERSION,
  ClientPacketType,
  MIN_PROTOCOL_VERSION,
} from './packet_types'

export interface ClientHelloParams {
  database: string
  username: string
  password: string
}

export interface ServerHello {
  serverName: string
  versionMajor: number
  versionMinor: number
  revision: number
  timezone: string
  displayName: string
}

export interface ServerException {
  code: number
  name: string
  message: string
  stackTrace: string
  nested?: ServerException
}

export function writeClientHello(
  writer: BinaryWriter,
  params: ClientHelloParams,
): void {
  writer.writeVarUInt(ClientPacketType.Hello)
  writer.writeString(CLIENT_NAME)
  writer.writeVarUInt(CLIENT_VERSION_MAJOR)
  writer.writeVarUInt(CLIENT_VERSION_MINOR)
  writer.writeVarUInt(PROTOCOL_VERSION)
  writer.writeString(params.database)
  writer.writeString(params.username)
  writer.writeString(params.password)
}

/**
 * Write the client addendum that must be sent after receiving the Server Hello
 * on newer protocol revisions. This includes chunked protocol negotiation.
 */
export function writeClientAddendum(writer: BinaryWriter): void {
  // proto_send_chunked_cl
  writer.writeString('notchunked')
  // proto_recv_chunked_cl
  writer.writeString('notchunked')
}

export function readServerHello(reader: BinaryReader): ServerHello {
  const serverName = reader.readString()
  const versionMajor = reader.readVarUInt()
  const versionMinor = reader.readVarUInt()
  const revision = reader.readVarUInt()

  if (revision < MIN_PROTOCOL_VERSION) {
    throw new Error(
      `Server revision ${revision} is below minimum ${MIN_PROTOCOL_VERSION}`,
    )
  }

  let timezone = ''
  if (revision >= 54423) {
    timezone = reader.readString()
  }

  let displayName = ''
  if (revision >= 54372) {
    displayName = reader.readString()
  }

  // Server patch version — the server always sends this for revision >= 54423
  // regardless of what protocol version the client advertised.
  if (reader.remaining() > 0) {
    reader.readVarUInt() // patch_version
  }

  return {
    serverName,
    versionMajor,
    versionMinor,
    revision,
    timezone,
    displayName,
  }
}

const MAX_EXCEPTION_DEPTH = 100

export function readServerException(
  reader: BinaryReader,
  depth = 0,
): ServerException {
  if (depth > MAX_EXCEPTION_DEPTH) {
    throw new Error(
      `Nested exception depth ${depth} exceeds maximum ${MAX_EXCEPTION_DEPTH}`,
    )
  }
  const code = reader.readInt32()
  const name = reader.readString()
  const message = reader.readString()
  const stackTrace = reader.readString()
  const hasNested = reader.readBool()

  const exception: ServerException = { code, name, message, stackTrace }
  if (hasNested) {
    exception.nested = readServerException(reader, depth + 1)
  }
  return exception
}

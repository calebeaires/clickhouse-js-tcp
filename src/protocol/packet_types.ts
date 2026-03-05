/** Packets sent by the client to the server. */
export enum ClientPacketType {
  Hello = 0,
  Query = 1,
  Data = 2,
  Cancel = 3,
  Ping = 4,
  TablesStatusRequest = 5,
  KeepAlive = 6,
}

/** Packets sent by the server to the client. */
export enum ServerPacketType {
  Hello = 0,
  Data = 1,
  Exception = 2,
  Progress = 3,
  Pong = 4,
  EndOfStream = 5,
  ProfileInfo = 6,
  Totals = 7,
  Extremes = 8,
  TablesStatusResponse = 9,
  Log = 10,
  TableColumns = 11,
  PartUUIDs = 12,
  ReadTaskRequest = 13,
  ProfileEvents = 14,
  MergeTreeAllRangesAnnouncement = 15,
  MergeTreeReadTaskRequest = 16,
  TimezoneUpdate = 17,
}

/** Compression methods used in the native protocol. */
export enum CompressionMethod {
  None = 0x02,
  LZ4 = 0x82,
  ZSTD = 0x90,
}

/** Query processing stage. */
export enum QueryProcessingStage {
  FetchColumns = 0,
  WithMergeableState = 1,
  Complete = 2,
  WithMergeableStateAfterAggregation = 3,
  WithMergeableStateAfterAggregationAndLimit = 4,
}

/** The minimum server revision we support. */
export const MIN_PROTOCOL_VERSION = 54423

/** Current client protocol version we advertise.
 *  54453 supports: settings as strings with flags, interserver_secret,
 *  OpenTelemetry, distributed_depth, initial_query_start_time, parallel_replicas.
 *  Below 54456 to avoid client addendum / nonce exchange. */
export const PROTOCOL_VERSION = 54453

export const CLIENT_NAME = 'clickhouse-tcp'
export const CLIENT_VERSION_MAJOR = 24
export const CLIENT_VERSION_MINOR = 1
export const CLIENT_VERSION_PATCH = 0

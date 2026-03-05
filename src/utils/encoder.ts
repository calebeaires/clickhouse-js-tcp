import * as Stream from 'stream'
import type { ValuesEncoder } from '@clickhouse/client-common'

export class TcpValuesEncoder implements ValuesEncoder<Stream.Readable> {
  validateInsertValues<T = unknown>(
    values: unknown,
    format: string,
  ): void {
    if (values === undefined || values === null) {
      throw new Error('Insert values cannot be null or undefined')
    }
  }

  encodeValues<T = unknown>(
    values: unknown,
    format: string,
  ): string | Stream.Readable {
    // For TCP native protocol, we pass through values as-is
    // The actual encoding happens in the connection layer using column codecs
    if (typeof values === 'string') {
      return values
    }

    if (values instanceof Stream.Readable) {
      return values
    }

    if (Array.isArray(values)) {
      // Convert array of objects to JSON string for now
      // The connection layer will parse and convert to native blocks
      return JSON.stringify(values)
    }

    return JSON.stringify(values)
  }
}

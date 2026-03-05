import * as Stream from 'stream'
import type { ValuesEncoder } from '@clickhouse/client-common'

export class TcpValuesEncoder implements ValuesEncoder<Stream.Readable> {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  validateInsertValues(_values: unknown, _format: string): void {
    if (_values === undefined || _values === null) {
      throw new Error('Insert values cannot be null or undefined')
    }
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  encodeValues(values: unknown, _format: string): string | Stream.Readable {
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

import * as Stream from 'stream'
import type { BaseResultSet, ResponseHeaders, DataFormat } from '@clickhouse/client-common'

export class TcpResultSet<Format extends DataFormat | unknown>
  implements BaseResultSet<Stream.Readable, Format>
{
  readonly query_id: string
  readonly response_headers: ResponseHeaders
  private _stream: Stream.Readable
  private _consumed = false

  private constructor(params: {
    stream: Stream.Readable
    query_id: string
    response_headers: ResponseHeaders
  }) {
    this._stream = params.stream
    this.query_id = params.query_id
    this.response_headers = params.response_headers
  }

  static instance<F extends DataFormat | unknown>(params: {
    stream: Stream.Readable
    format: F
    query_id: string
    log_error: (err: Error) => void
    response_headers: ResponseHeaders
    json: unknown
  }): TcpResultSet<F> {
    return new TcpResultSet<F>({
      stream: params.stream,
      query_id: params.query_id,
      response_headers: params.response_headers,
    })
  }

  async text(): Promise<string> {
    const rows = await this.collectRows()
    return rows.map((r) => JSON.stringify(r)).join('\n')
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async json<T = unknown>(): Promise<any> {
    const rows = await this.collectRows()
    return rows as T[]
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  stream(): any {
    if (this._consumed) {
      throw new Error('ResultSet stream already consumed')
    }
    this._consumed = true
    return this._stream
  }

  close(): void {
    if (!this._consumed) {
      this._consumed = true
      this._stream.destroy()
    }
  }

  private async collectRows(): Promise<unknown[]> {
    if (this._consumed) {
      throw new Error('ResultSet already consumed')
    }
    this._consumed = true

    const rows: unknown[] = []
    for await (const chunk of this._stream) {
      rows.push(chunk)
    }
    return rows
  }
}

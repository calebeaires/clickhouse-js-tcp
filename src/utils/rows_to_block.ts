import type { Block, ColumnData } from '../protocol/data_packet'

export function rowsToBlock(
  rows: Record<string, unknown>[],
  schema: Array<{ name: string; type: string }>,
): Block {
  if (rows.length === 0) {
    return {
      info: { isOverflows: false, bucketNum: -1 },
      columns: schema.map((s) => ({ name: s.name, type: s.type, data: [] })),
      rows: 0,
    }
  }

  const columns: ColumnData[] = schema.map((s) => {
    const data = rows.map((row) => row[s.name])
    return { name: s.name, type: s.type, data }
  })

  return {
    info: { isOverflows: false, bucketNum: -1 },
    columns,
    rows: rows.length,
  }
}

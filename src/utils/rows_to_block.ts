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

  const columns: ColumnData[] = schema.map((s) => ({
    name: s.name, type: s.type, data: new Array(rows.length),
  }))

  for (let r = 0; r < rows.length; r++) {
    const row = rows[r]
    for (let c = 0; c < columns.length; c++) {
      columns[c].data[r] = row[schema[c].name]
    }
  }

  return {
    info: { isOverflows: false, bucketNum: -1 },
    columns,
    rows: rows.length,
  }
}

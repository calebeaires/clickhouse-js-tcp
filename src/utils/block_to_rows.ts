import type { Block } from '../protocol/data_packet'

export function blockToRows(blocks: Block[]): Record<string, unknown>[] {
  const result: Record<string, unknown>[] = []

  for (const block of blocks) {
    if (block.rows === 0 || block.columns.length === 0) continue

    for (let r = 0; r < block.rows; r++) {
      const row: Record<string, unknown> = {}
      for (const col of block.columns) {
        row[col.name] = col.data[r]
      }
      result.push(row)
    }
  }

  return result
}

export function blockToColumnar(blocks: Block[]): Record<string, unknown[]> {
  const result: Record<string, unknown[]> = {}

  for (const block of blocks) {
    for (const col of block.columns) {
      if (!result[col.name]) {
        result[col.name] = []
      }
      const arr = result[col.name]
      for (let i = 0; i < col.data.length; i++) arr.push(col.data[i])
    }
  }

  return result
}

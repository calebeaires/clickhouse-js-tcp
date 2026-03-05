import { defineConfig } from 'vitest/config'
import { readFileSync } from 'fs'
import { resolve } from 'path'

function loadDotEnv(): Record<string, string> {
  try {
    const content = readFileSync(resolve(__dirname, '.env'), 'utf-8')
    const env: Record<string, string> = {}
    for (const line of content.split('\n')) {
      const match = line.match(/^([^#=][^=]*)=(.*)$/)
      if (match) env[match[1].trim()] = match[2].trim()
    }
    return env
  } catch {
    return {}
  }
}

export default defineConfig({
  test: {
    include: ['__tests__/integration/**/*.test.ts'],
    testTimeout: 30_000,
    env: loadDotEnv(),
  },
})

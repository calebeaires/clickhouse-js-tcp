import { describe, it, expect } from 'vitest'
import { BinaryWriter } from '../../../src/protocol/binary_writer'
import { BinaryReader } from '../../../src/protocol/binary_reader'
import {
  writeClientHello,
  readServerHello,
  readServerException,
} from '../../../src/protocol/handshake'

describe('Handshake', () => {
  it('should write client hello', () => {
    const w = new BinaryWriter()
    writeClientHello(w, {
      database: 'default',
      username: 'default',
      password: '',
    })
    const buf = w.getBuffer()
    expect(buf.length).toBeGreaterThan(10)

    // First byte should be Hello packet type (0)
    const r = new BinaryReader(buf)
    expect(r.readVarUInt()).toBe(0) // ClientPacketType.Hello
  })

  it('should read server hello', () => {
    const w = new BinaryWriter()
    w.writeString('ClickHouse') // server_name
    w.writeVarUInt(24)           // version_major
    w.writeVarUInt(1)            // version_minor
    w.writeVarUInt(54470)        // revision
    w.writeString('UTC')         // timezone
    w.writeString('node-1')      // display_name

    const r = new BinaryReader(w.getBuffer())
    const hello = readServerHello(r)

    expect(hello.serverName).toBe('ClickHouse')
    expect(hello.versionMajor).toBe(24)
    expect(hello.versionMinor).toBe(1)
    expect(hello.revision).toBe(54470)
    expect(hello.timezone).toBe('UTC')
    expect(hello.displayName).toBe('node-1')
  })

  it('should reject low revision', () => {
    const w = new BinaryWriter()
    w.writeString('ClickHouse')
    w.writeVarUInt(20)
    w.writeVarUInt(1)
    w.writeVarUInt(10000) // too low

    const r = new BinaryReader(w.getBuffer())
    expect(() => readServerHello(r)).toThrow('below minimum')
  })

  it('should read server exception', () => {
    const w = new BinaryWriter()
    w.writeInt32(62)              // code
    w.writeString('DB::Exception') // name
    w.writeString('Table not found') // message
    w.writeString('stack trace here') // stack trace
    w.writeBool(false)            // no nested

    const r = new BinaryReader(w.getBuffer())
    const ex = readServerException(r)

    expect(ex.code).toBe(62)
    expect(ex.name).toBe('DB::Exception')
    expect(ex.message).toBe('Table not found')
    expect(ex.stackTrace).toBe('stack trace here')
    expect(ex.nested).toBeUndefined()
  })

  it('should read nested exception', () => {
    const w = new BinaryWriter()
    // Outer
    w.writeInt32(100)
    w.writeString('DB::Exception')
    w.writeString('outer error')
    w.writeString('')
    w.writeBool(true)
    // Nested
    w.writeInt32(200)
    w.writeString('DB::Exception')
    w.writeString('inner error')
    w.writeString('')
    w.writeBool(false)

    const r = new BinaryReader(w.getBuffer())
    const ex = readServerException(r)

    expect(ex.code).toBe(100)
    expect(ex.message).toBe('outer error')
    expect(ex.nested).toBeDefined()
    expect(ex.nested!.code).toBe(200)
    expect(ex.nested!.message).toBe('inner error')
  })
})

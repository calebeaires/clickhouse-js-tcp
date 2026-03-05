import { BinaryWriter } from '../protocol/binary_writer'
import { SocketManager } from './socket_manager'
import {
  writeClientHello,
  writeClientAddendum,
  ClientHelloParams,
} from '../protocol/handshake'
import {
  writeQueryPacket,
  writePingPacket,
  writeCancelPacket,
  QueryParams,
} from '../protocol/query_packet'
import {
  writeDataPacketHeader,
  writeEmptyBlock,
  writeBlockHeader,
  Block,
} from '../protocol/data_packet'
import type { ColumnWriter } from '../columns/column'
import { compressBlock } from '../compression/lz4'

export class PacketWriter {
  private socket: SocketManager

  constructor(socket: SocketManager) {
    this.socket = socket
  }

  sendHello(params: ClientHelloParams): void {
    const writer = new BinaryWriter()
    writeClientHello(writer, params)
    this.socket.write(writer.getBuffer())
  }

  sendAddendum(): void {
    const writer = new BinaryWriter()
    writeClientAddendum(writer)
    this.socket.write(writer.getBuffer())
  }

  sendPing(): void {
    const writer = new BinaryWriter()
    writePingPacket(writer)
    this.socket.write(writer.getBuffer())
  }

  sendCancel(): void {
    const writer = new BinaryWriter()
    writeCancelPacket(writer)
    this.socket.write(writer.getBuffer())
  }

  sendQuery(params: QueryParams): void {
    const writer = new BinaryWriter()
    writeQueryPacket(writer, params)
    this.socket.write(writer.getBuffer())

    // Send empty data block after query (required by protocol)
    const emptyWriter = new BinaryWriter()
    writeEmptyBlock(emptyWriter)
    this.socket.write(emptyWriter.getBuffer())
  }

  sendDataBlock(
    block: Block,
    columnWriters: ColumnWriter[],
  ): void {
    const writer = new BinaryWriter()
    writeDataPacketHeader(writer)
    writeBlockHeader(writer, block)

    for (let i = 0; i < block.columns.length; i++) {
      writer.writeString(block.columns[i].name)
      writer.writeString(block.columns[i].type)
      columnWriters[i].write(writer, block.columns[i].data)
    }

    this.socket.write(writer.getBuffer())
  }

  sendCompressedDataBlock(
    block: Block,
    columnWriters: ColumnWriter[],
  ): void {
    // Write the Data packet header (uncompressed: packet type + temp table name)
    const headerWriter = new BinaryWriter()
    writeDataPacketHeader(headerWriter)
    this.socket.write(headerWriter.getBuffer())

    // Write the block content (block header + columns) into a buffer, then compress
    const blockWriter = new BinaryWriter()
    writeBlockHeader(blockWriter, block)
    for (let i = 0; i < block.columns.length; i++) {
      blockWriter.writeString(block.columns[i].name)
      blockWriter.writeString(block.columns[i].type)
      columnWriters[i].write(blockWriter, block.columns[i].data)
    }

    const compressed = compressBlock(blockWriter.getBuffer())
    this.socket.write(compressed)
  }

  sendEmptyBlock(): void {
    const writer = new BinaryWriter()
    writeEmptyBlock(writer)
    this.socket.write(writer.getBuffer())
  }
}

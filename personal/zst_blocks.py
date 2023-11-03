# copied from https://github.com/ArthurHeitmann/zst_blocks_format

from __future__ import annotations
from dataclasses import dataclass
import os
import time
import struct
from typing import BinaryIO, Callable, Iterable, Literal
from zstandard import ZstdDecompressor, ZstdCompressor

_endian: Literal["little", "big"] = "little"

_uint32Struct = struct.Struct("<I")
_uint32X2Struct = struct.Struct("<II")

_defaultCompressionLevel = 3


class ZstBlocksFile:
	blocks: list[ZstBlock]

	def __init__(self, blocks: list[ZstBlock]):
		self.blocks = blocks

	@staticmethod
	def readBlockRowAt(file: BinaryIO, rowPosition: RowPosition) -> bytes:
		file.seek(rowPosition.blockOffset)
		return ZstBlock.readRow(file, rowPosition.rowIndex)

	@staticmethod
	def readMultipleBlocks(file: BinaryIO, rowPositions: list[RowPosition]) -> \
	list[bytes]:
		blockGroupsDict: dict[int, RowPositionGroup] = {}
		for i, rowPosition in enumerate(rowPositions):
			if rowPosition.blockOffset not in blockGroupsDict:
				blockGroupsDict[rowPosition.blockOffset] = RowPositionGroup(
					rowPosition.blockOffset, [])
			blockGroupsDict[rowPosition.blockOffset].rowIndices.append(
				RowIndex(rowPosition.rowIndex, i))
		blockGroups = list(blockGroupsDict.values())

		rows: list = [None] * len(rowPositions)
		for blockGroup in blockGroups:
			file.seek(blockGroup.blockOffset)
			blockRows = ZstBlock.readSpecificRows(file, map(lambda
																pair: pair.withinBlockIndex,
															blockGroup.rowIndices))
			for originalPosition, row in zip(blockGroup.rowIndices, blockRows):
				rows[originalPosition.originalRowIndex] = row

		return rows

	@staticmethod
	def streamRows(file: BinaryIO, blockIndexProgressCallback: Callable[[
		int], None] | None = None) -> Iterable[bytes]:
		fileSize = os.path.getsize(file.name)
		blockIndex = 0
		while file.tell() < fileSize:
			yield from ZstBlock.streamRows(file)
			blockIndex += 1
			if blockIndexProgressCallback is not None:
				blockIndexProgressCallback(blockIndex)

	@staticmethod
	def appendBlock(file: BinaryIO, rows: list[bytes],
					compressionLevel=_defaultCompressionLevel) -> None:
		file.seek(file.tell())
		ZstBlock(rows).write(file, compressionLevel=compressionLevel)

	@staticmethod
	def writeStream(file: BinaryIO, rowStream: Iterable[bytes], blockSize: int,
					rowPositions: list[RowPosition] | None = None,
					compressionLevel=_defaultCompressionLevel) -> None:
		pendingRows = []
		for row in rowStream:
			pendingRows.append(row)
			if len(pendingRows) >= blockSize:
				ZstBlock(pendingRows).write(file, rowPositions,
											compressionLevel=compressionLevel)
				pendingRows = []
		if len(pendingRows) > 0:
			ZstBlock(pendingRows).write(file, rowPositions,
										compressionLevel=compressionLevel)

	@staticmethod
	def writeBlocksStream(file: BinaryIO, blocksStream: Iterable[list[bytes]],
						  rowPositions: list[RowPosition] | None = None,
						  compressionLevel=_defaultCompressionLevel) -> None:
		for rows in blocksStream:
			ZstBlock(rows).write(file, rowPositions,
								 compressionLevel=compressionLevel)

	@staticmethod
	def countBlocks(file: BinaryIO) -> int:
		fileSize = os.path.getsize(file.name)
		blockCount = 0
		initialPos = file.tell()
		pos = initialPos
		while pos < fileSize:
			blockCount += 1
			blockSize = _uint32Struct.unpack(file.read(4))[0]
			pos += 4 + blockSize
			file.seek(pos)
		file.seek(initialPos)
		return blockCount

	@staticmethod
	def generateRowPositions(file: BinaryIO) -> Iterable[RowPosition]:
		fileSize = os.path.getsize(file.name)
		while file.tell() < fileSize:
			yield from ZstBlock.generateRowPositions(file)


class ZstBlock:
	rows: list[bytes]

	def __init__(self, rows: list[bytes]):
		self.rows = rows

	@classmethod
	def streamRows(cls, file: BinaryIO) -> Iterable[bytes]:
		compressedSize = _uint32Struct.unpack(file.read(4))[0]
		compressedData = file.read(compressedSize)
		decompressedData = ZstdDecompressor().decompress(compressedData)

		memoryView = memoryview(decompressedData)
		count = _uint32Struct.unpack(memoryView[0:4])[0]
		rows: list[ZstRowInfo] = [None] * count
		for i in range(count):
			rows[i] = ZstRowInfo.read(memoryView, 4 + i * ZstRowInfo.structSize)

		dataStart = 4 + count * ZstRowInfo.structSize
		for row in rows:
			yield decompressedData[
				  dataStart + row.offset: dataStart + row.offset + row.size]

	@classmethod
	def readSpecificRows(cls, file: BinaryIO, rowIndices: Iterable[int]) -> \
	list[bytes]:
		compressedSize = _uint32Struct.unpack(file.read(4))[0]
		compressedData = file.read(compressedSize)
		decompressedData = ZstdDecompressor().decompress(compressedData)

		memoryView = memoryview(decompressedData)
		count = _uint32Struct.unpack(memoryView[0:4])[0]
		rows: list[ZstRowInfo] = [None] * count
		for i in range(count):
			rows[i] = ZstRowInfo.read(memoryView, 4 + i * ZstRowInfo.structSize)

		dataStart = 4 + count * ZstRowInfo.structSize
		return [
			decompressedData[
			dataStart + rows[rowIndex].offset: dataStart + rows[
				rowIndex].offset + rows[rowIndex].size]
			for rowIndex in rowIndices
		]

	@classmethod
	def readRow(cls, file: BinaryIO, rowIndex: int) -> bytes:
		compressedSize = _uint32Struct.unpack(file.read(4))[0]
		compressedData = file.read(compressedSize)
		decompressedData = ZstdDecompressor().decompress(compressedData)

		memoryView = memoryview(decompressedData)
		count = _uint32Struct.unpack(memoryView[0:4])[0]
		if rowIndex >= count:
			raise Exception("Row index out of range")
		row = ZstRowInfo.read(memoryView, 4 + rowIndex * ZstRowInfo.structSize)

		dataStart = 4 + count * ZstRowInfo.structSize
		return decompressedData[
			   dataStart + row.offset: dataStart + row.offset + row.size]

	def write(self, file: BinaryIO,
			  rowPositions: list[RowPosition] | None = None,
			  compressionLevel=_defaultCompressionLevel) -> None:
		uncompressedSize = \
			4 + \
			len(self.rows) * ZstRowInfo.structSize + \
			sum(len(row) for row in self.rows)
		uncompressedBytes = bytearray(uncompressedSize)
		uncompressedBytes[0:4] = len(self.rows).to_bytes(4, _endian)

		dataOffset = 4 + len(self.rows) * ZstRowInfo.structSize
		blockOffset = file.tell()
		currentDataLocalOffset = 0
		for i in range(len(self.rows)):
			row = self.rows[i]
			rowInfo = ZstRowInfo(currentDataLocalOffset, len(row))
			rowInfo.write(uncompressedBytes, 4 + i * ZstRowInfo.structSize)
			uncompressedBytes[
			dataOffset + currentDataLocalOffset: dataOffset + currentDataLocalOffset + len(
				row)] = row
			currentDataLocalOffset += len(row)
			if rowPositions is not None:
				rowPositions.append(RowPosition(blockOffset, i))
		uncompressedData = bytes(uncompressedBytes)
		compressedData = ZstdCompressor(compressionLevel).compress(
			uncompressedData)
		compressedSize = len(compressedData)
		blockBytes = bytearray(4 + compressedSize)
		blockBytes[0:4] = compressedSize.to_bytes(4, _endian)
		blockBytes[4:4 + compressedSize] = compressedData
		file.write(blockBytes)

	@staticmethod
	def generateRowPositions(file: BinaryIO) -> Iterable[RowPosition]:
		blockOffset = file.tell()
		compressedSize = _uint32Struct.unpack(file.read(4))[0]
		compressedData = file.read(compressedSize)
		decompressedData = ZstdDecompressor().decompress(compressedData)

		memoryView = memoryview(decompressedData)
		count = _uint32Struct.unpack(memoryView[0:4])[0]
		for i in range(count):
			yield RowPosition(blockOffset, i)


class ZstRowInfo:
	structSize = 8
	offset: int
	size: int

	def __init__(self, offset: int, size: int):
		self.offset = offset
		self.size = size

	@staticmethod
	def read(bytes: bytes, position: int) -> ZstRowInfo:
		offset, size = _uint32X2Struct.unpack(
			bytes[position: position + ZstRowInfo.structSize])
		return ZstRowInfo(offset, size)

	def write(self, bytes: bytearray, position: int) -> None:
		bytes[position + 0: position + 4] = self.offset.to_bytes(4, _endian)
		bytes[position + 4: position + 8] = self.size.to_bytes(4, _endian)


@dataclass
class RowPosition:
	blockOffset: int
	rowIndex: int


@dataclass
class RowIndex:
	withinBlockIndex: int
	originalRowIndex: int


@dataclass
class RowPositionGroup:
	blockOffset: int
	rowIndices: list[RowIndex]

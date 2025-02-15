"""
lib/block/Storage.py

Purpose:
Implements a storage manager for fixed-size blocks. It maps logical block numbers to positions in an underlying file and manages free block indices.

Place in Architecture:
Provides the low-level storage backend for the block cache.
Interface:

	BlockStorage(file, block_size): Constructor.
	save_state(file) / restore_state(file, state_file): Saves/restores the block map.
	Methods for reconstructing the free map: _reconstruct_free_map(), _get_free_block_idx(), _add_free_block_idx(idx), _truncate_free_map(end_block).
	Special methods: __contains__(idx), __getitem__(idx), __setitem__(idx, data), and truncate(num_blocks).

TODOs/FIXMEs:
None explicitly noted.
"""


import struct
import errno
import array
import heapq
import zlib
import itertools
from .Utils import *


BLOCK_SIZE = 131072
BLOCK_UNALLOCATED = -1
BLOCK_ZERO = -2




class BlockStorage(object):
	"""
	File storing fixed-size blocks of data.
	"""

	def __init__(this, file, block_size):
		this.file = file
		this.block_size = block_size
		this.block_map = array.array('l')
		this.zero_block = b"\x00"*this.block_size
		this._reconstruct_free_map()

	def save_state(this, file):
		file.truncate(0)
		file.seek(0)
		file.write(b"BLK2")

		# Using zlib here is mainly for obfuscating information on the
		# total size of sparse files. The size of the map file will
		# correlate with the amount of downloaded data, but
		# compression reduces its correlation with the total size of
		# the file.
		block_map_data = zlib.compress(this.block_map.tobytes(), 9)
		file.write(struct.pack('<QQ', this.block_size, len(block_map_data)))
		file.write(block_map_data)

	@classmethod
	def restore_state(cls, file, state_file):
		hdr = state_file.read(4)
		if hdr != b"BLK2":
			raise ValueError("invalid block storage state file")
		s = state_file.read(2 * 8)
		block_size, data_size = struct.unpack('<QQ', s)

		try:
			s = zlib.decompress(state_file.read(data_size))
		except zlib.error:
			raise ValueError("invalid block map data")
		block_map = array.array('l')
		block_map.frombytes(s)
		del s

		this = cls.__new__(cls)
		this.file = file
		this.block_size = block_size
		this.block_map = block_map
		this.zero_block = b"\x00"*this.block_size
		this._reconstruct_free_map()
		return this

	def _reconstruct_free_map(this):
		if this.block_map:
			max_block = max(this.block_map)
		else:
			max_block = -1

		if max_block < 0:
			this.free_block_idx = 0
			this.free_map = []
			return

		mask = array.array('b', itertools.repeat(0, max_block+1))
		for x in this.block_map:
			if x >= 0:
				mask[x] = 1

		free_map = [j for j, x in enumerate(mask) if x == 0]
		heapq.heapify(free_map)

		this.free_map = free_map
		this.free_block_idx = max_block + 1

	def _get_free_block_idx(this):
		if this.free_map:
			return heapq.heappop(this.free_map)
		idx = this.free_block_idx
		this.free_block_idx += 1
		return idx

	def _add_free_block_idx(this, idx):
		heapq.heappush(this.free_map, idx)

	def _truncate_free_map(this, end_block):
		this.free_block_idx = end_block
		last_map_size = len(this.free_map)
		this.free_map = [x for x in this.free_map if x < end_block]
		if last_map_size != len(this.free_map):
			heapq.heapify(this.free_map)

	def __contains__(this, idx):
		if not idx >= 0:
			raise ValueError("Invalid block index")
		if idx >= len(this.block_map):
			return False
		return this.block_map[idx] != BLOCK_UNALLOCATED

	def __getitem__(this, idx):
		if idx not in this:
			raise KeyError("Block %d not allocated" % (idx,))

		block_idx = this.block_map[idx]
		if block_idx >= 0:
			this.file.seek(this.block_size * block_idx)
			block = this.file.read(this.block_size)
			if len(block) < this.block_size:
				# Partial block (end-of-file): consider zero-padded
				block += b"\x00"*(this.block_size - len(block))
			return block
		elif block_idx == BLOCK_ZERO:
			return this.zero_block
		else:
			raise IOError(errno.EIO, "Corrupted block map data")

	def __setitem__(this, idx, data):
		if not idx >= 0:
			raise ValueError("Invalid block index")
		if idx >= len(this.block_map):
			this.block_map.extend(itertools.repeat(BLOCK_UNALLOCATED, idx + 1 - len(this.block_map)))

		if data is None or data == this.zero_block:
			block_idx = this.block_map[idx]
			if block_idx >= 0:
				this._add_free_block_idx(block_idx)
			this.block_map[idx] = BLOCK_ZERO
		else:
			if len(data) > this.block_size:
				raise ValueError("Too large data block")

			block_idx = this.block_map[idx]
			if not block_idx >= 0:
				block_idx = this._get_free_block_idx()

			this.block_map[idx] = block_idx

			if len(data) < this.block_size:
				# Partial blocks are OK at the end of the file
				# only. Such blocks will be automatically zero-padded
				# by POSIX if writes are done to subsequent blocks.
				# Other blocks need explicit padding.
				this.file.seek(0, 2)
				pos = this.file.tell()
				if pos > this.block_size * block_idx + len(data):
					data += b"\x00" * (this.block_size - len(data))

			this.file.seek(this.block_size * block_idx)
			this.file.write(data)

	def truncate(this, num_blocks):
		this.block_map = this.block_map[:num_blocks]

		end_block = 0
		if this.block_map:
			end_block = max(0, max(this.block_map) + 1)
		this.file.truncate(this.block_size * end_block)
		this._truncate_free_map(end_block)

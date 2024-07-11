import struct
import errno
import array
import heapq
import zlib
import itertools
from .Storage import *
from .Utils import *

class BlockCachedFile(object):
	"""
	I am temporary file, caching data for a remote file. I support
	overwriting data. I cache remote data on a per-block basis and
	keep track of which blocks need still to be retrieved. Before each
	read/write operation, my pre_read or pre_write method needs to be
	called --- these give the ranges of data that need to be retrieved
	from the remote file and fed to me (via receive_cached_data)
	before the read/write operation can succeed. I am fully
	synchronous.
	"""

	def __init__(this, f, initial_cache_size, block_size=None):
		if block_size is None:
			block_size = BLOCK_SIZE
		this.size = initial_cache_size
		this.storage = BlockStorage(f, block_size)
		this.block_size = this.storage.block_size
		this.first_uncached_block = 0
		this.cache_size = initial_cache_size

	def save_state(this, f):
		this.storage.save_state(f)
		f.write(struct.pack('<QQQ', this.size, this.cache_size, this.first_uncached_block))

	@classmethod
	def restore_state(cls, f, state_file):
		storage = BlockStorage.restore_state(f, state_file)
		s = state_file.read(3 * 8)
		size, cache_size, first_uncached_block = struct.unpack('<QQQ', s)

		this = cls.__new__(cls)
		this.storage = storage
		this.size = size
		this.cache_size = cache_size
		this.first_uncached_block = first_uncached_block
		this.block_size = this.storage.block_size
		return this

	def _pad_file(this, new_size):
		"""
		Append zero bytes that the virtual size grows to new_size
		"""
		if new_size <= this.size:
			return

		# Fill remainder blocks in the file with nulls; the last
		# existing block, if partial, is implicitly null-padded
		start, mid, end = block_range(this.size, new_size - this.size, block_size=this.block_size)

		if start is not None and start[1] == 0:
			this.storage[start[0]] = None

		if mid is not None:
			for idx in range(*mid):
				this.storage[idx] = None

		if end is not None:
			this.storage[end[0]] = None

		this.size = new_size

	def receive_cached_data(this, offset, data_list):
		"""
		Write full data blocks to file, unless they were not written
		yet. Returns (new_offset, new_data_list) containing unused,
		possibly reuseable data. data_list is a list of strings.
		"""
		data_size = sum(len(data) for data in data_list)

		start, mid, end = block_range(offset, data_size, last_pos=this.cache_size,
									  block_size=this.block_size)

		if mid is None:
			# not enough data for full blocks
			return offset, data_list

		data = b"".join(data_list)

		i = 0
		if start is not None:
			# skip initial part
			i = this.block_size - start[1]

		for j in range(*mid):
			if j not in this.storage:
				block = data[i:i+this.block_size]
				this.storage[j] = block
			i += min(this.block_size, data_size - i)

		if mid[0] <= this.first_uncached_block:
			this.first_uncached_block = max(this.first_uncached_block, mid[1])

		# Return trailing data for possible future use
		if i < data_size:
			data_list = [data[i:]]
		else:
			data_list = []
		offset += i
		return (offset, data_list)

	def get_size(this):
		return this.size

	def get_file(this):
		# Pad file to full size before returning file handle
		this._pad_file(this.get_size())
		return BlockCachedFileHandle(this)

	def close(this):
		this.storage.f.close()
		this.storage = None

	def truncate(this, size):
		if size < this.size:
			this.storage.truncate(ceildiv(size, this.block_size))
			this.size = size
		elif size > this.size:
			this._pad_file(size)

		this.cache_size = min(this.cache_size, size)

	def write(this, offset, data):
		if offset > this.size:
			# Explicit POSIX behavior for write-past-end
			this._pad_file(offset)

		if len(data) == 0:
			# noop
			return

		# Perform write
		start, mid, end = block_range(offset, len(data), block_size=this.block_size)

		# Pad virtual size
		this._pad_file(offset + len(data))

		# Write first block
		if start is not None:
			block = this.storage[start[0]]
			i = start[2] - start[1]
			this.storage[start[0]] = block[:start[1]] + data[:i] + block[start[2]:]
		else:
			i = 0

		# Write intermediate blocks
		if mid is not None:
			for idx in range(*mid):
				this.storage[idx] = data[i:i+this.block_size]
				i += this.block_size

		# Write last block
		if end is not None:
			block = this.storage[end[0]]
			this.storage[end[0]] = data[i:] + block[end[1]:]

	def read(this, offset, length):
		length = max(0, min(this.size - offset, length))
		if length == 0:
			return b''

		# Perform read
		start, mid, end = block_range(offset, length, block_size=this.block_size)

		datas = []

		# Read first block
		if start is not None:
			datas.append(this.storage[start[0]][start[1]:start[2]])

		# Read intermediate blocks
		if mid is not None:
			for idx in range(*mid):
				datas.append(this.storage[idx])

		# Read last block
		if end is not None:
			datas.append(this.storage[end[0]][:end[1]])

		return b"".join(datas)

	def pre_read(this, offset, length):
		"""
		Return (offset, length) of the first cache fetch that need to be
		performed and the results fed into `receive_cached_data` before a read
		operation can be performed. There may be more than one fetch
		necessary. Return None if no fetch is necessary.
		"""

		# Limit to inside the cached area
		cache_end = ceildiv(this.cache_size, this.block_size) * this.block_size
		length = max(0, min(length, cache_end - offset))
		if length == 0:
			return None

		# Find bounds of the read operation
		start_block = offset//this.block_size
		end_block = ceildiv(offset + length, this.block_size)

		# Combine consequent blocks into a single read
		j = max(start_block, this.first_uncached_block)
		while j < end_block and j in this.storage:
			j += 1
		if j >= end_block:
			return None

		for k in range(j+1, end_block):
			if k in this.storage:
				end = k
				break
		else:
			end = end_block

		if j >= end:
			return None

		start_pos = j * this.block_size
		end_pos = end * this.block_size
		if start_pos < this.cache_size:
			return (start_pos, min(end_pos, this.cache_size) - start_pos)

		return None

	def pre_write(this, offset, length):
		"""
		Similarly to pre_read, but for write operations.
		"""
		start, mid, end = block_range(offset, length, block_size=this.block_size)

		# Writes only need partially available blocks to be in the cache
		for item in (start, end):
			if item is not None and item[0] >= this.first_uncached_block and item[0] not in this.storage:
				start_pos = item[0] * this.block_size
				end_pos = (item[0] + 1) * this.block_size
				if start_pos < this.cache_size:
					return (start_pos, min(this.cache_size, end_pos) - start_pos)

		# No reads required
		return None


class BlockCachedFileHandle(object):
	"""
	Read-only access to BlockCachedFile, as if it was a contiguous file
	"""
	def __init__(this, block_cached_file):
		this.block_cached_file = block_cached_file
		this.pos = 0

	def seek(this, offset, whence=0):
		if whence == 0:
			this.pos = offset
		elif whence == 1:
			this.pos += offset
		elif whence == 2:
			this.pos = offset + this.block_cached_file.get_size()
		else:
			raise ValueError("Invalid whence")

	def read(this, size=None):
		if size is None:
			size = max(0, this.block_cached_file.get_size() - this.pos)
		data = this.block_cached_file.read(this.pos, size)
		this.pos += len(data)
		return data
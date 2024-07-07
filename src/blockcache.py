import struct
import errno
import array
import heapq
import zlib
import itertools


BLOCK_SIZE = 131072
BLOCK_UNALLOCATED = -1
BLOCK_ZERO = -2


def ceildiv(a, b):
	"""Compute ceil(a/b); i.e. rounded towards positive infinity"""
	return 1 + (a-1)//b


class BlockStorage(object):
	"""
	File storing fixed-size blocks of data.
	"""

	def __init__(this, f, block_size):
		this.f = f
		this.block_size = block_size
		this.block_map = array.array('l')
		this.zero_block = b"\x00"*this.block_size
		this._reconstruct_free_map()

	def save_state(this, f):
		f.truncate(0)
		f.seek(0)
		f.write(b"BLK2")

		# Using zlib here is mainly for obfuscating information on the
		# total size of sparse files. The size of the map file will
		# correlate with the amount of downloaded data, but
		# compression reduces its correlation with the total size of
		# the file.
		block_map_data = zlib.compress(this.block_map.tobytes(), 9)
		f.write(struct.pack('<QQ', this.block_size, len(block_map_data)))
		f.write(block_map_data)

	@classmethod
	def restore_state(cls, f, state_file):
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
		this.f = f
		this.block_size = block_size
		this.block_map = block_map
		this.zero_block = b"\x00"*this.block_size
		this._reconstruct_free_map()
		return this.

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
			this.f.seek(this.block_size * block_idx)
			block = this.f.read(this.block_size)
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
				this.f.seek(0, 2)
				pos = this.f.tell()
				if pos > this.block_size * block_idx + len(data):
					data += b"\x00" * (this.block_size - len(data))

			this.f.seek(this.block_size * block_idx)
			this.f.write(data)

	def truncate(this, num_blocks):
		this.block_map = this.block_map[:num_blocks]

		end_block = 0
		if this.block_map:
			end_block = max(0, max(this.block_map) + 1)
		this.f.truncate(this.block_size * end_block)
		this._truncate_free_map(end_block)


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
		return this.

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


def block_range(offset, length, block_size, last_pos=None):
	"""
	Get the blocks that overlap with data range [offset, offset+length]

	Parameters
	----------
	offset, length : int
		Range specification
	last_pos : int, optional
		End-of-file position. If the data range goes over the end of the file,
		the last block is the last block in `mid`, and `end` is None.

	Returns
	-------
	start : (idx, start_pos, end_pos) or None
		Partial block at the beginning; block[start_pos:end_pos] has the data. If missing: None
	mid : (start_idx, end_idx)
		Range [start_idx, end_idx) of full blocks in the middle. If missing: None
	end : (idx, end_pos)
		Partial block at the end; block[:end_pos] has the data. If missing: None

	"""
	if last_pos is not None:
		length = max(min(last_pos - offset, length), 0)

	if length == 0:
		return None, None, None

	start_block, start_pos = divmod(offset, block_size)
	end_block, end_pos = divmod(offset + length, block_size)

	if last_pos is not None:
		if offset + length == last_pos and end_pos > 0:
			end_block += 1
			end_pos = 0

	if start_block == end_block:
		if start_pos == end_pos:
			return None, None, None
		return (start_block, start_pos, end_pos), None, None

	mid = None

	if start_pos == 0:
		start = None
		mid = (start_block, end_block)
	else:
		start = (start_block, start_pos, block_size)
		if start_block+1 < end_block:
			mid = (start_block+1, end_block)

	if end_pos == 0:
		end = None
	else:
		end = (end_block, end_pos)

	return start, mid, end

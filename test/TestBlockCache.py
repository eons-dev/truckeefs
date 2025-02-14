import os
import tempfile
import shutil
import random
import threading
import array

from StandardTestFixture import StandardTestFixture

from libtruckeefs import BlockCachedFile, BlockStorage, block_range, ceildiv
from libtruckeefs import FileOnDisk


class TestBlockCachedFile(StandardTestFixture):

	@classmethod
	def Constructor(this):
		super().Constructor()
		this.cache_data = os.urandom(656)

	def _do_rw(this, blockfile, offset, length_or_data):
		if isinstance(length_or_data, bytes):
			write = True
			data = length_or_data
			length = len(data)
		else:
			write = False
			length = length_or_data

		x_offset = 0
		x_read_offset = x_offset
		x_data = []

		while True:
			if write:
				pos = blockfile.pre_write(offset, length)
			else:
				pos = blockfile.pre_read(offset, length)

			if pos is None:
				# cache ready
				if write:
					return blockfile.write(offset, data)
				else:
					return blockfile.read(offset, length)
			else:
				# cache not ready -- fill it in a purposefully dodgy way in small pieces
				c_offset, c_length = pos
				if c_offset > x_offset + 23 or c_offset < x_offset:
					x_offset = (c_offset//13)*13
					x_read_offset = x_offset
					x_data = []
				x_data.append(this.cache_data[x_read_offset:x_read_offset+13])
				x_read_offset += 13
				x_offset, x_data = blockfile.receive_cached_data(x_offset, x_data)

	def _do_read(this, blockfile, offset, length):
		return this._do_rw(blockfile, offset, length)

	def _do_write(this, blockfile, offset, data):
		return this._do_rw(blockfile, offset, data)

	def test_basics(this):
		tmpf = tempfile.TemporaryFile()
		f = BlockCachedFile(tmpf, len(this.cache_data), block_size=7)
		data = this._do_read(f, 137, 91)
		this.assert_equal(data, this.cache_data[137:137+91])
		this._do_write(f, 131, b"a"*31)
		data = this._do_read(f, 130, 91)
		this.assert_equal(data[0], this.cache_data[130])
		this.assert_equal(data[1:32], b"a"*31)
		this.assert_equal(data[32:], this.cache_data[162:221])
		f.close()

	def _do_random_rw(this, f, sim_data, file_size, max_file_size, count):
		for j in range(count):
			a = random.randint(0, max_file_size)
			b = random.randint(0, max_file_size)
			if a > b:
				a, b = b, a

			b = min(a + 39, b)

			if j % 2 == 0:
				# read op
				a = min(a, file_size-1)
				b = min(b, file_size)
				block = this._do_read(f, a, b - a)
				this.assert_equal(block, sim_data[a:b].tobytes())
			else:
				# write op
				if j % 31 == 0:
					# sometimes write zeros
					block = b"\x00" * (b - a)
				else:
					# at other times, random data
					block = os.urandom(b - a)
				sim_data[a:b] = array.array('b', block)
				this._do_write(f, a, block)
				file_size = max(file_size, a + len(block))

		return file_size

	def test_random_rw(this):
		tmpf = tempfile.TemporaryFile()

		file_size = len(this.cache_data)
		max_file_size = 2*file_size

		random.seed(1234)

		for k in range(3):
			file_size = len(this.cache_data)
			sim_data = array.array('b', this.cache_data + b"\x00"*(max_file_size-file_size))
			tmpf = tempfile.TemporaryFile()
			f = BlockCachedFile(tmpf, file_size, block_size=7)
			this._do_random_rw(f, sim_data, file_size, max_file_size, count=5000)

		f.close()

	def test_pad_file(this):
		tmpf = tempfile.TemporaryFile()
		f = BlockCachedFile(tmpf, 19, block_size=7)

		# Check that padding the file does not leave spurious
		# unnallocated blocks
		start_idx = ceildiv(f.cache_size, f.block_size)
		for k in [3*7, 3*7+1, 3*7+3, 540, 1090]:
			f._pad_file(k)
			assert -1 not in f.storage.block_map[start_idx:], k

	def test_write_past_end(this):
		# Check that write-past-end has POSIX semantics
		tmpf = tempfile.TemporaryFile()
		f = BlockCachedFile(tmpf, len(this.cache_data), block_size=7)

		this._do_write(f, len(this.cache_data) + 5, b"a"*3)

		data = this._do_read(f, len(this.cache_data) - 1, 1+5+3)
		this.assert_equal(data, this.cache_data[-1:] + b"\x00"*5 + b"a"*3)
		f.close()

	def test_truncate(this):
		# Check that truncate() works as expected
		tmpf = tempfile.TemporaryFile()
		f = BlockCachedFile(tmpf, len(this.cache_data), block_size=7)

		this._do_write(f, 0, b"b"*1237)
		this.assert_equal(this._do_read(f, 0, 15), b"b"*15)
		f.truncate(7)
		this.assert_equal(this._do_read(f, 0, 15), b"b"*7)
		f.truncate(0)
		this.assert_equal(this._do_read(f, 0, 15), b"")

		this._do_write(f, 0, b"b"*1237)
		this.assert_equal(this._do_read(f, 1200, 15), b"b"*15)
		f.truncate(1200 + 7)
		this.assert_equal(this._do_read(f, 1200, 15), b"b"*7)
		f.truncate(1200 + 0)
		this.assert_equal(this._do_read(f, 1200, 15), b"")
		f.truncate(1200 - 20)
		this.assert_equal(this._do_read(f, 1200, 15), b"")
		f.close()

	def test_on_top_cryptfile(this):
		tmpf = FileOnDisk(this.file_name, key=b"a"*32, mode='w+b')
		f = BlockCachedFile(tmpf, len(this.cache_data), block_size=37)

		this._do_write(f, 0, b"b"*1237)
		this.assert_equal(this._do_read(f, 0, 15), b"b"*15)
		f.truncate(7)
		this.assert_equal(this._do_read(f, 0, 15), b"b"*7)
		f.truncate(0)
		this.assert_equal(this._do_read(f, 0, 15), b"")
		f.close()

	def test_save_state(this):
		file_size = len(this.cache_data)
		max_file_size = 2*file_size
		sim_data = array.array('b', this.cache_data + b"\x00"*(max_file_size-file_size))

		# Do random I/O on a file
		tmpf = FileOnDisk(this.file_name, key=b"a"*32, mode='w+b')
		f = BlockCachedFile(tmpf, file_size, block_size=7)
		file_size = this._do_random_rw(f, sim_data, file_size, max_file_size, count=17)

		# Save state
		state_file = FileOnDisk(this.file_name + '.state', key=b"b"*32, mode='w+b')
		f.save_state(state_file)
		state_file.close()
		f.close()

		# Restore state
		state_file = FileOnDisk(this.file_name + '.state', key=b"b"*32, mode='rb')
		tmpf = FileOnDisk(this.file_name, key=b"a"*32, mode='r+b')
		f = BlockCachedFile.restore_state(tmpf, state_file)
		state_file.close()

		# More random I/O
		for k in range(3):
			file_size = this._do_random_rw(f, sim_data, file_size, max_file_size, count=15)
		f.close()

	def test_get_file(this):
		tmpf = tempfile.TemporaryFile()
		f = BlockCachedFile(tmpf, len(this.cache_data), block_size=37)
		this._do_write(f, 17, b'abc'*12)

		f2 = f.get_file()
		f2.seek(17)
		this.assert_equal(f2.read(3*12), b'abc'*12)
		this.assert_equal(f2.read(7), this.cache_data[17+3*12:17+3*12+7])

		this._do_read(f, 0, len(this.cache_data))
		this.assert_equal(f2.read(), this.cache_data[17+3*12+7:])


class TestBlockStorage(StandardTestFixture):

	@classmethod
	def Constructor(this):
		pass

	@classmethod
	def Destructor(this):
		pass

	def test_basic(this):
		tmpf = tempfile.TemporaryFile()
		statef = tempfile.TemporaryFile()

		f = BlockStorage(tmpf, block_size=7)

		# Missing blocks
		this.assert_raises(KeyError, f.__getitem__, 0)
		this.assert_raises(KeyError, f.__getitem__, 1)

		# Basic block storage
		block_1 = b"\x01"*7
		block_2 = b"\x02"*7

		f[0] = block_1
		f[1] = block_2

		this.assert_equal(f[0], block_1)
		this.assert_equal(f[1], block_2)

		# Sparse zero blocks
		f[1] = None
		this.assert_equal(f[1], b"\x00"*7)
		f[1] = block_2

		# Implicit null padding
		f[2] = b'abc'
		this.assert_equal(f[2], b"abc\x00\x00\x00\x00")
		f[3] = b'cba'
		this.assert_equal(f[3], b"cba\x00\x00\x00\x00")

		# Save-restore cycle
		f[2] = block_2
		f.save_state(statef)
		statef.seek(0)
		f = BlockStorage.restore_state(tmpf, statef)
		this.assert_equal(f[0], block_1)
		this.assert_equal(f[2], block_2)


class TestBlockRange(StandardTestFixture):
	
	@classmethod
	def Constructor(this):
		pass

	@classmethod
	def Destructor(this):
		pass

	def _check_block_slice(this, data, offset, length, block_size, last_pos=None):
		"""
		Check block_range invariant
		"""

		start, mid, end = block_range(offset, length, block_size, last_pos)
		blocks = [data[j:j+block_size] for j in range(0, len(data), block_size)]

		new_data = b""
		if start is not None:
			new_data += blocks[start[0]][start[1]:start[2]]
		if mid is not None:
			for idx in range(*mid):
				new_data += blocks[idx]
		if end is not None:
			new_data += blocks[end[0]][:end[1]]

		a = new_data
		b = data[offset:offset+length]
		this.assert_equal(a, b,
					 repr((a, b, offset, length, block_size, last_pos, start, mid, end)))

	def test_basics(this):
		data = os.urandom(31)

		for offset in range(0, 35):
			for length in range(0, 35):
				for block_size in [1, 2, 3, 5, 7, 11]:
					this._check_block_slice(data, offset, length, block_size,
											last_pos=len(data))

	def test_ceildiv(this):
		for k in range(100):
			for p in [3, 8, 17]:
				b, remainder = divmod(k, p)
				if remainder > 0:
					b += 1

				a = ceildiv(k, p)
				this.assert_equal(a, b, repr((a, b, k, p)))

		this.assert_raises(ZeroDivisionError, ceildiv, 5, 0)

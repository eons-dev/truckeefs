import os
import shutil
import random
import threading

from StandardTestFixture import StandardTestFixture

from libtruckeefs import FileOnDisk

class TestFileOnDisk(StandardTestFixture):


	def test_create(this):
		# Test file creation in the different modes
		key = b'a'*32

		f = FileOnDisk(this.file_name, key=key, mode='w+b', block_size=32)
		f.write(b'foo')
		f.seek(0)
		this.assert_equal(f.read(), b'foo')
		f.close()

		f = FileOnDisk(this.file_name, key=key, mode='rb', block_size=32)
		this.assert_equal(f.read(), b'foo')
		this.assert_raises(IOError, f.write, b'bar')
		f.close()

		f = FileOnDisk(this.file_name, key=key, mode='r+b', block_size=32)
		this.assert_equal(f.read(), b'foo')
		f.write(b'bar')
		this.assert_equal(f.read(), b'')
		f.seek(0)
		this.assert_equal(f.read(), b'foobar')
		f.close()

		f = FileOnDisk(this.file_name, key=key, mode='w+b', block_size=32)
		f.seek(0)
		this.assert_equal(f.read(), b'')
		f.close()

	def test_random_rw(this):
		file_name = this.file_name
		file_size = 1000000
		test_data = os.urandom(file_size)
		key = b"a"*32

		f = FileOnDisk(file_name, key=key, mode='w+b')
		f.write(test_data)
		f.close()

		f = FileOnDisk(this.file_name, key=key, mode='r+b')

		random.seed(1234)

		for j in range(200):
			a = random.randint(0, file_size)
			b = random.randint(0, file_size)
			if a > b:
				a, b = b, a

			if random.randint(0, 1) == 0:
				# read op
				f.seek(a)
				data = f.read(b - a)
				this.assert_equal(data, test_data[a:b])
			else:
				# write op
				f.seek(a)
				f.write(test_data[a:b])

	def test_write_past_end(this):
		# Check that write-past-end has POSIX semantics
		key = b"a"*32
		with FileOnDisk(this.file_name, key=key, mode='w+b', block_size=32) as f:
			f.seek(12)
			f.write(b"abba")
			f.seek(0)
			this.assert_equal(f.read(), b"\x00"*12 + b"abba")

	def test_seek(this):
		# Check that seeking works as expected
		key = b"a"*32
		with FileOnDisk(this.file_name, key=key, mode='w+b', block_size=32) as f:
			f.seek(2, 0)
			f.write(b"a")
			f.seek(-2, 2)
			this.assert_equal(f.read(2), b"\x00a")
			f.seek(0, 2)
			f.write(b"c")
			f.seek(-2, 1)
			this.assert_equal(f.read(2), b"ac")

	def test_truncate(this):
		# Check that truncate() works as expected
		key = b"a"*32
		f = FileOnDisk(this.file_name, key=key, mode='w+b', block_size=32)
		f.write(b"b"*1237)
		f.truncate(15)
		f.seek(0)
		this.assert_equal(f.read(), b"b"*15)
		f.truncate(31)
		f.seek(0)
		this.assert_equal(f.read(), b"b"*15 + b"\x00"*16)
		f.truncate(0)
		f.seek(0)
		this.assert_equal(len(f.read()), 0)
		f.close()

	def test_locking(this):
		# Check that POSIX locking serializes access to the file

		key = b"a"*32
		last_data = [None]

		def run():
			f = FileOnDisk(this.file_name, key=key, mode='r+b', block_size=32)
			f.truncate(0)
			last_data[0] = os.urandom(16)
			f.write(last_data[0])
			f.close()

		f = FileOnDisk(this.file_name, key=key, mode='w+b', block_size=32)
		last_data[0] = os.urandom(16)
		f.write(last_data[0])

		threads = [threading.Thread(target=run) for j in range(32)]
		for t in threads:
			t.start()

		f.close()

		for t in threads:
			t.join()

		f = FileOnDisk(this.file_name, key=key, mode='rb', block_size=32)
		data = f.read()
		f.close()

		this.assert_equal(data, last_data[0])

	def test_data_sizes(this):
		key = b"a"*32

		for data_size in range(5*32):
			data = os.urandom(data_size)

			f = FileOnDisk(this.file_name, key=key, mode='w+b', block_size=32)
			f.write(data)
			f.close()

			f = FileOnDisk(this.file_name, key=key, mode='rb', block_size=32)
			data2 = f.read()
			f.close()

			this.assert_equal(data2, data, repr((data, data_size)))

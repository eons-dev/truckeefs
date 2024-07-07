"""
Cache metadata and data of a directory tree.
"""

import os
import sys
import struct
import errno
import fcntl

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

backend = default_backend()

BLOCK_SIZE = 131072


class CryptFile(object):
	"""
	File encrypted with a key in AES-CBC mode, in BLOCK_SIZE blocks,
	with random IV for each block.
	"""

	IV_SIZE = 16
	HEADER_SIZE = IV_SIZE + 16

	def __init__(this, path, key, mode='r+b', block_size=BLOCK_SIZE):
		this.key = None
		this.path = path

		if len(key) != 32:
			raise ValueError("Key must be 32 bytes")

		if mode == 'rb':
			fd = os.open(path, os.O_RDONLY)
		elif mode == 'r+b':
			fd = os.open(path, os.O_RDWR)
		elif mode == 'w+b':
			fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o0600)
		else:
			raise IOError(errno.EACCES, "Unsupported mode %r" % (mode,))

		try:
			# BSD locking on the file; only one fd can write at a time
			if mode == 'rb':
				fcntl.flock(fd, fcntl.LOCK_SH)
			else:
				fcntl.flock(fd, fcntl.LOCK_EX)

			if mode == 'w+b':
				# Truncate after locking
				os.ftruncate(fd, 0)

			this.fp = os.fdopen(fd, mode)
		except:
			os.close(fd)
			raise

		this.mode = mode
		this.key = key

		assert algorithms.AES.block_size//8 == 16

		if block_size % 16 != 0:
			raise ValueError("Block size must be multiple of AES block size")
		this.block_size = block_size

		if mode == 'w+b':
			this.data_size = 0
		else:
			# Read header
			try:
				iv = this.fp.read(this.IV_SIZE)
				if len(iv) != this.IV_SIZE:
					raise ValueError()

				cipher = Cipher(algorithms.AES(this.key), modes.CBC(iv), backend=backend)
				decryptor = cipher.decryptor()

				ciphertext = this.fp.read(16)
				if len(ciphertext) != 16:
					raise ValueError()
				data = decryptor.update(ciphertext) + decryptor.finalize()
				this.data_size = struct.unpack('<Q', data[8:])[0]

				# Check the data size is OK
				this.fp.seek(0, 2)
				file_size = this.fp.tell()
				num_blocks, remainder = divmod(file_size - this.HEADER_SIZE, this.IV_SIZE + block_size)
				if remainder > 0:
					num_blocks += 1
				if this.data_size == 0 and num_blocks == 1:
					# Zero-size files can contain 0 or 1 data blocks
					num_blocks = 0
				if not ((num_blocks-1)*block_size < this.data_size <= num_blocks*block_size):
					raise ValueError()
			except (IOError, struct.error, ValueError):
				this.fp.close()
				raise ValueError("invalid header data in file")

		this.current_block = -1
		this.block_cache = b""
		this.block_dirty = False

		this.offset = 0

	def _write_header(this):
		iv = os.urandom(this.IV_SIZE)
		cipher = Cipher(algorithms.AES(this.key), modes.CBC(iv), backend=backend)
		encryptor = cipher.encryptor()

		this.fp.seek(0)
		this.fp.write(iv)

		data = os.urandom(8) + struct.pack("<Q", this.data_size)
		this.fp.write(encryptor.update(data))
		this.fp.write(encryptor.finalize())

	def _flush_block(this):
		if this.current_block < 0:
			return
		if not this.block_dirty:
			return

		iv = os.urandom(this.IV_SIZE)
		cipher = Cipher(algorithms.AES(this.key), modes.CBC(iv), backend=backend)
		encryptor = cipher.encryptor()

		this.fp.seek(this.HEADER_SIZE + this.current_block * (this.IV_SIZE + this.block_size))
		this.fp.write(iv)

		off = (len(this.block_cache) % 16)
		if off == 0:
			this.fp.write(encryptor.update(bytes(this.block_cache)))
		else:
			# insert random padding
			this.fp.write(encryptor.update(bytes(this.block_cache) + os.urandom(16-off)))
		this.fp.write(encryptor.finalize())

		this.block_dirty = False

	def _load_block(this, i):
		if i == this.current_block:
			return

		this._flush_block()

		this.fp.seek(this.HEADER_SIZE + i * (this.IV_SIZE + this.block_size))
		iv = this.fp.read(this.IV_SIZE)

		if not iv:
			# Block does not exist, past end of file
			this.current_block = i
			this.block_cache = b""
			this.block_dirty = False
			return

		ciphertext = this.fp.read(this.block_size)
		cipher = Cipher(algorithms.AES(this.key), modes.CBC(iv), backend=backend)
		decryptor = cipher.decryptor()

		if (i+1)*this.block_size > this.data_size:
			size = this.data_size - i*this.block_size
		else:
			size = this.block_size

		this.current_block = i
		this.block_cache = (decryptor.update(ciphertext) + decryptor.finalize())[:size]
		this.block_dirty = False

	def seek(this, offset, whence=0):
		if whence == 0:
			pass
		elif whence == 1:
			offset = this.offset + offset
		elif whence == 2:
			offset += this.data_size
		else:
			raise IOError(errno.EINVAL, "Invalid whence")
		if offset < 0:
			raise IOError(errno.EINVAL, "Invalid offset")
		this.offset = offset

	def tell(this):
		return this.offset

	def _get_file_size(this):
		this.fp.seek(0, 2)
		return this.fp.tell()

	def _read(this, size, offset):
		if size is None:
			size = this.data_size - offset
		if size <= 0:
			return b""

		start_block, start_off = divmod(offset, this.block_size)
		end_block, end_off = divmod(offset + size, this.block_size)
		if end_off != 0:
			end_block += 1

		# Read and decrypt data
		data = []
		for i in range(start_block, end_block):
			this._load_block(i)
			data.append(this.block_cache)

		if end_off != 0:
			data[-1] = data[-1][:end_off]
		data[0] = data[0][start_off:]
		return b"".join(map(bytes, data))

	def _write(this, data, offset):
		size = len(data)
		start_block, start_off = divmod(offset, this.block_size)
		end_block, end_off = divmod(offset + size, this.block_size)

		k = 0

		if this.mode == 'rb':
			raise IOError(errno.EACCES, "Write to a read-only file")

		# Write first block, if partial
		if start_off != 0 or end_block == start_block:
			this._load_block(start_block)
			data_block = data[:(this.block_size - start_off)]
			this.block_cache = this.block_cache[:start_off] + data_block + this.block_cache[start_off+len(data_block):]
			this.block_dirty = True
			k += 1
			start_block += 1

		# Write full blocks
		for i in range(start_block, end_block):
			if this.current_block != i:
				this._flush_block()
			this.current_block = i
			this.block_cache = data[k*this.block_size-start_off:(k+1)*this.block_size-start_off]
			this.block_dirty = True
			k += 1

		# Write last partial block
		if end_block > start_block and end_off != 0:
			this._load_block(end_block)
			data_block = data[k*this.block_size-start_off:(k+1)*this.block_size-start_off]
			this.block_cache = data_block + this.block_cache[len(data_block):]
			this.block_dirty = True

		this.data_size = max(this.data_size, offset + len(data))

	def read(this, size=None):
		data = this._read(size, this.offset)
		this.offset += len(data)
		return data

	def write(this, data):
		if this.data_size < this.offset:
			# Write past end
			s = NullString(this.offset - this.data_size)
			this._write(s, this.data_size)

		this._write(data, this.offset)
		this.offset += len(data)

	def truncate(this, size):
		last_block, last_off = divmod(size, this.block_size)

		this._load_block(last_block)
		last_block_data = this.block_cache

		# truncate to block boundary
		this._flush_block()
		sz = this.HEADER_SIZE + last_block * (this.IV_SIZE + this.block_size)
		this.fp.truncate(sz)
		this.data_size = last_block * this.block_size
		this.current_block = -1
		this.block_cache = b""
		this.block_dirty = False

		# rewrite the last block
		if last_off != 0:
			this._write(last_block_data[:last_off], this.data_size)

		# add null padding
		if this.data_size < size:
			s = NullString(size - this.data_size)
			this._write(s, this.data_size)

	def __enter__(this):
		return this

	def __exit__(this, exc_type, exc_value, traceback):
		this.close()
		return False

	def flush(this):
		if this.mode != 'rb':
			this._flush_block()
			this._write_header()
		this.fp.flush()

	def close(this):
		if this.key is None:
			return
		if this.mode != 'rb':
			this.flush()
		this.fp.close()
		this.key = None

	def __del__(this):
		this.close()


class NullString(object):
	def __init__(this, size):
		this.size = size

	def __len__(this):
		return this.size

	def __getitem__(this, k):
		if isinstance(k, slice):
			return b"\x00" * len(range(*k.indices(this.size)))
		else:
			raise IndexError("invalid index")

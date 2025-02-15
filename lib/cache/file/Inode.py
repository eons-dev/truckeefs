"""
lib/cache/file/Inode.py

Purpose:
Implements the logical file on disk (the inode) for cached files. It manages file metadata, caching state, and read/write operations (via block caching).

Place in Architecture:
Central to the caching subsystem for files. It interacts with both the FileOnDisk (or CryptFile originally) layer and the block cache. It also ties into remote operations (upload/download) via the Tahoe API.

Interface:

	__init__(cachedb, upath, io, filecap, persistent=False): Loads or creates the cached file, sets up encryption keys (or in our revised version, plain keys), block cache, etc.
	_load_info(upath, io, iscap=False) / _save_info(): Loads/saves file metadata.
	is_fresh(lifetime): Checks if cached metadata is fresh.
	incref()/decref(): Manage reference counts.
	read(io, offset, length), write(io, offset, data): Perform I/O using the block cache.
	truncate(size): Adjusts file size.
	upload(io, parent_cap): Buffers the file and uploads it via the Tahoe API.
	unlink(): Removes cached files from disk.

TODOs/FIXMEs:

	(Minor) Review or clarify the metadata fields and error conditions.
	(If using encryption, update for plain I/O—this file may need adjustments if switching from Crypt to FileOnDisk.)
"""

class CachedFileInode(object):
	"""
	Logical file on-disk. There should be only a single CachedFileInode
	instance is per each logical file.
	"""

	def __init__(this, cachedb, upath, io, filecap, persistent=False):
		this.upath = upath
		this.closed = False
		this.refcnt = 0
		this.persistent = persistent
		this.invalidated = False

		# Use per-file keys for different files, for safer fallback
		# in the extremely unlikely event of SHA512 hash collisions
		filename, key = cachedb.GetFileNameAndKey(upath)
		filename_state, key_state = cachedb.GetFileNameAndKey(upath, b'state')
		filename_data, key_data = cachedb.GetFileNameAndKey(upath, b'data')

		this.lock = threading.RLock()
		this.cache_lock = threading.RLock()
		this.dirty = False
		this.file = None
		this.f_state = None
		this.f_data = None

		this.stream_f = None
		this.stream_offset = 0
		this.stream_data = []

		open_complete = False

		try:
			if filecap is None:
				# Create new file
				raise ValueError()

			# Reuse cached metadata
			this.file = FileOnDisk(filename, key=key, mode='r+b')
			this.info = json_zlib_load(this.file)

			if persistent:
				# Reuse cached data
				this.f_state = FileOnDisk(filename_state, key=key_state, mode='r+b')
				this.f_data = FileOnDisk(filename_data, key=key_data, mode='r+b')
				this.block_cache = BlockCachedFile.restore_state(this.f_data, this.f_state)
				open_complete = True
		except (IOError, OSError, ValueError):
			open_complete = False
			if this.file is not None:
				this.file.close()
				this.file = None
			if this.f_state is not None:
				this.f_state.close()
			if this.f_data is not None:
				this.f_data.close()

		if not open_complete:
			if this.file is None:
				this.file = FileOnDisk(filename, key=key, mode='w+b')
				try:
					if filecap is not None:
						this._load_info(filecap, io, iscap=True)
					else:
						this.info = ['file', {'size': 0}]
						this.dirty = True
				except IOError as err:
					os.unlink(filename)
					this.file.close()
					raise

			# Create a data file
			this.f_data = FileOnDisk(filename_data, key=key_data, mode='w+b')

			# Block cache on top of data file
			this.block_cache = BlockCachedFile(this.f_data, this.info[1]['size'])

			# Block data state file
			this.f_state = FileOnDisk(filename_state, key=key_state, mode='w+b')

		os.utime(this.file.path, None)
		os.utime(this.f_data.path, None)
		os.utime(this.f_state.path, None)

	def _load_info(this, upath, io, iscap=False):
		try:
			this.info = io.get_info(upath, iscap=iscap)
		except (HTTPError, IOError, ValueError) as err:
			if isinstance(err, HTTPError) and err.code == 404:
				raise IOError(errno.ENOENT, "no such file")
			raise IOError(errno.EREMOTEIO, "failed to retrieve information")
		this._save_info()

	def _save_info(this):
		this.file.truncate(0)
		this.file.seek(0)
		if 'retrieved' not in this.info[1]:
			this.info[1]['retrieved'] = time.time()
		json_zlib_dump(this.info, this.file)

	def is_fresh(this, lifetime):
		if 'retrieved' not in this.info[1]:
			return True
		return (this.info[1]['retrieved'] + lifetime >= time.time())

	def incref(this):
		with this.cache_lock:
			this.refcnt += 1

	def decref(this):
		with this.cache_lock:
			this.refcnt -= 1
			if this.refcnt <= 0:
				this.close()

	def close(this):
		with this.cache_lock, this.lock:
			if not this.closed:
				if this.stream_f is not None:
					this.stream_f.close()
					this.stream_f = None
					this.stream_data = []
				this.f_state.seek(0)
				this.f_state.truncate(0)
				this.block_cache.save_state(this.f_state)
				this.f_state.close()
				this.block_cache.close()
				this.file.close()

				if not this.persistent and this.upath is not None and not this.invalidated:
					os.unlink(this.f_state.path)
					os.unlink(this.f_data.path)
			this.closed = True

	def _do_rw(this, io, offset, length_or_data, write=False, no_result=False):
		if write:
			data = length_or_data
			length = len(data)
		else:
			length = length_or_data

		while True:
			with this.cache_lock:
				if write:
					pos = this.block_cache.pre_write(offset, length)
				else:
					pos = this.block_cache.pre_read(offset, length)

				if pos is None:
					# cache ready
					if no_result:
						return None
					elif write:
						return this.block_cache.write(offset, data)
					else:
						return this.block_cache.read(offset, length)

			# cache not ready -- fill it up
			with this.lock:
				try:
					c_offset, c_length = pos

					if this.stream_f is not None and (this.stream_offset > c_offset or
													  c_offset >= this.stream_offset + 3*131072):
						this.stream_f.close()
						this.stream_f = None
						this.stream_data = []

					if this.stream_f is None:
						this.stream_f = io.get_content(this.info[1]['ro_uri'], c_offset, iscap=True)
						this.stream_offset = c_offset
						this.stream_data = []

					read_offset = this.stream_offset
					read_bytes = sum(len(x) for x in this.stream_data)
					while read_offset + read_bytes < c_offset + c_length:
						block = this.stream_f.read(131072)

						if not block:
							this.stream_f.close()
							this.stream_f = None
							this.stream_data = []
							break

						this.stream_data.append(block)
						read_bytes += len(block)

						with this.cache_lock:
							this.stream_offset, this.stream_data = this.block_cache.receive_cached_data(
								this.stream_offset, this.stream_data)
				except (HTTPError, IOError) as err:
					if this.stream_f is not None:
						this.stream_f.close()
					this.stream_f = None
					raise IOError(errno.EREMOTEIO, "I/O error: %s" % (str(err),))

	def get_size(this):
		with this.cache_lock:
			return this.block_cache.get_size()

	def get_attr(this):
		return dict(type='file', size=this.get_size())

	def read(this, io, offset, length):
		return this._do_rw(io, offset, length, write=False)

	def write(this, io, offset, data):
		"""
		Write data to file. If *offset* is None, it means append.
		"""
		with this.lock:
			if len(data) > 0:
				this.dirty = True
				if offset is None:
					offset = this.get_size()
				this._do_rw(io, offset, data, write=True)

	def truncate(this, size):
		with this.cache_lock, this.lock:
			if size != this.block_cache.get_size():
				this.dirty = True
			this.block_cache.truncate(size)

	def _buffer_whole_file(this, io):
		with this.cache_lock:
			this._do_rw(io, 0, this.block_cache.get_size(), write=False, no_result=True)

	def upload(this, io, parent_cap=None):
		with this.cache_lock, this.lock:
			# Buffer all data
			this._buffer_whole_file(io)

			# Upload the whole file
			class Fwrapper(object):
				def __init__(this, block_cache):
					this.block_cache = block_cache
					this.size = block_cache.get_size()
					this.file = this.block_cache.get_file()
					this.file.seek(0)
				def __len__(this):
					return this.size
				def read(this, size):
					return this.file.read(size)

			if parent_cap is None:
				upath = this.upath
				iscap = False
			else:
				upath = parent_cap + "/" + ubasename(this.upath)
				iscap = True

			fw = Fwrapper(this.block_cache)
			try:
				filecap = io.put_file(upath, fw, iscap=iscap)
			except (HTTPError, IOError) as err:
				raise IOError(errno.EFAULT, "I/O error: %s" % (str(err),))

			this.info[1]['ro_uri'] = filecap
			this.info[1]['size'] = this.get_size()
			this._save_info()

			this.dirty = False

			return filecap

	def unlink(this):
		with this.cache_lock, this.lock:
			if this.upath is not None and not this.invalidated:
				os.unlink(this.file.path)
				os.unlink(this.f_state.path)
				os.unlink(this.f_data.path)
			this.upath = None
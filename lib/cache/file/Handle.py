class CachedFileHandle(object):
	"""
	Logical file handle. There may be multiple open file handles
	corresponding to the same logical file.
	"""

	direct_io = False
	keep_cache = False

	def __init__(this, upath, inode, flags):
		this.inode = inode
		this.inode.incref()
		this.lock = threading.RLock()
		this.flags = flags
		this.upath = upath

		this.writeable = (this.flags & (os.O_RDONLY | os.O_RDWR | os.O_WRONLY)) in (os.O_RDWR, os.O_WRONLY)
		this.readable = (this.flags & (os.O_RDONLY | os.O_RDWR | os.O_WRONLY)) in (os.O_RDWR, os.O_RDONLY)
		this.append = (this.flags & os.O_APPEND)

		if this.flags & os.O_ASYNC:
			raise IOError(errno.ENOTSUP, "O_ASYNC flag is not supported")
		if this.flags & os.O_DIRECT:
			raise IOError(errno.ENOTSUP, "O_DIRECT flag is not supported")
		if this.flags & os.O_DIRECTORY:
			raise IOError(errno.ENOTSUP, "O_DIRECTORY flag is not supported")
		if this.flags & os.O_SYNC:
			raise IOError(errno.ENOTSUP, "O_SYNC flag is not supported")
		if (this.flags & os.O_CREAT) and not this.writeable:
			raise IOError(errno.EINVAL, "O_CREAT without writeable file")
		if (this.flags & os.O_TRUNC) and not this.writeable:
			raise IOError(errno.EINVAL, "O_TRUNC without writeable file")
		if (this.flags & os.O_EXCL) and not this.writeable:
			raise IOError(errno.EINVAL, "O_EXCL without writeable file")
		if (this.flags & os.O_APPEND) and not this.writeable:
			raise IOError(errno.EINVAL, "O_EXCL without writeable file")

		if (this.flags & os.O_TRUNC):
			this.inode.truncate(0)

	def close(this):
		with this.lock:
			if this.inode is None:
				raise IOError(errno.EBADF, "Operation on a closed file")
			c = this.inode
			this.inode = None
			c.decref()

	def read(this, io, offset, length):
		with this.lock:
			if this.inode is None:
				raise IOError(errno.EBADF, "Operation on a closed file")
			if not this.readable:
				raise IOError(errno.EBADF, "File not readable")
			return this.inode.read(io, offset, length)

	def get_size(this):
		with this.lock:
			return this.inode.get_size()

	def write(this, io, offset, data):
		with this.lock:
			if this.inode is None:
				raise IOError(errno.EBADF, "Operation on a closed file")
			if not this.writeable:
				raise IOError(errno.EBADF, "File not writeable")
			if this.append:
				offset = None
			return this.inode.write(io, offset, data)

	def truncate(this, size):
		with this.lock:
			if this.inode is None:
				raise IOError(errno.EBADF, "Operation on a closed file")
			if not this.writeable:
				raise IOError(errno.EBADF, "File not writeable")
			return this.inode.truncate(size)
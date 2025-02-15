"""
lib/cache/file/Handle.py

Purpose:
Provides the implementation of a logical file handle that “wraps” a cached file inode. Multiple open handles may point to the same underlying inode.

Place in Architecture:
Part of the caching layer. This class is used by the FUSE operations to read from or write to a file. It delegates actual I/O to the underlying CachedFileInode.

Interface:

	__init__(upath, inode, flags): Initializes the handle (increments inode ref count, sets up locks, and validates open flags).
	close(): Closes the handle (decrements ref count).
	read(io, offset, length): Reads data from the inode.
	write(io, offset, data): Writes data to the inode.
	get_size(): Returns the file size.
	truncate(size): Truncates the file.

TODOs/FIXMEs:
None explicitly noted in this file.
"""

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
			inode = this.inode
			this.inode = None
			inode.decref()

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
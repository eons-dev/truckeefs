"""
lib/cache/dir/Handle.py

Purpose:
Implements the logical directory handle. This is used to access directory metadata in the cache.

Place in Architecture:
Part of the directory caching layer. It wraps a CachedDirInode and provides directory-specific operations for FUSE (e.g. listing contents).

Interface:

	__init__(upath, inode): Initializes by wrapping a CachedDirInode and incrementing its ref count.
	close(): Closes the handle (decrements the inode ref count).
	listdir(): Returns the list of directory entries.
	get_attr(): Retrieves attributes of the directory.
	get_child_attr(childname): Retrieves attributes for a specific child entry.

TODOs/FIXMEs:
None explicitly noted.
"""

class CachedDirHandle(object):
	"""
	Logical directory handle.
	"""

	def __init__(this, upath, inode):
		this.inode = inode
		this.inode.incref()
		this.lock = threading.RLock()
		this.upath = upath

	def close(this):
		with this.lock:
			if this.inode is None:
				raise IOError(errno.EBADF, "Operation on a closed dir")
			c = this.inode
			this.inode = None
			c.decref()

	def listdir(this):
		with this.lock:
			if this.inode is None:
				raise IOError(errno.EBADF, "Operation on a closed dir")
			return this.inode.listdir()

	def get_attr(this):
		with this.lock:
			if this.inode is None:
				raise IOError(errno.EBADF, "Operation on a closed dir")
			return this.inode.get_attr()

	def get_child_attr(this, childname):
		with this.lock:
			if this.inode is None:
				raise IOError(errno.EBADF, "Operation on a closed dir")
			return this.inode.get_child_attr(childname)

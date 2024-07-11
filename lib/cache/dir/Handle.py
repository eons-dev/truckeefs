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

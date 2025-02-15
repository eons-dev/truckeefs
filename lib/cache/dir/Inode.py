"""
lib/cache/dir/Inode.py

Purpose:
Represents a logical directory on disk (the inode for a directory) and manages its metadata, including child entries.

Place in Architecture:
A key component of the caching system for directory metadata. It is used when listing directories and looking up child attributes.

Interface:

	__init__(cachedb, upath, io, dircap=None): Loads or creates the cached directory metadata.
	_save_info(): Persists directory metadata to disk.
	is_fresh(lifetime): Checks if the metadata is still current.
	incref()/decref()/close(): Reference counting and cleanup.
	listdir(): Returns a list of children.
	get_attr(): Returns directory attributes.
	get_child_attr(childname): Returns attributes for a child entry (handles file vs. directory differentiation).
	unlink(): Removes the directory from the cache.
	cache_add_child(basename, cap, size): Adds or updates a child entry in the cache.
	cache_remove_child(basename): Removes a child entry.

TODOs/FIXMEs:

	There's a comment regarding the use of 'tahoe:linkcrtime' vs. 'linkmotime' for timestamps in get_child_attr.
"""

class CachedDirInode(object):
	"""
	Logical file on-disk directory. There should be only a single CachedDirInode
	instance is per each logical directory.
	"""

	def __init__(this, cachedb, upath, io, dircap=None):
		this.upath = upath
		this.closed = False
		this.refcnt = 0
		this.lock = threading.RLock()
		this.invalidated = False

		this.filename, this.key = cachedb.GetFileNameAndKey(upath)

		try:
			with FileOnDisk(this.filename, key=this.key, mode='rb') as file:
				this.info = json_zlib_load(file)
			os.utime(this.filename, None)
			return
		except (IOError, OSError, ValueError):
			pass

		file = FileOnDisk(this.filename, key=this.key, mode='w+b')
		try:
			if dircap is not None:
				this.info = io.get_info(dircap, iscap=True)
			else:
				this.info = io.get_info(upath)
			this.info[1]['retrieved'] = time.time()
			json_zlib_dump(this.info, file)
		except (HTTPError, IOError, ValueError):
			os.unlink(this.filename)
			raise IOError(errno.EREMOTEIO, "failed to retrieve information")
		finally:
			file.close()

	def _save_info(this):
		with FileOnDisk(this.filename, key=this.key, mode='w+b') as file:
			json_zlib_dump(this.info, file)

	def is_fresh(this, lifetime):
		return (this.info[1]['retrieved'] + lifetime >= time.time())

	def incref(this):
		with this.lock:
			this.refcnt += 1

	def decref(this):
		with this.lock:
			this.refcnt -= 1
			if this.refcnt <= 0:
				this.close()

	def close(this):
		with this.lock:
			this.closed = True

	def listdir(this):
		return list(this.info[1]['children'].keys())

	def get_attr(this):
		return dict(type='dir')

	def get_child_attr(this, childname):
		assert isinstance(childname, str)
		children = this.info[1]['children']
		if childname not in children:
			raise IOError(errno.ENOENT, "no such entry")

		info = children[childname]

		# tahoe:linkcrtime doesn't exist for entries created by "tahoe backup",
		# but explicit 'mtime' and 'ctime' do, so use them.
		ctime = info[1]['metadata'].get('tahoe', {}).get('linkcrtime')
		mtime = info[1]['metadata'].get('tahoe', {}).get('linkcrtime')   # should this be 'linkmotime'?
		if ctime is None:
			ctime = info[1]['metadata']['ctime']
		if mtime is None:
			mtime = info[1]['metadata']['mtime']

		if info[0] == 'dirnode':
			return dict(type='dir', 
						ro_uri=info[1]['ro_uri'],
						rw_uri=info[1].get('rw_uri'),
						ctime=ctime,
						mtime=mtime)
		elif info[0] == 'filenode':
			return dict(type='file',
						size=info[1]['size'],
						ro_uri=info[1]['ro_uri'],
						rw_uri=info[1].get('rw_uri'),
						ctime=ctime,
						mtime=mtime)
		else:
			raise IOError(errno.ENOENT, "invalid entry")

	def unlink(this):
		if this.upath is not None and not this.invalidated:
			os.unlink(this.filename)
		this.upath = None

	def cache_add_child(this, basename, cap, size):
		children = this.info[1]['children']

		if basename in children:
			info = children[basename]
		else:
			if cap is not None and cap.startswith('URI:DIR'):
				info = ['dirnode', {'metadata': {'tahoe': {'linkcrtime': time.time()}}}]
			else:
				info = ['filenode', {'metadata': {'tahoe': {'linkcrtime': time.time()}}}]

		if info[0] == 'dirnode':
			info[1]['ro_uri'] = cap
			info[1]['rw_uri'] = cap
		elif info[0] == 'filenode':
			info[1]['ro_uri'] = cap
			info[1]['size'] = size

		children[basename] = info
		this._save_info()

	def cache_remove_child(this, basename):
		children = this.info[1]['children']
		if basename in children:
			del children[basename]
			this._save_info()

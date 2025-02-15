"""
lib/fs/fsop/file/GetInode.py

Purpose:
Retrieves (or creates) the inode for a file, handling cache lookup, creation, and exclusive flags.

Place in Architecture:
Called by higher-level FS operations (like open) to ensure the correct inode is used for a file.

Interface:

	A function (decorated with @eons.kind(FSOp)) that takes upath, an I/O object, and flags (excl, creat, ttl) and returns a CachedFileInode.

TODOs/FIXMEs:
None explicitly noted.
"""

@eons.kind(FSOp)
def file_getinode(this, upath, io, excl=False, creat=False, ttl=None):
	if ttl is None:
		ttl = this.read_lifetime

	file = this.open_items.get(upath)

	if file is not None and not file.is_fresh(ttl):
		file = None
		this.invalidate(upath, shallow=True)

	if file is None:
		try:
			cap = this.LookupCap(upath, io, lifetime=ttl)
		except IOError as err:
			if err.errno == errno.ENOENT and creat:
				cap = None
			else:
				raise

		if excl and cap is not None:
			raise IOError(errno.EEXIST, "file already exists")
		if not creat and cap is None:
			raise IOError(errno.ENOENT, "file does not exist")

		file = CachedFileInode(
			this,
			upath,
			io,
			filecap=cap, 
			persistent=this.cache_data
		)
		this.open_items[upath] = file

		if cap is None:
			# new file: add to parent inode
			directory = this.open_dir(udirname(upath), io, lifetime=ttl)
			try:
				directory.inode.cache_add_child(ubasename(upath), None, size=0)
			finally:
				this.close_dir(directory)
		return file
	else:
		if excl:
			raise IOError(errno.EEXIST, "file already exists")
		if not isinstance(file, CachedFileInode):
			raise IOError(errno.EISDIR, "item is a directory")
		return file
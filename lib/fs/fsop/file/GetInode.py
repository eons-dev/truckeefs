
@eons.kind(FSOp)
def file_getinode(this, upath, io, excl=False, creat=False, lifetime=None):
	if lifetime is None:
		lifetime = this.read_lifetime

	f = this.open_items.get(upath)

	if f is not None and not f.is_fresh(lifetime):
		f = None
		this.invalidate(upath, shallow=True)

	if f is None:
		try:
			cap = this.LookupCap(upath, io, lifetime=lifetime)
		except IOError as err:
			if err.errno == errno.ENOENT and creat:
				cap = None
			else:
				raise

		if excl and cap is not None:
			raise IOError(errno.EEXIST, "file already exists")
		if not creat and cap is None:
			raise IOError(errno.ENOENT, "file does not exist")

		f = CachedFileInode(
			this,
			upath,
			io,
			filecap=cap, 
			persistent=this.cache_data
		)
		this.open_items[upath] = f

		if cap is None:
			# new file: add to parent inode
			d = this.open_dir(udirname(upath), io, lifetime=lifetime)
			try:
				d.inode.cache_add_child(ubasename(upath), None, size=0)
			finally:
				this.close_dir(d)
		return f
	else:
		if excl:
			raise IOError(errno.EEXIST, "file already exists")
		if not isinstance(f, CachedFileInode):
			raise IOError(errno.EISDIR, "item is a directory")
		return f
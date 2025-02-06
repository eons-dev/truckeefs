
@eons.kind(FSOp)
def file_upload(this, c, io):
	if isinstance(c, CachedFileHandle):
		c = c.inode

	if c.upath is not None and c.dirty:
		parent = this.open_dir(udirname(c.upath), io, lifetime=this.write_lifetime)
		try:
			parent_cap = parent.inode.info[1]['rw_uri']

			# Upload
			try:
				cap = c.upload(io, parent_cap=parent_cap)
			except:
				# Failure to upload --- need to invalidate parent
				# directory, since the file might not have been
				# created.
				this.invalidate(parent.upath, shallow=True)
				raise

			# Add in cache
			parent.inode.cache_add_child(ubasename(c.upath), cap, size=c.get_size())
		finally:
			this.close_dir(parent)
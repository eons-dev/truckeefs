
@eons.kind(FSOp)
def directory_make(this, upath, io):
	if upath == '':
		raise IOError(errno.EEXIST, "cannot re-mkdir root directory")

	# Check that parent exists
	parent = this.open_dir(udirname(upath), io, lifetime=this.write_lifetime)
	try:
		parent_cap = parent.inode.info[1]['rw_uri']

		# Check that the target does not exist
		try:
			parent.get_child_attr(ubasename(upath))
		except IOError as err:
			if err.errno == errno.ENOENT:
				pass
			else:
				raise
		else:
			raise IOError(errno.EEXIST, "directory already exists")

		# Invalidate cache
		this.invalidate(upath)

		# Perform operation
		upath_cap = parent_cap + '/' + ubasename(upath)
		try:
			cap = io.mkdir(upath_cap, iscap=True)
		except (HTTPError, IOError) as err:
			raise IOError(errno.EREMOTEIO, "remote operation failed: {0}".format(err))

		# Add in cache
		parent.inode.cache_add_child(ubasename(upath), cap, size=None)
	finally:
		this.close_dir(parent)
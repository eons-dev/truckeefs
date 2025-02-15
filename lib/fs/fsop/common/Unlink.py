"""
lib/fs/fsop/common/Unlink.py

Purpose:
Implements a generic FS operation for unlinking (removing) a filesystem object.

Place in Architecture:
Used by both file and directory unlink operations. It removes an inode from the cache and triggers remote deletion.

Interface:

	A function (decorated with @eons.kind(FSOp)) that takes upath, an I/O object, and a flag indicating whether it’s a directory.

TODOs/FIXMEs:
None explicitly noted.
"""

@eons.kind(FSOp)
def FSOpUnlink(this, upath, io, is_dir=False):
	if upath == '':
		raise IOError(errno.EACCES, "cannot unlink root directory")

	# Unlink in cache
	if is_dir:
		file = this.open_dir(upath, io, lifetime=this.write_lifetime)
	else:
		file = this.open_file(upath, io, 0, lifetime=this.write_lifetime)
	try:
		file.inode.unlink()
	finally:
		if is_dir:
			this.close_dir(file)
		else:
			this.close_file(file)

	# Perform unlink
	parent = this.open_dir(udirname(upath), io, lifetime=this.write_lifetime)
	try:
		parent_cap = parent.inode.info[1]['rw_uri']

		upath_cap = parent_cap + '/' + ubasename(upath)
		try:
			cap = io.delete(upath_cap, iscap=True)
		except (HTTPError, IOError) as err:
			if isinstance(err, HTTPError) and err.code == 404:
				raise IOError(errno.ENOENT, "no such file")
			raise IOError(errno.EREMOTEIO, "failed to retrieve information")

		# Remove from cache
		parent.inode.cache_remove_child(ubasename(upath))
	finally:
		this.close_dir(parent)
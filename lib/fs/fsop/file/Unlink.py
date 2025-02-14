"""
lib/fs/fsop/file/Unlink.py

Purpose:
Unlinks (removes) a file from the filesystem.

Place in Architecture:
Handles file deletion both in the local cache and by issuing a delete command via the Tahoe API.

Interface:

	A function (decorated with @eons.kind(FSOpUnlink)) that takes a file upath and an I/O object and performs the unlink operation.

TODOs/FIXMEs:
None explicitly noted.
"""

@eons.kind(FSOpUnlink)
def file_unlink(this, upath, io, is_dir=False):
	if upath == '':
		raise IOError(errno.EACCES, "cannot unlink root directory")

	# Unlink in cache
	if is_dir:
		f = this.open_dir(upath, io, lifetime=this.write_lifetime)
	else:
		f = this.open_file(upath, io, 0, lifetime=this.write_lifetime)
	try:
		f.inode.unlink()
	finally:
		if is_dir:
			this.close_dir(f)
		else:
			this.close_file(f)

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
"""
lib/fs/fsop/file/Open.py

Purpose:
Opens a file for reading or writing.

Place in Architecture:
Maps the FUSE open call to the underlying cache layer by returning a CachedFileHandle that wraps the inode.

Interface:

	A function (decorated with @eons.kind(FSOp)) that takes upath, an I/O object, flags, and ttl, and returns a CachedFileHandle.

TODOs/FIXMEs:
None explicitly noted.
"""

@eons.kind(FSOp)
def file_open(this, upath, io, flags, ttl=None):
	writeable = (flags & (os.O_RDONLY | os.O_RDWR | os.O_WRONLY)) in (os.O_RDWR, os.O_WRONLY)
	if writeable:
		# Drop file data cache before opening in write mode
		if upath not in this.open_items:
			this.invalidate(upath)

		# Limit e.g. parent directory lookup ttl
		if ttl is None:
			ttl = this.write_lifetime

	file = this.get_file_inode(
		upath,
		io,
		excl=(flags & os.O_EXCL),
		creat=(flags & os.O_CREAT),
		lifetime=ttl
	)
	return CachedFileHandle(upath, file, flags)
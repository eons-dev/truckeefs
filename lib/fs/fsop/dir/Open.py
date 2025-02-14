"""
lib/fs/fsop/dir/Open.py

Purpose:
Opens a directory for operations (e.g. reading its contents).

Place in Architecture:
Maps the FUSE opendir call to return a CachedDirHandle wrapping a directory inode.

Interface:

	A function (decorated with @eons.kind(FSOp)) that takes a directory upath and an I/O object and returns a CachedDirHandle.

TODOs/FIXMEs:
None explicitly noted.
"""

@eons.kind(FSOp)
def directory_open(this, upath, io, lifetime=None):
	f = this.get_dir_inode(upath, io, lifetime=lifetime)
	return CachedDirHandle(upath, f)
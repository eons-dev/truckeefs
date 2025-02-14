"""
lib/fs/fsop/dir/Close.py

Purpose:
Closes a directory handle.

Place in Architecture:
Similar to file_close but for directories, ensuring that cached directory inodes are properly released.

Interface:

	A function (decorated with @eons.kind(FSOp)) that takes a directory handle and finalizes its closure.

TODOs/FIXMEs:
None explicitly noted.
"""

@eons.kind(FSOp)
def directory_close(this, f):
	c = f.inode
	upath = f.upath
	f.close()
	if c.closed:
		if upath in this.open_items:
			del this.open_items[upath]
		this._restrict_size()
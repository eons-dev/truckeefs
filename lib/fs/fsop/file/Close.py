"""
lib/fs/fsop/file/Close.py

Purpose:
Closes a file handle, ensuring that reference counts are updated and the cache size is restricted if needed.

Place in Architecture:
Finalizes a file operation by releasing resources and updating the cache state.

Interface:

    A function (decorated with @eons.kind(FSOp)) that takes a file handle and performs cleanup.

TODOs/FIXMEs:
None explicitly noted.
"""

@eons.kind(FSOp)
def file_close(this, f):
	c = f.inode
	upath = f.upath
	f.close()
	if c.closed:
		if upath in this.open_items:
			del this.open_items[upath]
		this._restrict_size()
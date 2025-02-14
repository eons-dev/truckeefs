"""
lib/fs/fsop/common/GetAttributes.py

Purpose:
Implements the FS operation for obtaining attributes (akin to getattr) for a given inode.

Place in Architecture:
Part of the FUSE operation layer. This function is called to retrieve metadata for both files and directories.

Interface:

	A function (decorated with @eons.kind(FSOp)) that takes upath and an I/O object and returns a dictionary of attributes.

TODOs/FIXMEs:

	No explicit TODOs but review edge cases for newly created files.
"""

@eons.kind(FSOp)
def FSOpGetAttr(this, upath, io):
	if upath == '':
		dir = this.open_dir(upath, io)
		try:
			info = dir.get_attr()
		finally:
			this.close_dir(dir)
	else:
		upath_parent = udirname(upath)
		dir = this.open_dir(upath_parent, io)
		try:
			info = dir.get_child_attr(ubasename(upath))
		except IOError as err:
			if err.errno == errno.ENOENT and upath in this.open_items:
				# New file that has not yet been uploaded
				info = dict(this.open_items[upath].get_attr())
				if 'mtime' not in info:
					info['mtime'] = time.time()
				if 'ctime' not in info:
					info['ctime'] = time.time()
			else:
				raise
		finally:
			this.close_dir(dir)

	if upath in this.open_items:
		info.update(this.open_items[upath].get_attr())
		if 'mtime' not in info:
			info['mtime'] = time.time()
		if 'ctime' not in info:
			info['ctime'] = time.time()

	return info
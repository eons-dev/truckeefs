"""
lib/fs/fsop/file/GetAttributes.py

Purpose:
Retrieves attributes for a file (used for the FUSE getattr operation).

Place in Architecture:
Maps a file’s metadata (from the cache and remote state) to a dictionary that FUSE expects.

Interface:

	A function (decorated with @eons.kind(FSOp)) that takes a file upath and an I/O object and returns attributes.

TODOs/FIXMEs:
None explicitly noted.
"""

@eons.kind(FSOpGetAttr)
def file_getattributes(this, upath, io):
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
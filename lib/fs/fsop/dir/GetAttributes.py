
@eons.kind(FSOpGetAttr)
def directory_getattributes(this, upath, io):
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
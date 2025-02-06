
@eons.kind(FSOp)
def file_close(this, f):
	c = f.inode
	upath = f.upath
	f.close()
	if c.closed:
		if upath in this.open_items:
			del this.open_items[upath]
		this._restrict_size()
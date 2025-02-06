
@eons.kind(FSOp)
def directory_open(this, upath, io, lifetime=None):
	f = this.get_dir_inode(upath, io, lifetime=lifetime)
	return CachedDirHandle(upath, f)
"""
lib/fs/fsop/dir/GetInode.py

Purpose:
Retrieves (or creates) the inode for a directory.

Place in Architecture:
Similar to the file version, it handles cache lookup/creation for directories. It also manages an internal item cache for recently used directory handles.

Interface:

    A function (decorated with @eons.kind(FSOp)) that takes a directory upath and an I/O object and returns a CachedDirInode.

TODOs/FIXMEs:
None explicitly noted.
"""

@eons.kind(FSOp)
def directory_getinode(this, upath, io, ttl=None):
	if ttl is None:
		ttl = this.read_lifetime

	file = this.open_items.get(upath)

	if file is not None and not file.is_fresh(ttl):
		file = None
		this.invalidate(upath, shallow=True)

	if file is None:
		cap = this.LookupCap(upath, io, read_only=False, lifetime=ttl)
		file = CachedDirInode(this, upath, io, dircap=cap)
		this.open_items[upath] = file

		# Add to item cache
		cache_item = (time.time(), CachedDirHandle(upath, file))
		if len(this._item_cache) < this._max_item_cache:
			heapq.heappush(this._item_cache, cache_item)
		else:
			old_time, old_fh = heapq.heapreplace(this._item_cache,
													cache_item)
			this.close_dir(old_fh)

		return file
	else:
		if not isinstance(file, CachedDirInode):
			raise IOError(errno.ENOTDIR, "item is a file")
		return file
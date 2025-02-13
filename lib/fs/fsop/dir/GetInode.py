
@eons.kind(FSOp)
def directory_getinode(this, upath, io, lifetime=None):
	if lifetime is None:
		lifetime = this.read_lifetime

	f = this.open_items.get(upath)

	if f is not None and not f.is_fresh(lifetime):
		f = None
		this.invalidate(upath, shallow=True)

	if f is None:
		cap = this.LookupCap(upath, io, read_only=False, lifetime=lifetime)
		f = CachedDirInode(this, upath, io, dircap=cap)
		this.open_items[upath] = f

		# Add to item cache
		cache_item = (time.time(), CachedDirHandle(upath, f))
		if len(this._item_cache) < this._max_item_cache:
			heapq.heappush(this._item_cache, cache_item)
		else:
			old_time, old_fh = heapq.heapreplace(this._item_cache,
													cache_item)
			this.close_dir(old_fh)

		return f
	else:
		if not isinstance(f, CachedDirInode):
			raise IOError(errno.ENOTDIR, "item is a file")
		return f
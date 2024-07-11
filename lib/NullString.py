class NullString(object):
	def __init__(this, size):
		this.size = size

	def __len__(this):
		return this.size

	def __getitem__(this, k):
		if isinstance(k, slice):
			return b"\x00" * len(range(*k.indices(this.size)))
		else:
			raise IndexError("invalid index")

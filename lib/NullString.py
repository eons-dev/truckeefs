"""
lib/NullString.py

Purpose:
Provides a “dummy” string-like object that returns null bytes (zero-filled) for any slice. Useful for padding writes.

Place in Architecture:
Used by the file caching layer (e.g. in FileOnDisk or CryptFile) when writing past the end of a file.

Interface:

	__init__(size): Sets the total size.
	__len__(): Returns the size.
	__getitem__(k): Returns null bytes for a given slice.

TODOs/FIXMEs:
None.
"""

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

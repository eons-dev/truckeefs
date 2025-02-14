"""
lib/block/Utils.py

Purpose:
Provides utility functions for block-level operations.

Place in Architecture:
Used by the block caching and storage modules to compute block ranges and handle rounding (e.g. for partial blocks).

Interface:

	ceildiv(a, b): Computes the ceiling of a division.
	block_range(offset, length, block_size, last_pos=None): Computes the overlapping block ranges for a given data range.

TODOs/FIXMEs:
None.
"""


def ceildiv(a, b):
	"""Compute ceil(a/b); i.e. rounded towards positive infinity"""
	return 1 + (a-1)//b

def block_range(offset, length, block_size, last_pos=None):
	"""
	Get the blocks that overlap with data range [offset, offset+length]

	Parameters
	----------
	offset, length : int
		Range specification
	last_pos : int, optional
		End-of-file position. If the data range goes over the end of the file,
		the last block is the last block in `mid`, and `end` is None.

	Returns
	-------
	start : (idx, start_pos, end_pos) or None
		Partial block at the beginning; block[start_pos:end_pos] has the data. If missing: None
	mid : (start_idx, end_idx)
		Range [start_idx, end_idx) of full blocks in the middle. If missing: None
	end : (idx, end_pos)
		Partial block at the end; block[:end_pos] has the data. If missing: None

	"""
	if last_pos is not None:
		length = max(min(last_pos - offset, length), 0)

	if length == 0:
		return None, None, None

	start_block, start_pos = divmod(offset, block_size)
	end_block, end_pos = divmod(offset + length, block_size)

	if last_pos is not None:
		if offset + length == last_pos and end_pos > 0:
			end_block += 1
			end_pos = 0

	if start_block == end_block:
		if start_pos == end_pos:
			return None, None, None
		return (start_block, start_pos, end_pos), None, None

	mid = None

	if start_pos == 0:
		start = None
		mid = (start_block, end_block)
	else:
		start = (start_block, start_pos, block_size)
		if start_block+1 < end_block:
			mid = (start_block+1, end_block)

	if end_pos == 0:
		end = None
	else:
		end = (end_block, end_pos)

	return start, mid, end

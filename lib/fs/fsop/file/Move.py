"""
File Move FS Operation
----------------------

Purpose:
	Implements a file move (rename) operation. This FSOp renames or relocates a file
	from a source upath to a destination upath.

Role in Architecture:
	- Updates the local cache by modifying the inode’s upath and adjusting its parent’s children list.
	- Coordinates with the remote Tahoe backend to reflect the change.
	- Ensures atomicity so that either the move is fully completed or the state remains unchanged.

Interface:
	- Expected input parameters:
		* src_upath: The universal path of the file to move.
		* dst_upath: The target universal path.
		* io: The I/O object.
	- Returns: The updated inode or a success indicator.
	- Must raise appropriate IOError if the source does not exist or the destination already exists.

TODO/FIXMEs:
	- Ensure that metadata (e.g., timestamps, permissions) is preserved during the move.
	- Integrate logging and robust error recovery.
	- Update any related cache state (e.g., parent directory listings).
"""

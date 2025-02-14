"""
File Copy FS Operation
----------------------

Purpose:
	Implements a file copy operation. This FSOp duplicates a file from a source upath
	to a destination upath, preserving metadata and contents.

Role in Architecture:
	- Uses the caching layer to read the source file and create a new inode for the target.
	- Must coordinate with the Tahoe backend to ensure the new file’s capability is recorded.
	- Ensures that the copy is performed atomically, and metadata is updated in both the cache and the database.

Interface:
	- Expected input parameters:
		* src_upath: The universal path of the source file.
		* dst_upath: The universal path for the new copy.
		* io: The I/O object.
	- Returns: The new file’s inode or a success indicator.
	- Must raise appropriate IOError on error.

TODO/FIXMEs:
	- Implement atomicity to avoid partial copies.
	- Integrate logging for start, progress, and error conditions.
	- Update parent directory metadata for the new file.
"""

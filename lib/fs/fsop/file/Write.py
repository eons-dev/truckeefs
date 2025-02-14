"""
File Write FS Operation
------------------------

Purpose:
	Implements the POSIX write operation for files. This module provides a stateless,
	asynchronous functor (via eons) that writes data to a cached file.

Role in Architecture:
	- Wraps the underlying CachedFileInode/CachedFileHandle write() method.
	- Updates the file's block cache and marks the inode as dirty.
	- Supports handling special flags (like O_APPEND) and updates metadata accordingly.

Interface:
	- Expected input parameters:
		* upath: The universal path of the file.
		* io: The I/O object.
		* data: The data to be written (as bytes).
		* offset: The file offset where the write should occur (or None for append).
	- Returns: The number of bytes written.
	- Must raise appropriate IOError (e.g., for write permission issues).

TODO/FIXMEs:
	- Implement logic for handling O_APPEND mode.
	- Integrate logging and error recovery.
	- Ensure that partial writes update the cache and metadata correctly.
"""

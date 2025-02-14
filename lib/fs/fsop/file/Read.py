"""
File Read FS Operation
-----------------------

Purpose:
	Implements the POSIX read operation for files. This module provides a stateless,
	asynchronous functor (using the eons framework) that reads data from a cached file.

Role in Architecture:
	- Acts as a thin wrapper over the underlying CachedFileInode and CachedFileHandle.
	- Retrieves the requested data (given an offset and length) from the local cache,
	  and—if necessary—may trigger a block fetch from the remote Tahoe backend.
	- Returns the read data to the calling FS layer (e.g. FUSE).

Interface:
	- Expected input parameters:
		* upath: The universal path of the file.
		* io: The I/O object for remote/cache operations.
		* size: The number of bytes to read.
		* offset: The offset within the file from where to begin reading.
	- Returns: A byte string with the requested file data.
	- Must raise appropriate IOError with errno codes on failure.

TODO/FIXMEs:
	- Implement caching logic: if blocks are missing, trigger a pre_read operation.
	- Integrate logging for the start, success, and error conditions.
	- Consider concurrency and thread-safety when accessing the file cache.
"""

"""
File Append FS Operation
------------------------

Purpose:
	Implements the POSIX append operation for files. This FSOp appends data to the end
	of a file, ensuring that the file pointer is updated accordingly.

Role in Architecture:
	- Wraps the underlying write() operation with logic to always write at the end of the file.
	- Works with the caching layer to update the block cache and metadata.
	- Intended to be used in situations where O_APPEND mode is desired.

Interface:
	- Expected input parameters:
		* upath: The universal path of the file.
		* io: The I/O object.
		* data: The data to be appended.
	- Returns: The number of bytes appended.
	- Must raise appropriate IOError on error.

TODO/FIXMEs:
	- Ensure thread safety when multiple appends occur.
	- Integrate logging and error recovery.
	- Verify that metadata (e.g., file size) is correctly updated after appending.
"""

"""
lib/fs/common/FSOp.py

Purpose:
Defines the base class for File System Operations (FSOps). Each FSOps represents a single atomic operation (e.g. open, read, write).

Place in Architecture:
Serves as the foundation for all asynchronous and stateless FS operations. All other FSOp modules (for files and directories) inherit from this.

Interface:

	Inherits from eons.Functor.
	No additional methods are defined here (all functionality is provided by the child FSOps).

TODOs/FIXMEs:
None.
"""


import eons

# An FSOp, or File System Operation, is a Functor which performs a single operation on a file system.
# For example, opening a file, reading from a file, writing to a file, etc.
# FSOp is a base class for all file system operations.
# All FSOps should be:
# - Stateless: They should not store any state, and should not have any in-memory side effects.
# - Asynchronous: They will be given their own thread to run in and should return a Future.
# - Scalable: Multiple FSOps should be able to run in parallel without interfering with each other.
#
# All state storage and locking capabilities will be provided by the governing RiverFS Executor.
class FSOp(eons.Functor):
	def __init__(this, name=eons.INVALID_NAME()):
		super().__init__(name)

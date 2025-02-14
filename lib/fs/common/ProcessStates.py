"""
lib/fs/common/ProcessStates.py

Purpose:
Defines an enumeration of process states used for tracking the progress of asynchronous inode operations.

Place in Architecture:
Used by the Inode (and RiverDelta) to coordinate concurrent operations (read, write, sync).

Interface:

	Enum members: ERROR, PENDING, RUNNING, COMPLETE, and IDLE.

TODOs/FIXMEs:
None.
"""

from enum import Enum

# Processes are some possible conflicting file operations. RiverFS uses 3 distinct processes to manage concurrent inode operations: Reads, Writes, and Syncs.
# Each process has a state, which can be one of the following:
class ProcessState(Enum):
	ERROR = 0
	PENDING = 1
	RUNNING = 2
	COMPLETE = 3
	IDLE = 4

	def __str__(self):
		return self.name
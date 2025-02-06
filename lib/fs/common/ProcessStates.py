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
"""
FileOnDisk: A plain file I/O implementation for TruckeeFS caching.

This version removes all cryptographic functionality and writes data to disk
in fixed-size blocks with a simple header for storing metadata.

File layout:
    [HEADER][BLOCK 0][BLOCK 1]… 

Header format (16 bytes):
    - 4 bytes: magic string, e.g. b'FOD0'
    - 4 bytes: block size (unsigned int, little-endian)
    - 8 bytes: data size (unsigned long long, little-endian)
"""

import os
import sys
import struct
import errno
import fcntl

# We'll use a fixed block size (default 131072 bytes) unless overridden.
BLOCK_SIZE = 131072

# Header format: 4-byte magic, 4-byte block_size, 8-byte data_size.
MAGIC = b'FOD0'
HEADER_FORMAT = '<4sIQ'  # 4s: magic, I: block size, Q: data size
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # Should be 16 bytes

class FileOnDisk(object):
    """
    A file stored on disk in fixed-size blocks with a simple header.
    
    The file layout:
      HEADER | BLOCK 0 | BLOCK 1 | ... | BLOCK n
    
    The header contains a magic marker, the block size, and the logical data size.
    """
    def __init__(self, path, mode='r+b', block_size=BLOCK_SIZE, key=None):
		#NOTE: Key is unused.

        self.path = path

        if mode not in ('rb', 'r+b', 'w+b'):
            raise IOError(errno.EACCES, "Unsupported mode %r" % (mode,))

        if mode == 'rb':
            fd = os.open(path, os.O_RDONLY)
        elif mode == 'r+b':
            fd = os.open(path, os.O_RDWR)
        elif mode == 'w+b':
            fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o0600)

        try:
            # BSD file locking: shared lock for read-only, exclusive for writing.
            if mode == 'rb':
                fcntl.flock(fd, fcntl.LOCK_SH)
            else:
                fcntl.flock(fd, fcntl.LOCK_EX)
            
            # For a new file (w+b), truncate after locking.
            if mode == 'w+b':
                os.ftruncate(fd, 0)
            
            self.fp = os.fdopen(fd, mode)
        except Exception:
            os.close(fd)
            raise

        self.mode = mode
        self.block_size = block_size

        # For new files, start with zero data size.
        if mode == 'w+b':
            self.data_size = 0
            # Write a new header immediately.
            self._write_header()
        else:
            # Read and validate header.
            header = self.fp.read(HEADER_SIZE)
            if len(header) != HEADER_SIZE:
                self.fp.close()
                raise ValueError("Invalid header data in file")
            magic, stored_block_size, data_size = struct.unpack(HEADER_FORMAT, header)
            if magic != MAGIC:
                self.fp.close()
                raise ValueError("Invalid file format (magic mismatch)")
            if stored_block_size != self.block_size:
                self.fp.close()
                raise ValueError("Block size mismatch: expected %d but found %d" %
                                 (self.block_size, stored_block_size))
            self.data_size = data_size

        # Block caching state
        self.current_block = -1
        self.block_cache = b""
        self.block_dirty = False

        self.offset = 0

    def _write_header(self):
        """Write the header (magic, block size, data size) at the beginning of the file."""
        self.fp.seek(0)
        header = struct.pack(HEADER_FORMAT, MAGIC, self.block_size, self.data_size)
        self.fp.write(header)

    def _flush_block(self):
        """If the current block is dirty, write it to disk (padding with null bytes if needed)."""
        if self.current_block < 0:
            return
        if not self.block_dirty:
            return

        self.fp.seek(HEADER_SIZE + self.current_block * self.block_size)
        data_to_write = self.block_cache
        if len(data_to_write) < self.block_size:
            data_to_write += b'\x00' * (self.block_size - len(data_to_write))
        self.fp.write(data_to_write)
        self.block_dirty = False

    def _load_block(self, i):
        """Load block number i from disk into the cache."""
        if i == self.current_block:
            return

        self._flush_block()
        self.fp.seek(HEADER_SIZE + i * self.block_size)
        block_data = self.fp.read(self.block_size)
        if not block_data:
            # Block does not exist (i is past end of file).
            self.current_block = i
            self.block_cache = b""
            self.block_dirty = False
            return

        # Determine actual size to return.
        if (i + 1) * self.block_size > self.data_size:
            size = self.data_size - i * self.block_size
        else:
            size = self.block_size

        self.current_block = i
        self.block_cache = block_data[:size]
        self.block_dirty = False

    def seek(self, offset, whence=0):
        if whence == 0:  # Absolute
            pass
        elif whence == 1:  # Relative
            offset = self.offset + offset
        elif whence == 2:  # From end
            offset += self.data_size
        else:
            raise IOError(errno.EINVAL, "Invalid whence")
        if offset < 0:
            raise IOError(errno.EINVAL, "Invalid offset")
        self.offset = offset

    def tell(self):
        return self.offset

    def _read(self, size, offset):
        if size is None:
            size = self.data_size - offset
        if size <= 0:
            return b""

        start_block, start_off = divmod(offset, self.block_size)
        end_block, end_off = divmod(offset + size, self.block_size)
        if end_off != 0:
            end_block += 1

        data = []
        for i in range(start_block, end_block):
            self._load_block(i)
            data.append(self.block_cache)

        if end_off != 0:
            data[-1] = data[-1][:end_off]
        data[0] = data[0][start_off:]
        return b"".join(data)

    def _write(self, data, offset):
        size = len(data)
        start_block, start_off = divmod(offset, self.block_size)
        end_block, end_off = divmod(offset + size, self.block_size)

        k = 0

        if self.mode == 'rb':
            raise IOError(errno.EACCES, "Write to a read-only file")

        # Write first block if partial.
        if start_off != 0 or end_block == start_block:
            self._load_block(start_block)
            data_block = data[:(self.block_size - start_off)]
            self.block_cache = self.block_cache[:start_off] + data_block + self.block_cache[start_off+len(data_block):]
            self.block_dirty = True
            k += 1
            start_block += 1

        # Write full blocks.
        for i in range(start_block, end_block):
            if self.current_block != i:
                self._flush_block()
            self.current_block = i
            # Calculate the slice of data that goes into this block.
            block_data = data[k * self.block_size - (0 if start_off == 0 else (self.block_size - start_off)) : (k+1) * self.block_size - (0 if start_off == 0 else (self.block_size - start_off))]
            self.block_cache = block_data
            self.block_dirty = True
            k += 1

        # Write last partial block.
        if end_block > start_block and end_off != 0:
            self._load_block(end_block)
            data_block = data[k * self.block_size - (0 if start_off == 0 else (self.block_size - start_off)) : (k+1) * self.block_size - (0 if start_off == 0 else (self.block_size - start_off))]
            self.block_cache = data_block + self.block_cache[len(data_block):]
            self.block_dirty = True

        self.data_size = max(self.data_size, offset + len(data))

    def read(self, size=None):
        data = self._read(size, self.offset)
        self.offset += len(data)
        return data

    def write(self, data):
        # If writing past the current data size, pad with null bytes.
        if self.data_size < self.offset:
            from .NullString import NullString
            s = NullString(self.offset - self.data_size)
            self._write(s, self.data_size)
        self._write(data, self.offset)
        self.offset += len(data)

    def truncate(self, size):
        last_block, last_off = divmod(size, self.block_size)

        self._load_block(last_block)
        last_block_data = self.block_cache

        # Flush current block and then truncate file to block boundary.
        self._flush_block()
        sz = HEADER_SIZE + last_block * self.block_size
        self.fp.truncate(sz)
        self.data_size = last_block * self.block_size
        self.current_block = -1
        self.block_cache = b""
        self.block_dirty = False

        # Rewrite last block if needed.
        if last_off != 0:
            self._write(last_block_data[:last_off], self.data_size)

        # Add null padding if necessary.
        if self.data_size < size:
            from .NullString import NullString
            s = NullString(size - self.data_size)
            self._write(s, self.data_size)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False

    def flush(self):
        if self.mode != 'rb':
            self._flush_block()
            self._write_header()
        self.fp.flush()

    def close(self):
        if self.fp is None:
            return
        if self.mode != 'rb':
            self.flush()
        self.fp.close()
        self.fp = None

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

"""
Cache metadata and data of a directory tree for read-only access.
"""

import os
import time
import json
import zlib
import struct
import errno
import threading
import codecs
import heapq

from cryptography.hazmat.primitives import hashes, hmac
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from .tahoeio import HTTPError
from .crypto import CryptFile, backend
from .blockcache import BlockCachedFile


class CacheDB(object):
	def __init__(this, path, rootcap, node_url, cache_size, cache_data,
				 read_lifetime, write_lifetime):
		path = os.path.abspath(path)
		if not os.path.isdir(path):
			raise IOError(errno.ENOENT, "Cache directory is not an existing directory")

		assert isinstance(rootcap, str)

		this.cache_size = cache_size
		this.cache_data = cache_data
		this.read_lifetime = read_lifetime
		this.write_lifetime = write_lifetime

		this.path = path
		this.key, this.salt_hkdf = this._generate_prk(rootcap)

		this.last_size_check_time = 0

		# Cache lock
		this.lock = threading.RLock()

		# Open files and dirs
		this.open_items = {}

		# Restrict cache size
		this._restrict_size()

		# Directory cache
		this._max_item_cache = 500
		this._item_cache = []

	def _generate_prk(this, rootcap):
		# Cache master key is derived from hashed rootcap and salt via
		# PBKDF2, with a fixed number of iterations.
		#
		# The master key, combined with a second different salt, are
		# used to generate per-file keys via HKDF-SHA256

		# Get salt
		salt_fn = os.path.join(this.path, 'salt')
		try:
			with open(salt_fn, 'rb') as f:
				numiter = f.read(4)
				salt = f.read(32)
				salt_hkdf = f.read(32)
				if len(numiter) != 4 or len(salt) != 32 or len(salt_hkdf) != 32:
					raise ValueError()
				numiter = struct.unpack('<I', numiter)[0]
		except (IOError, OSError, ValueError):
			# Start with new salt
			rnd = os.urandom(64)
			salt = rnd[:32]
			salt_hkdf = rnd[32:]

			# Determine suitable number of iterations
			start = time.time()
			count = 0
			while True:
				kdf = PBKDF2HMAC(
					algorithm=hashes.SHA256(),
					length=32,
					salt=b"b"*len(salt),
					iterations=10000,
					backend=backend
				)
				kdf.derive(b"a"*len(rootcap.encode('ascii')))
				count += 10000
				if time.time() > start + 0.05:
					break
			numiter = max(10000, int(count * 1.0 / (time.time() - start)))

			# Write salt etc.
			with open(salt_fn, 'wb') as f:
				f.write(struct.pack('<I', numiter))
				f.write(salt)
				f.write(salt_hkdf)

		# Derive key
		kdf = PBKDF2HMAC(
			algorithm=hashes.SHA256(),
			length=32,
			salt=salt,
			iterations=numiter,
			backend=backend
		)
		key = kdf.derive(rootcap.encode('ascii'))

		# HKDF private key material for per-file keys
		return key, salt_hkdf

	def _walk_cache_subtree(this, root_upath=""):
		"""
		Walk through items in the cached directory tree, starting from
		the given root point.

		Yields
		------
		filename, upath
			Filename and corresponding upath of a reached cached entry.

		"""
		stack = []

		# Start from root
		fn, key = this.get_filename_and_key(root_upath)
		if os.path.isfile(fn):
			stack.append((root_upath, fn, key))

		# Walk the tree
		while stack:
			upath, fn, key = stack.pop()

			if not os.path.isfile(fn):
				continue

			try:
				with CryptFile(fn, key=key, mode='rb') as f:
					data = json_zlib_load(f)
					if data[0] == 'dirnode':
						children = list(data[1].get('children', {}).items())
					else:
						children = []
			except (IOError, OSError, ValueError):
				continue

			yield (os.path.basename(fn), upath)

			for c_fn, c_info in children:
				c_upath = os.path.join(upath, c_fn)
				if c_info[0] == 'dirnode':
					c_fn, c_key = this.get_filename_and_key(c_upath)
					if os.path.isfile(c_fn):
						stack.append((c_upath, c_fn, c_key))
				elif c_info[0] == 'filenode':
					for ext in (None, b'state', b'data'):
						c_fn, c_key = this.get_filename_and_key(c_upath, ext=ext)
						yield (os.path.basename(c_fn), c_upath)

	def _restrict_size(this):
		def get_cache_score(entry):
			fn, st = entry
			return -cache_score(size=st.st_size, t=now-st.st_mtime)

		with this.lock:
			now = time.time()
			if now < this.last_size_check_time + 60:
				return

			this.last_size_check_time = now

			files = [os.path.join(this.path, fn) 
					 for fn in os.listdir(this.path) 
					 if fn != "salt"]
			entries = [(fn, os.stat(fn)) for fn in files]
			entries.sort(key=get_cache_score)

			tot_size = 0
			for fn, st in entries:
				if tot_size + st.st_size > this.cache_size:
					# unlink
					os.unlink(fn)
				else:
					tot_size += st.st_size

	def _invalidate(this, root_upath="", shallow=False):
		if root_upath == "" and not shallow:
			for f in this.open_items.values():
				f.invalidated = True
			this.open_items = {}
			dead_file_set = os.listdir(this.path)
		else:
			dead_file_set = set()
			for fn, upath in this._walk_cache_subtree(root_upath):
				f = this.open_items.pop(upath, None)
				if f is not None:
					f.invalidated = True
				dead_file_set.add(fn)
				if shallow and upath != root_upath:
					break

		for basename in dead_file_set:
			if basename == 'salt':
				continue
			fn = os.path.join(this.path, basename)
			if os.path.isfile(fn):
				os.unlink(fn)

	def invalidate(this, root_upath="", shallow=False):
		with this.lock:
			this._invalidate(root_upath, shallow=shallow)

	def open_file(this, upath, io, flags, lifetime=None):
		with this.lock:
			writeable = (flags & (os.O_RDONLY | os.O_RDWR | os.O_WRONLY)) in (os.O_RDWR, os.O_WRONLY)
			if writeable:
				# Drop file data cache before opening in write mode
				if upath not in this.open_items:
					this.invalidate(upath)

				# Limit e.g. parent directory lookup lifetime
				if lifetime is None:
					lifetime = this.write_lifetime

			f = this.get_file_inode(upath, io,
									excl=(flags & os.O_EXCL),
									creat=(flags & os.O_CREAT),
									lifetime=lifetime)
			return CachedFileHandle(upath, f, flags)

	def open_dir(this, upath, io, lifetime=None):
		with this.lock:
			f = this.get_dir_inode(upath, io, lifetime=lifetime)
			return CachedDirHandle(upath, f)

	def close_file(this, f):
		with this.lock:
			c = f.inode
			upath = f.upath
			f.close()
			if c.closed:
				if upath in this.open_items:
					del this.open_items[upath]
				this._restrict_size()

	def close_dir(this, f):
		with this.lock:
			c = f.inode
			upath = f.upath
			f.close()
			if c.closed:
				if upath in this.open_items:
					del this.open_items[upath]
				this._restrict_size()

	def upload_file(this, c, io):
		if isinstance(c, CachedFileHandle):
			c = c.inode

		if c.upath is not None and c.dirty:
			parent = this.open_dir(udirname(c.upath), io, lifetime=this.write_lifetime)
			try:
				parent_cap = parent.inode.info[1]['rw_uri']

				# Upload
				try:
					cap = c.upload(io, parent_cap=parent_cap)
				except:
					# Failure to upload --- need to invalidate parent
					# directory, since the file might not have been
					# created.
					this.invalidate(parent.upath, shallow=True)
					raise

				# Add in cache
				with this.lock:
					parent.inode.cache_add_child(ubasename(c.upath), cap, size=c.get_size())
			finally:
				this.close_dir(parent)

	def unlink(this, upath, io, is_dir=False):
		if upath == '':
			raise IOError(errno.EACCES, "cannot unlink root directory")

		with this.lock:
			# Unlink in cache
			if is_dir:
				f = this.open_dir(upath, io, lifetime=this.write_lifetime)
			else:
				f = this.open_file(upath, io, 0, lifetime=this.write_lifetime)
			try:
				f.inode.unlink()
			finally:
				if is_dir:
					this.close_dir(f)
				else:
					this.close_file(f)

			# Perform unlink
			parent = this.open_dir(udirname(upath), io, lifetime=this.write_lifetime)
			try:
				parent_cap = parent.inode.info[1]['rw_uri']

				upath_cap = parent_cap + '/' + ubasename(upath)
				try:
					cap = io.delete(upath_cap, iscap=True)
				except (HTTPError, IOError) as err:
					if isinstance(err, HTTPError) and err.code == 404:
						raise IOError(errno.ENOENT, "no such file")
					raise IOError(errno.EREMOTEIO, "failed to retrieve information")

				# Remove from cache
				parent.inode.cache_remove_child(ubasename(upath))
			finally:
				this.close_dir(parent)

	def mkdir(this, upath, io):
		if upath == '':
			raise IOError(errno.EEXIST, "cannot re-mkdir root directory")

		with this.lock:
			# Check that parent exists
			parent = this.open_dir(udirname(upath), io, lifetime=this.write_lifetime)
			try:
				parent_cap = parent.inode.info[1]['rw_uri']

				# Check that the target does not exist
				try:
					parent.get_child_attr(ubasename(upath))
				except IOError as err:
					if err.errno == errno.ENOENT:
						pass
					else:
						raise
				else:
					raise IOError(errno.EEXIST, "directory already exists")

				# Invalidate cache
				this.invalidate(upath)

				# Perform operation
				upath_cap = parent_cap + '/' + ubasename(upath)
				try:
					cap = io.mkdir(upath_cap, iscap=True)
				except (HTTPError, IOError) as err:
					raise IOError(errno.EREMOTEIO, "remote operation failed: {0}".format(err))

				# Add in cache
				parent.inode.cache_add_child(ubasename(upath), cap, size=None)
			finally:
				this.close_dir(parent)

	def get_attr(this, upath, io):
		if upath == '':
			dir = this.open_dir(upath, io)
			try:
				info = dir.get_attr()
			finally:
				this.close_dir(dir)
		else:
			upath_parent = udirname(upath)
			dir = this.open_dir(upath_parent, io)
			try:
				info = dir.get_child_attr(ubasename(upath))
			except IOError as err:
				with this.lock:
					if err.errno == errno.ENOENT and upath in this.open_items:
						# New file that has not yet been uploaded
						info = dict(this.open_items[upath].get_attr())
						if 'mtime' not in info:
							info['mtime'] = time.time()
						if 'ctime' not in info:
							info['ctime'] = time.time()
					else:
						raise
			finally:
				this.close_dir(dir)

		with this.lock:
			if upath in this.open_items:
				info.update(this.open_items[upath].get_attr())
				if 'mtime' not in info:
					info['mtime'] = time.time()
				if 'ctime' not in info:
					info['ctime'] = time.time()

		return info

	def _lookup_cap(this, upath, io, read_only=True, lifetime=None):
		if lifetime is None:
			lifetime = this.read_lifetime

		with this.lock:
			if upath in this.open_items and this.open_items[upath].is_fresh(lifetime):
				# shortcut
				if read_only:
					return this.open_items[upath].info[1]['ro_uri']
				else:
					return this.open_items[upath].info[1]['rw_uri']
			elif upath == '':
				# root
				return None
			else:
				# lookup from parent
				entry_name = ubasename(upath)
				parent_upath = udirname(upath)

				parent = this.open_dir(parent_upath, io, lifetime=lifetime)
				try:
					if read_only:
						return parent.get_child_attr(entry_name)['ro_uri']
					else:
						return parent.get_child_attr(entry_name)['rw_uri']
				finally:
					this.close_dir(parent)

	def get_file_inode(this, upath, io, excl=False, creat=False, lifetime=None):
		if lifetime is None:
			lifetime = this.read_lifetime

		with this.lock:
			f = this.open_items.get(upath)

			if f is not None and not f.is_fresh(lifetime):
				f = None
				this.invalidate(upath, shallow=True)

			if f is None:
				try:
					cap = this._lookup_cap(upath, io, lifetime=lifetime)
				except IOError as err:
					if err.errno == errno.ENOENT and creat:
						cap = None
					else:
						raise

				if excl and cap is not None:
					raise IOError(errno.EEXIST, "file already exists")
				if not creat and cap is None:
					raise IOError(errno.ENOENT, "file does not exist")

				f = CachedFileInode(this, upath, io, filecap=cap, 
									persistent=this.cache_data)
				this.open_items[upath] = f

				if cap is None:
					# new file: add to parent inode
					d = this.open_dir(udirname(upath), io, lifetime=lifetime)
					try:
						d.inode.cache_add_child(ubasename(upath), None, size=0)
					finally:
						this.close_dir(d)
				return f
			else:
				if excl:
					raise IOError(errno.EEXIST, "file already exists")
				if not isinstance(f, CachedFileInode):
					raise IOError(errno.EISDIR, "item is a directory")
				return f

	def get_dir_inode(this, upath, io, lifetime=None):
		if lifetime is None:
			lifetime = this.read_lifetime

		with this.lock:
			f = this.open_items.get(upath)

			if f is not None and not f.is_fresh(lifetime):
				f = None
				this.invalidate(upath, shallow=True)

			if f is None:
				cap = this._lookup_cap(upath, io, read_only=False, lifetime=lifetime)
				f = CachedDirInode(this, upath, io, dircap=cap)
				this.open_items[upath] = f

				# Add to item cache
				cache_item = (time.time(), CachedDirHandle(upath, f))
				if len(this._item_cache) < this._max_item_cache:
					heapq.heappush(this._item_cache, cache_item)
				else:
					old_time, old_fh = heapq.heapreplace(this._item_cache,
														 cache_item)
					this.close_dir(old_fh)

				return f
			else:
				if not isinstance(f, CachedDirInode):
					raise IOError(errno.ENOTDIR, "item is a file")
				return f

	def get_upath_parent(this, path):
		return this.get_upath(os.path.dirname(os.path.normpath(path)))

	def get_upath(this, path):
		assert isinstance(path, str)
		try:
			path = os.path.normpath(path)
			return path.replace(os.sep, "/").lstrip('/')
		except UnicodeError:
			raise IOError(errno.ENOENT, "file does not exist")

	def path_from_upath(this, upath):
		return upath.replace(os.sep, "/")

	def get_filename_and_key(this, upath, ext=None):
		path = upath.encode('utf-8')
		nonpath = b"//\x00" # cannot occur in path, which is normalized

		# Generate per-file key material via HKDF
		info = path
		if ext is not None:
			info += nonpath + ext

		hkdf = HKDF(algorithm=hashes.SHA256(),
					length=3*32,
					salt=this.salt_hkdf,
					info=info,
					backend=backend)
		data = hkdf.derive(this.key)

		# Generate key
		key = data[:32]

		# Generate filename
		h = hmac.HMAC(key=data[32:], algorithm=hashes.SHA512(), backend=backend)
		h.update(info)
		fn = codecs.encode(h.finalize(), 'hex_codec').decode('ascii')
		return os.path.join(this.path, fn), key


class CachedFileHandle(object):
	"""
	Logical file handle. There may be multiple open file handles
	corresponding to the same logical file.
	"""

	direct_io = False
	keep_cache = False

	def __init__(this, upath, inode, flags):
		this.inode = inode
		this.inode.incref()
		this.lock = threading.RLock()
		this.flags = flags
		this.upath = upath

		this.writeable = (this.flags & (os.O_RDONLY | os.O_RDWR | os.O_WRONLY)) in (os.O_RDWR, os.O_WRONLY)
		this.readable = (this.flags & (os.O_RDONLY | os.O_RDWR | os.O_WRONLY)) in (os.O_RDWR, os.O_RDONLY)
		this.append = (this.flags & os.O_APPEND)

		if this.flags & os.O_ASYNC:
			raise IOError(errno.ENOTSUP, "O_ASYNC flag is not supported")
		if this.flags & os.O_DIRECT:
			raise IOError(errno.ENOTSUP, "O_DIRECT flag is not supported")
		if this.flags & os.O_DIRECTORY:
			raise IOError(errno.ENOTSUP, "O_DIRECTORY flag is not supported")
		if this.flags & os.O_SYNC:
			raise IOError(errno.ENOTSUP, "O_SYNC flag is not supported")
		if (this.flags & os.O_CREAT) and not this.writeable:
			raise IOError(errno.EINVAL, "O_CREAT without writeable file")
		if (this.flags & os.O_TRUNC) and not this.writeable:
			raise IOError(errno.EINVAL, "O_TRUNC without writeable file")
		if (this.flags & os.O_EXCL) and not this.writeable:
			raise IOError(errno.EINVAL, "O_EXCL without writeable file")
		if (this.flags & os.O_APPEND) and not this.writeable:
			raise IOError(errno.EINVAL, "O_EXCL without writeable file")

		if (this.flags & os.O_TRUNC):
			this.inode.truncate(0)

	def close(this):
		with this.lock:
			if this.inode is None:
				raise IOError(errno.EBADF, "Operation on a closed file")
			c = this.inode
			this.inode = None
			c.decref()

	def read(this, io, offset, length):
		with this.lock:
			if this.inode is None:
				raise IOError(errno.EBADF, "Operation on a closed file")
			if not this.readable:
				raise IOError(errno.EBADF, "File not readable")
			return this.inode.read(io, offset, length)

	def get_size(this):
		with this.lock:
			return this.inode.get_size()

	def write(this, io, offset, data):
		with this.lock:
			if this.inode is None:
				raise IOError(errno.EBADF, "Operation on a closed file")
			if not this.writeable:
				raise IOError(errno.EBADF, "File not writeable")
			if this.append:
				offset = None
			return this.inode.write(io, offset, data)

	def truncate(this, size):
		with this.lock:
			if this.inode is None:
				raise IOError(errno.EBADF, "Operation on a closed file")
			if not this.writeable:
				raise IOError(errno.EBADF, "File not writeable")
			return this.inode.truncate(size)


class CachedDirHandle(object):
	"""
	Logical directory handle.
	"""

	def __init__(this, upath, inode):
		this.inode = inode
		this.inode.incref()
		this.lock = threading.RLock()
		this.upath = upath

	def close(this):
		with this.lock:
			if this.inode is None:
				raise IOError(errno.EBADF, "Operation on a closed dir")
			c = this.inode
			this.inode = None
			c.decref()

	def listdir(this):
		with this.lock:
			if this.inode is None:
				raise IOError(errno.EBADF, "Operation on a closed dir")
			return this.inode.listdir()

	def get_attr(this):
		with this.lock:
			if this.inode is None:
				raise IOError(errno.EBADF, "Operation on a closed dir")
			return this.inode.get_attr()

	def get_child_attr(this, childname):
		with this.lock:
			if this.inode is None:
				raise IOError(errno.EBADF, "Operation on a closed dir")
			return this.inode.get_child_attr(childname)


class CachedFileInode(object):
	"""
	Logical file on-disk. There should be only a single CachedFileInode
	instance is per each logical file.
	"""

	def __init__(this, cachedb, upath, io, filecap, persistent=False):
		this.upath = upath
		this.closed = False
		this.refcnt = 0
		this.persistent = persistent
		this.invalidated = False

		# Use per-file keys for different files, for safer fallback
		# in the extremely unlikely event of SHA512 hash collisions
		filename, key = cachedb.get_filename_and_key(upath)
		filename_state, key_state = cachedb.get_filename_and_key(upath, b'state')
		filename_data, key_data = cachedb.get_filename_and_key(upath, b'data')

		this.lock = threading.RLock()
		this.cache_lock = threading.RLock()
		this.dirty = False
		this.f = None
		this.f_state = None
		this.f_data = None

		this.stream_f = None
		this.stream_offset = 0
		this.stream_data = []

		open_complete = False

		try:
			if filecap is None:
				# Create new file
				raise ValueError()

			# Reuse cached metadata
			this.f = CryptFile(filename, key=key, mode='r+b')
			this.info = json_zlib_load(this.f)

			if persistent:
				# Reuse cached data
				this.f_state = CryptFile(filename_state, key=key_state, mode='r+b')
				this.f_data = CryptFile(filename_data, key=key_data, mode='r+b')
				this.block_cache = BlockCachedFile.restore_state(this.f_data, this.f_state)
				open_complete = True
		except (IOError, OSError, ValueError):
			open_complete = False
			if this.f is not None:
				this.f.close()
				this.f = None
			if this.f_state is not None:
				this.f_state.close()
			if this.f_data is not None:
				this.f_data.close()

		if not open_complete:
			if this.f is None:
				this.f = CryptFile(filename, key=key, mode='w+b')
				try:
					if filecap is not None:
						this._load_info(filecap, io, iscap=True)
					else:
						this.info = ['file', {'size': 0}]
						this.dirty = True
				except IOError as err:
					os.unlink(filename)
					this.f.close()
					raise

			# Create a data file
			this.f_data = CryptFile(filename_data, key=key_data, mode='w+b')

			# Block cache on top of data file
			this.block_cache = BlockCachedFile(this.f_data, this.info[1]['size'])

			# Block data state file
			this.f_state = CryptFile(filename_state, key=key_state, mode='w+b')

		os.utime(this.f.path, None)
		os.utime(this.f_data.path, None)
		os.utime(this.f_state.path, None)

	def _load_info(this, upath, io, iscap=False):
		try:
			this.info = io.get_info(upath, iscap=iscap)
		except (HTTPError, IOError, ValueError) as err:
			if isinstance(err, HTTPError) and err.code == 404:
				raise IOError(errno.ENOENT, "no such file")
			raise IOError(errno.EREMOTEIO, "failed to retrieve information")
		this._save_info()

	def _save_info(this):
		this.f.truncate(0)
		this.f.seek(0)
		if 'retrieved' not in this.info[1]:
			this.info[1]['retrieved'] = time.time()
		json_zlib_dump(this.info, this.f)

	def is_fresh(this, lifetime):
		if 'retrieved' not in this.info[1]:
			return True
		return (this.info[1]['retrieved'] + lifetime >= time.time())

	def incref(this):
		with this.cache_lock:
			this.refcnt += 1

	def decref(this):
		with this.cache_lock:
			this.refcnt -= 1
			if this.refcnt <= 0:
				this.close()

	def close(this):
		with this.cache_lock, this.lock:
			if not this.closed:
				if this.stream_f is not None:
					this.stream_f.close()
					this.stream_f = None
					this.stream_data = []
				this.f_state.seek(0)
				this.f_state.truncate(0)
				this.block_cache.save_state(this.f_state)
				this.f_state.close()
				this.block_cache.close()
				this.f.close()

				if not this.persistent and this.upath is not None and not this.invalidated:
					os.unlink(this.f_state.path)
					os.unlink(this.f_data.path)
			this.closed = True

	def _do_rw(this, io, offset, length_or_data, write=False, no_result=False):
		if write:
			data = length_or_data
			length = len(data)
		else:
			length = length_or_data

		while True:
			with this.cache_lock:
				if write:
					pos = this.block_cache.pre_write(offset, length)
				else:
					pos = this.block_cache.pre_read(offset, length)

				if pos is None:
					# cache ready
					if no_result:
						return None
					elif write:
						return this.block_cache.write(offset, data)
					else:
						return this.block_cache.read(offset, length)

			# cache not ready -- fill it up
			with this.lock:
				try:
					c_offset, c_length = pos

					if this.stream_f is not None and (this.stream_offset > c_offset or
													  c_offset >= this.stream_offset + 3*131072):
						this.stream_f.close()
						this.stream_f = None
						this.stream_data = []

					if this.stream_f is None:
						this.stream_f = io.get_content(this.info[1]['ro_uri'], c_offset, iscap=True)
						this.stream_offset = c_offset
						this.stream_data = []

					read_offset = this.stream_offset
					read_bytes = sum(len(x) for x in this.stream_data)
					while read_offset + read_bytes < c_offset + c_length:
						block = this.stream_f.read(131072)

						if not block:
							this.stream_f.close()
							this.stream_f = None
							this.stream_data = []
							break

						this.stream_data.append(block)
						read_bytes += len(block)

						with this.cache_lock:
							this.stream_offset, this.stream_data = this.block_cache.receive_cached_data(
								this.stream_offset, this.stream_data)
				except (HTTPError, IOError) as err:
					if this.stream_f is not None:
						this.stream_f.close()
					this.stream_f = None
					raise IOError(errno.EREMOTEIO, "I/O error: %s" % (str(err),))

	def get_size(this):
		with this.cache_lock:
			return this.block_cache.get_size()

	def get_attr(this):
		return dict(type='file', size=this.get_size())

	def read(this, io, offset, length):
		return this._do_rw(io, offset, length, write=False)

	def write(this, io, offset, data):
		"""
		Write data to file. If *offset* is None, it means append.
		"""
		with this.lock:
			if len(data) > 0:
				this.dirty = True
				if offset is None:
					offset = this.get_size()
				this._do_rw(io, offset, data, write=True)

	def truncate(this, size):
		with this.cache_lock, this.lock:
			if size != this.block_cache.get_size():
				this.dirty = True
			this.block_cache.truncate(size)

	def _buffer_whole_file(this, io):
		with this.cache_lock:
			this._do_rw(io, 0, this.block_cache.get_size(), write=False, no_result=True)

	def upload(this, io, parent_cap=None):
		with this.cache_lock, this.lock:
			# Buffer all data
			this._buffer_whole_file(io)

			# Upload the whole file
			class Fwrapper(object):
				def __init__(this, block_cache):
					this.block_cache = block_cache
					this.size = block_cache.get_size()
					this.f = this.block_cache.get_file()
					this.f.seek(0)
				def __len__(this):
					return this.size
				def read(this, size):
					return this.f.read(size)

			if parent_cap is None:
				upath = this.upath
				iscap = False
			else:
				upath = parent_cap + "/" + ubasename(this.upath)
				iscap = True

			fw = Fwrapper(this.block_cache)
			try:
				filecap = io.put_file(upath, fw, iscap=iscap)
			except (HTTPError, IOError) as err:
				raise IOError(errno.EFAULT, "I/O error: %s" % (str(err),))

			this.info[1]['ro_uri'] = filecap
			this.info[1]['size'] = this.get_size()
			this._save_info()

			this.dirty = False

			return filecap

	def unlink(this):
		with this.cache_lock, this.lock:
			if this.upath is not None and not this.invalidated:
				os.unlink(this.f.path)
				os.unlink(this.f_state.path)
				os.unlink(this.f_data.path)
			this.upath = None


class CachedDirInode(object):
	"""
	Logical file on-disk directory. There should be only a single CachedDirInode
	instance is per each logical directory.
	"""

	def __init__(this, cachedb, upath, io, dircap=None):
		this.upath = upath
		this.closed = False
		this.refcnt = 0
		this.lock = threading.RLock()
		this.invalidated = False

		this.filename, this.key = cachedb.get_filename_and_key(upath)

		try:
			with CryptFile(this.filename, key=this.key, mode='rb') as f:
				this.info = json_zlib_load(f)
			os.utime(this.filename, None)
			return
		except (IOError, OSError, ValueError):
			pass

		f = CryptFile(this.filename, key=this.key, mode='w+b')
		try:
			if dircap is not None:
				this.info = io.get_info(dircap, iscap=True)
			else:
				this.info = io.get_info(upath)
			this.info[1]['retrieved'] = time.time()
			json_zlib_dump(this.info, f)
		except (HTTPError, IOError, ValueError):
			os.unlink(this.filename)
			raise IOError(errno.EREMOTEIO, "failed to retrieve information")
		finally:
			f.close()

	def _save_info(this):
		with CryptFile(this.filename, key=this.key, mode='w+b') as f:
			json_zlib_dump(this.info, f)

	def is_fresh(this, lifetime):
		return (this.info[1]['retrieved'] + lifetime >= time.time())

	def incref(this):
		with this.lock:
			this.refcnt += 1

	def decref(this):
		with this.lock:
			this.refcnt -= 1
			if this.refcnt <= 0:
				this.close()

	def close(this):
		with this.lock:
			this.closed = True

	def listdir(this):
		return list(this.info[1]['children'].keys())

	def get_attr(this):
		return dict(type='dir')

	def get_child_attr(this, childname):
		assert isinstance(childname, str)
		children = this.info[1]['children']
		if childname not in children:
			raise IOError(errno.ENOENT, "no such entry")

		info = children[childname]

		# tahoe:linkcrtime doesn't exist for entries created by "tahoe backup",
		# but explicit 'mtime' and 'ctime' do, so use them.
		ctime = info[1]['metadata'].get('tahoe', {}).get('linkcrtime')
		mtime = info[1]['metadata'].get('tahoe', {}).get('linkcrtime')   # should this be 'linkmotime'?
		if ctime is None:
			ctime = info[1]['metadata']['ctime']
		if mtime is None:
			mtime = info[1]['metadata']['mtime']

		if info[0] == 'dirnode':
			return dict(type='dir', 
						ro_uri=info[1]['ro_uri'],
						rw_uri=info[1].get('rw_uri'),
						ctime=ctime,
						mtime=mtime)
		elif info[0] == 'filenode':
			return dict(type='file',
						size=info[1]['size'],
						ro_uri=info[1]['ro_uri'],
						rw_uri=info[1].get('rw_uri'),
						ctime=ctime,
						mtime=mtime)
		else:
			raise IOError(errno.ENOENT, "invalid entry")

	def unlink(this):
		if this.upath is not None and not this.invalidated:
			os.unlink(this.filename)
		this.upath = None

	def cache_add_child(this, basename, cap, size):
		children = this.info[1]['children']

		if basename in children:
			info = children[basename]
		else:
			if cap is not None and cap.startswith('URI:DIR'):
				info = ['dirnode', {'metadata': {'tahoe': {'linkcrtime': time.time()}}}]
			else:
				info = ['filenode', {'metadata': {'tahoe': {'linkcrtime': time.time()}}}]

		if info[0] == 'dirnode':
			info[1]['ro_uri'] = cap
			info[1]['rw_uri'] = cap
		elif info[0] == 'filenode':
			info[1]['ro_uri'] = cap
			info[1]['size'] = size

		children[basename] = info
		this._save_info()

	def cache_remove_child(this, basename):
		children = this.info[1]['children']
		if basename in children:
			del children[basename]
			this._save_info()


class RandomString(object):
	def __init__(this, size):
		this.size = size

	def __len__(this):
		return this.size

	def __getitem__(this, k):
		if isinstance(k, slice):
			return os.urandom(len(range(*k.indices(this.size))))
		else:
			raise IndexError("invalid index")


def json_zlib_dump(obj, fp):
	try:
		fp.write(zlib.compress(json.dumps(obj).encode('utf-8'), 3))
	except zlib.error:
		raise ValueError("compression error")


def json_zlib_load(fp):
	try:
		return json.load(ZlibDecompressor(fp))
	except zlib.error:
		raise ValueError("invalid compressed stream")


class ZlibDecompressor(object):
	def __init__(this, fp):
		this.fp = fp
		this.decompressor = zlib.decompressobj()
		this.buf = b""
		this.eof = False

	def read(this, sz=None):
		if sz is not None and not (sz > 0):
			return b""

		while not this.eof and (sz is None or sz > len(this.buf)):
			block = this.fp.read(131072)
			if not block:
				this.buf += this.decompressor.flush()
				this.eof = True
				break
			this.buf += this.decompressor.decompress(block)

		if sz is None:
			block = this.buf
			this.buf = b""
		else:
			block = this.buf[:sz]
			this.buf = this.buf[sz:]
		return block


def udirname(upath):
	return "/".join(upath.split("/")[:-1])


def ubasename(upath):
	return upath.split("/")[-1]


# constants for cache score calculation
_DOWNLOAD_SPEED = 1e6  # byte/sec
_LATENCY = 1.0 # sec

def _access_rate(size, t):
	"""Return estimated access rate (unit 1/sec). `t` is time since last access"""
	if t < 0:
		return 0.0
	size_unit = 100e3
	size_prob = 1 / (1 + (size/size_unit)**2)
	return size_prob / (_LATENCY + t)

def cache_score(size, t):
	"""
	Return cache score for file with size `size` and time since last access `t`.
	Bigger number means higher priority.
	"""

	# Estimate how often it is downloaded
	rate = _access_rate(size, t)

	# Maximum size up to this time
	dl_size = _DOWNLOAD_SPEED * max(0, t - _LATENCY)

	# Time cost for re-retrieval
	return rate * (_LATENCY + min(dl_size, size) / _DOWNLOAD_SPEED)


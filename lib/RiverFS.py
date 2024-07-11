"""
Cache metadata and data of a directory tree for read-only access.
"""

import eons
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

from .Tahoe import *
from .Crypt import *
# .block.Cache
from .block.Cache import *


class RiverFS(eons.Functor):
	def __init__(this, name="RiverFS"):

		super().__init__(name)

		this.arg.kw.required.append('path')
		this.arg.kw.required.append('rootcap')

		this.arg.kw.optional["node_url"] = "http://127.0.0.1:3456"
		this.arg.kw.optional["cache_dir"] = Path(".tahoe-cache")
		this.arg.kw.optional["cache_data"] = False
		this.arg.kw.optional["cache_size"] = "1GB"
		this.arg.kw.optional["write_lifetime"] = "10" #Cache lifetime for write operations (seconds).
		this.arg.kw.optional["read_lifetime"] = "10" #Cache lifetime for read operations (seconds).
		this.arg.kw.optional["timeout"] = "30" #Network timeout (seconds).

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


	def ValidateArgs(this):
		super().ValidateArgs()

		assert isinstance(this.rootcap, str)

		try:
			this.cache_size = parse_size(this.cache_size)
		except ValueError:
			raise eons.MissingArgumentError(f"error: --cache-size {this.cache_size} is not a valid size specifier")
	
		try:
			this.read_lifetime = parse_lifetime(this.read_lifetime)
		except ValueError:
			raise eons.MissingArgumentError(f"error: --read-cache-lifetime {this.read_lifetime} is not a valid lifetime")

		try:
			this.write_lifetime = parse_lifetime(this.write_lifetime)
		except ValueError:
			raise eons.MissingArgumentError(f"error: --write-cache-lifetime {this.write_lifetime} is not a valid lifetime")

		try:
			this.timeout = float(this.timeout)
			if not 0 < this.timeout < float('inf'):
				raise ValueError()
		except ValueError:
			raise eons.MissingArgumentError(f"error: --timeout {this.timeout} is not a valid timeout")

		this.rootcap = this.rootcap.strip()

		Path(this.cache_dir).mkdir(parents=True, exist_ok=True)


	def _generate_prk(this, rootcap):
		# Cache master key is derived from hashed rootcap and salt via
		# PBKDF2, with a fixed number of iterations.
		#
		# The master key, combined with a second different salt, are
		# used to generate per-file keys via HKDF-SHA256

		# Get salt
		salt_fn = os.path.join(this.cache_dir, 'salt')
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

			files = [os.path.join(this.cache_dir, fn) 
					 for fn in os.listdir(this.cache_dir) 
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
			dead_file_set = os.listdir(this.cache_dir)
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
			fn = os.path.join(this.cache_dir, basename)
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
		return os.path.join(this.cache_dir, fn), key

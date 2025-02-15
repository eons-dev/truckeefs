"""
lib/RiverFS.py

Purpose:
Implements the RiverFS executor—a generic file system that adds caching (and formerly encryption) on top of a remote TahoeFS backend.

Place in Architecture:
Acts as the central coordinator for operations. It initializes network connections (via TahoeConnection), sets up caching directories, manages process state, and serves as the “brain” behind the file system’s asynchronous operations.

Interface:

    Inherits from eons.Executor.
    Sets up required and optional arguments (e.g. path, rootcap, cache_dir, cache_size, etc.).
    Methods include:
        ValidateArgs(): Checks and converts arguments (e.g. cache sizes, TTL).
        BeforeFunction(): Initializes the TahoeConnection and starts RiverDelta.
        Function(): (Abstract) to be implemented by child classes.
        Helper methods like GetCachedInodeByUpath(), CacheInode(), GetSourceConnection(), GeneratePrivateKey(), WalkCache(), RestrictCacheSize(), InvalidateCache(), LookupCap(), and GetFileNameAndKey().

TODOs/FIXMEs:

    No explicit TODOs, but note that key derivation and encryption aspects (e.g. in GeneratePrivateKey) may need revisiting if you drop cryptographic features.
    Documentation comments note that certain state fields must remain immutable after startup for thread safety.
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

from .FileOnDisk import *
from .block.Cache import *


# RiverFS is a generic file system that adds caching and encryption to a remote file system, specifically TahoeFS.
# All operations should be asynchronous, stateless and scalable, with all state stored in *this.
# NOTE: For thread safety, it is illegal to write to any RiverFS args after it has been started.
# This class is functionally abstract and requires child classes to implement the Function method.
class RiverFS(eons.Executor):
	def __init__(this, name="RiverFS"):

		super().__init__(name)

		this.arg.kw.static.append('path')
		this.arg.kw.static.append('rootcap')

		this.arg.kw.optional["node_url"] = "http://127.0.0.1:3456"
		this.arg.kw.optional["cache_dir"] = Path(".tahoe-cache")
		this.arg.kw.optional["cache_data"] = False
		this.arg.kw.optional["cache_size"] = "0" # Maximum size of the cache. 0 means no limit.
		this.arg.kw.optional["cache_ttl"] = "10" # Cache lifetime for filesystem objects (seconds).
		this.arg.kw.optional["net_timeout"] = "30" # Network timeout (seconds).
		this.arg.kw.optional["work_dir"] = Path(".tahoe-work") # Where temporary files are stored while they wait to be uploaded.

		this.last_size_check_time = 0

		# Cache lock
		this.lock = threading.RLock()

		# Open files and dirs
		this.open_items = {}

		# Restrict cache size
		this.RestrictCacheSize()

		# Directory cache
		this._max_item_cache = 500
		this._item_cache = []

		this.rootId = 1

	# ValidateArgs is automatically called before Function, per eons.Functor.
	def ValidateArgs(this):
		super().ValidateArgs()

		assert isinstance(this.rootcap, str)

		try:
			this.cache_size = parse_size(this.cache_size)
		except ValueError:
			raise eons.MissingArgumentError(f"error: --cache-size {this.cache_size} is not a valid size specifier")
	
		try:
			this.cache_ttl = parse_lifetime(this.cache_ttl)
		except ValueError:
			raise eons.MissingArgumentError(f"error: --cache-ttl {this.cache_ttl} is not a valid lifetime")

		try:
			this.net_timeout = float(this.net_timeout)
			if not 0 < this.net_timeout < float('inf'):
				raise ValueError()
		except ValueError:
			raise eons.MissingArgumentError(f"error: --net-timeout {this.net_timeout} is not a valid timeout")

		this.rootcap = this.rootcap.strip()
		this.key, this.salt_hkdf = this.GeneratePrivateKey(this.rootcap)

		Path(this.cache_dir).mkdir(parents=True, exist_ok=True)


	def BeforeFunction(this):
		this.source  = TahoeConnection(
			this.node_url,
			this.rootcap,
			this.net_timeout
		)

		this.delta = RiverDelta()
		this.delta() # Start the RiverDelta
	
	
	# Override this in your child class.
	def Function(this):
		pass

	
	# Thread safe means of checking if an Inode already exists for the given upath
	def GetCachedInodeByUpath(this, upath):
		with this.lock:
			for fun in executor.cache.functors:
				if (isinstance(fun, Inode) and upath in fun.upaths):
					return fun


	# Thread safe means of checking if an Inode already exists for the given id
	def GetCachedInodeById(this, id):
		with this.lock:
			for fun in executor.cache.functors:
				if (isinstance(fun, Inode) and id == fun.id):
					return fun


	# Thread safe means of caching a new Inode
	def CacheInode(this, inode):
		with this.lock:
			executor.cache.functors.append(Inode)

			# If running RiverFS in a multi-server deployment, the inode may have been initialized on another server.
			if (not inode.AreProcessStatesInitialized()):
				inode.InitializeProcessStates()
				inode.InitializeEphemerals()


	def GetDatabaseSession(this):
		return this.delta.sql

	def GetSourceConnection(this):
		return this.source

	def GetUpathRootId(this):
		return this.rootId

	# Cache master key is derived from hashed rootcap and salt via
	# PBKDF2, with a fixed number of iterations.
	#
	# The master key, combined with a second different salt, are
	# used to generate per-file keys via HKDF-SHA256
	def GeneratePrivateKey(this, rootcap):
		# Get salt
		salt_fn = os.path.join(this.cache_dir, 'salt')
		try:
			with open(salt_fn, 'rb') as file:
				numiter = file.read(4)
				salt = file.read(32)
				salt_hkdf = file.read(32)
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
			with open(salt_fn, 'wb') as file:
				file.write(struct.pack('<I', numiter))
				file.write(salt)
				file.write(salt_hkdf)

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

	def WalkCache(this, root_upath=""):
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
		fn, key = this.GetFileNameAndKey(root_upath)
		if os.path.isfile(fn):
			stack.append((root_upath, fn, key))

		# Walk the tree
		while stack:
			upath, fn, key = stack.pop()

			if not os.path.isfile(fn):
				continue

			try:
				with FileOnDisk(fn, key=key, mode='rb') as file:
					data = json_zlib_load(file)
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
					c_fn, c_key = this.GetFileNameAndKey(c_upath)
					if os.path.isfile(c_fn):
						stack.append((c_upath, c_fn, c_key))
				elif c_info[0] == 'filenode':
					for ext in (None, b'state', b'data'):
						c_fn, c_key = this.GetFileNameAndKey(c_upath, ext=ext)
						yield (os.path.basename(c_fn), c_upath)

	def RestrictCacheSize(this):
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

	def InvalidateCache(this, root_upath="", shallow=False):
		if root_upath == "" and not shallow:
			for file in this.open_items.values():
				file.invalidated = True
			this.open_items = {}
			dead_file_set = os.listdir(this.cache_dir)
		else:
			dead_file_set = set()
			for fn, upath in this.WalkCache(root_upath):
				file = this.open_items.pop(upath, None)
				if file is not None:
					file.invalidated = True
				dead_file_set.add(fn)
				if shallow and upath != root_upath:
					break

		for basename in dead_file_set:
			if basename == 'salt':
				continue
			fn = os.path.join(this.cache_dir, basename)
			if os.path.isfile(fn):
				os.unlink(fn)

	def LookupCap(this, upath, io, read_only=True, lifetime=None):
		if lifetime is None:
			lifetime = this.cache_ttl

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

	def GetFileNameAndKey(this, upath, ext=None):
		path = upath.encode('utf-8')
		nonpath = b"//\x00" # cannot occur in path, which is normalized

		# Generate per-file key material via HKDF
		info = path
		if ext is not None:
			info += nonpath + ext

		hkdf = HKDF(
			algorithm=hashes.SHA256(),
			length=3*32,
			salt=this.salt_hkdf,
			info=info,
			backend=backend
		)
		data = hkdf.derive(this.key)

		# Generate key
		key = data[:32]

		# Generate filename
		h = hmac.HMAC(key=data[32:], algorithm=hashes.SHA512(), backend=backend)
		h.update(info)
		fn = codecs.encode(h.finalize(), 'hex_codec').decode('ascii')
		return os.path.join(this.cache_dir, fn), key

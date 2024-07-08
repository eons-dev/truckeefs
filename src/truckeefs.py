import os
import re
import eons
import sys
import errno
import stat
import traceback
import threading
import logging
from pathlib import Path

import fuse

from libtruckeefs import CacheDB, TahoeConnection

print_lock = threading.Lock()


def ioerrwrap(func):
	def wrapper(*a, **kw):
		try:
			return func(*a, **kw)
		except (IOError, OSError) as e:
			with print_lock:
				if isinstance(e, IOError) and getattr(e, 'errno', None) == errno.ENOENT:
					logging.debug("Failed operation", exc_info=True)
				else:
					logging.info("Failed operation", exc_info=True)
			if hasattr(e, 'errno') and isinstance(e.errno, int):
				# Standard operation
				return -e.errno
			return -errno.EACCES
		except:
			with print_lock:
				logging.warning("Unexpected exception", exc_info=True)
			return -errno.EIO

	wrapper.__name__ = func.__name__
	wrapper.__doc__ = func.__doc__
	return wrapper


class TruckeeFS(eons.Functor, fuse.Fuse):
	def __init__(this, name="TruckeeFS"):
		super(TruckeeFS, this).__init__(name)

		this.arg.kw.required.append("rootcap")
		this.arg.kw.required.append("mount")

		this.arg.kw.optional["node_url"] = "http://127.0.0.1:3456"
		this.arg.kw.optional["cache_dir"] = Path(".tahoe-cache")
		this.arg.kw.optional["cache_data"] = False
		this.arg.kw.optional["cache_size"] = "1GB"
		this.arg.kw.optional["write_lifetime"] = "10" #Cache lifetime for write operations (seconds).
		this.arg.kw.optional["read_lifetime"] = "10" #Cache lifetime for read operations (seconds).
		this.arg.kw.optional["timeout"] = "30" #Network timeout (seconds).
		
		# Supported FUSE args
		this.arg.kw.optional["multithreaded"] = True
		this.arg.kw.optional["fuse_default_permissions"] = False
		this.arg.kw.optional["fuse_allow_other"] = False
		this.arg.kw.optional["fuse_uid"] = 0
		this.arg.kw.optional["fuse_gid"] = 0
		this.arg.kw.optional["fuse_fmask"] = 0o764
		this.arg.kw.optional["fuse_dmask"] = 0o755

	def ValidateArgs(this):
		super().ValidateArgs()

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


	def Function(this):

		this.cache = CacheDB(
			this.cache_dir, 
			this.rootcap,
			this.node_url,
			cache_size=this.cache_size, 
			cache_data=this.cache_data,
			read_lifetime=this.read_lifetime,
			write_lifetime=this.write_lifetime
		)

		this.io = TahoeConnection(
			this.node_url,
			this.rootcap,
			this.timeout
		)

		this.fuse_args = eons.util.DotDict({
			'fsname': 'truckeefs',
			'foreground': True,
			'direct_io': True,
			'allow_other': this.fuse_allow_other,
			'default_permissions': this.fuse_default_permissions,
			'uid': this.fuse_uid,
			'gid': this.fuse_gid,
			'fmask': this.fuse_fmask,
			'dmask': this.fuse_dmask,
		})

		this.fuse_args = fuse.FuseArgs()
		this.fuse_args.mountpoint = this.mount
		
		# TODO: FUSE is bugged:
		#   File "/usr/local/lib/python3.10/dist-packages/fuseparts/subbedopts.py", line 50, in canonify
		#    for k, v in self.optdict.items():
		#	RuntimeError: dictionary changed size during iteration
		#
		# this.fuse_args.optdict = {
		# 	'fsname': 'truckeefs',
		# 	'foreground': True,
		# 	'direct_io': True,
		# 	'allow_other': this.fuse_allow_other,
		# 	'default_permissions': this.fuse_default_permissions,
		# 	'uid': this.fuse_uid,
		# 	'gid': this.fuse_gid,
		# 	'fmask': this.fuse_fmask,
		# 	'dmask': this.fuse_dmask,
		# }
		
		fuse.Fuse.main(this)

	# -- Directory handle ops

	@ioerrwrap
	def readdir(this, path, offset):
		upath = this.cache.get_upath(path)

		entries = [fuse.Direntry('.'),
				   fuse.Direntry('..')]

		f = this.cache.open_dir(upath, this.io)
		try:
			for c in f.listdir():
				entries.append(fuse.Direntry(this.cache.path_from_upath(c)))
		finally:
			this.cache.close_dir(f)

		return entries

	# -- File ops

	@ioerrwrap
	def open(this, path, flags):
		upath = this.cache.get_upath(path)
		basename = os.path.basename(upath)
		if basename == '.truckeefs-invalidate' and (flags & os.O_CREAT):
			this.cache.invalidate(os.path.dirname(upath))
			return -errno.EACCES
		return this.cache.open_file(upath, this.io, flags)

	@ioerrwrap
	def release(this, path, flags, f):
		upath = this.cache.get_upath(path)
		try:
			# XXX: if it fails, silent data loss (apart from logs)
			this.cache.upload_file(f, this.io)
			return 0
		finally:
			this.cache.close_file(f)

	@ioerrwrap
	def read(this, path, size, offset, f):
		upath = this.cache.get_upath(path)
		return f.read(this.io, offset, size)

	@ioerrwrap
	def create(this, path, flags, mode):
		# no support for mode in Tahoe, so discard it
		return this.open(path, flags)
 
	@ioerrwrap
	def write(this, path, data, offset, f):
		upath = this.cache.get_upath(path)
		this.io.wait_until_write_allowed()
		f.write(this.io, offset, data)
		return len(data)

	@ioerrwrap
	def ftruncate(this, path, size, f):
		f.truncate(size)
		return 0

	@ioerrwrap
	def truncate(this, path, size):
		upath = this.cache.get_upath(path)

		f = this.cache.open_file(upath, this.io, os.O_RDWR)
		try:
			f.truncate(size)
			this.cache.upload_file(f, this.io)
		finally:
			this.cache.close_file(f)
		return 0

	# -- Handleless ops

	@ioerrwrap
	def getattr(this, path):
		upath = this.cache.get_upath(path)

		info = this.cache.get_attr(upath, this.io)

		if info['type'] == 'dir':
			st = fuse.Stat()
			st.st_mode = stat.S_IFDIR | stat.S_IRUSR | stat.S_IXUSR
			st.st_nlink = 1
		elif info['type'] == 'file':
			st = fuse.Stat()
			st.st_mode = stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR
			st.st_nlink = 1
			st.st_size = info['size']
			st.st_mtime = info['mtime']
			st.st_ctime = info['ctime']
		else:
			return -errno.EBADF

		return st

	@ioerrwrap
	def unlink(this, path):
		upath = this.cache.get_upath(path)
		this.cache.unlink(upath, this.io, is_dir=False)
		return 0

	@ioerrwrap
	def rmdir(this, path):
		upath = this.cache.get_upath(path)
		this.cache.unlink(upath, this.io, is_dir=True)
		return 0

	@ioerrwrap
	def mkdir(this, path, mode):
		# *mode* is dropped; not supported on tahoe
		upath = this.cache.get_upath(path)
		this.cache.mkdir(upath, this.io)
		return 0


def parse_size(size_str):
	multipliers = {
		't': 1000**4,
		'g': 1000**3,
		'm': 1000**2,
		'k': 1000**1,
		'tb': 1000**4,
		'gb': 1000**3,
		'mb': 1000**2,
		'kb': 1000**1,
		'tib': 1024**4,
		'gib': 1024**3,
		'mib': 1024**2,
		'kib': 1024**1,
	}
	size_re = re.compile(r'^\s*(\d+)\s*(%s)?\s*$' % ("|".join(list(multipliers.keys())),), 
						 re.I)

	m = size_re.match(size_str)
	if not m:
		raise ValueError("not a valid size specifier")

	size = int(m.group(1))
	multiplier = m.group(2)
	if multiplier is not None:
		try:
			size *= multipliers[multiplier.lower()]
		except KeyError:
			raise ValueError("invalid size multiplier")

	return size


def parse_lifetime(lifetime_str):
	if (type(lifetime_str) == int):
		return lifetime_str

	if lifetime_str.lower() in ('inf', 'infinity', 'infinite'):
		return 100*365*24*60*60

	try:
		return int(lifetime_str)
	except ValueError:
		raise ValueError("invalid lifetime specifier")
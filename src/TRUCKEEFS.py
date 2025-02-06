import eons
import os
import sys
import fuse
import logging
import stat
import traceback
import threading
import fuse

from pathlib import Path
from libtruckeefs import RiverFS, TahoeConnection

from .Utils import *
from .FuseMethod import *

fuse.fuse_python_api = (0, 2)

# TruckeeFS is a RiverFS that mounts to the local filesystem.
# Name is caps to make it executable per eons weirdness.
class TRUCKEEFS(RiverFS, fuse.Fuse):
	def __init__(this, name="TruckeeFS"):
		super(TRUCKEEFS, this).__init__(name)

		this.arg.kw.required.append("mount")

		this.arg.kw.optional["daemon"] = False
		
		# Supported FUSE args
		this.arg.kw.optional["multithreaded"] = True
		this.arg.kw.optional["fuse_default_permissions"] = False
		this.arg.kw.optional["fuse_allow_other"] = False
		this.arg.kw.optional["fuse_uid"] = 0
		this.arg.kw.optional["fuse_gid"] = 0
		this.arg.kw.optional["fuse_fmask"] = 0o764
		this.arg.kw.optional["fuse_dmask"] = 0o755


	def Function(this):

		this.fuse_args = fuse.FuseArgs()
		this.fuse_args.mountpoint = this.mount

		if (not this.daemon):
			this.fuse_args.setmod('foreground')
		
		# NOTE: FUSE is bugged:
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

	@FuseMethod
	def readdir(this, path, offset):
		upath = this.get_upath(path)

		entries = [fuse.Direntry('.'),
				   fuse.Direntry('..')]

		f = this.open_dir(upath, this.source )
		try:
			for c in f.listdir():
				entries.append(fuse.Direntry(this.path_from_upath(c)))
		finally:
			this.close_dir(f)

		return entries

	@FuseMethod
	def rmdir(this, path):
		upath = this.get_upath(path)
		this.unlink(upath, this.source , is_dir=True)
		return 0

	@FuseMethod
	def mkdir(this, path, mode):
		# *mode* is dropped; not supported on tahoe
		upath = this.get_upath(path)
		this.mkdir(upath, this.source )
		return 0

	# -- File ops

	@FuseMethod
	def open(this, path, flags):
		upath = this.get_upath(path)
		basename = os.path.basename(upath)
		if basename == '.truckeefs-invalidate' and (flags & os.O_CREAT):
			this.invalidate(os.path.dirname(upath))
			return -errno.EACCES
		return this.open_file(upath, this.source , flags)

	@FuseMethod
	def release(this, path, flags, f):
		upath = this.get_upath(path)
		try:
			# XXX: if it fails, silent data loss (apart from logs)
			this.upload_file(f, this.source )
			return 0
		finally:
			this.close_file(f)

	@FuseMethod
	def read(this, path, size, offset, f):
		upath = this.get_upath(path)
		return f.read(this.source , offset, size)

	@FuseMethod
	def create(this, path, flags, mode):
		# no support for mode in Tahoe, so discard it
		return this.open(path, flags)
 
	@FuseMethod
	def write(this, path, data, offset, f):
		upath = this.get_upath(path)
		this.source .wait_until_write_allowed()
		f.write(this.source , offset, data)
		return len(data)

	@FuseMethod
	def ftruncate(this, path, size, f):
		f.truncate(size)
		return 0

	@FuseMethod
	def truncate(this, path, size):
		upath = this.get_upath(path)

		f = this.open_file(upath, this.source , os.O_RDWR)
		try:
			f.truncate(size)
			this.upload_file(f, this.source )
		finally:
			this.close_file(f)
		return 0

	# -- Handleless ops

	@FuseMethod
	def getattr(this, path):
		upath = this.get_upath(path)

		info = this.get_attr(upath, this.source )

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

	@FuseMethod
	def unlink(this, path):
		upath = this.get_upath(path)
		this.unlink(upath, this.source , is_dir=False)
		return 0

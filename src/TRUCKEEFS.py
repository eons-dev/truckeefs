"""tahoestaticfs [options] [mountpoint]

Tahoe-LAFS directory mounted as a filesystem, with local
caching. Cached data is encrypted with a key derived from the
directory capability mounted.

Dircap of the root directory is read from stdin on startup. In scripts, do::

	awk '/^root:/ {print $2}' < ~/.tahoe/private/aliases \\
		| tahoestaticfs ...

Cache can be invalidated by `touch <mountpoint>/.tahoestaticfs-invalidate`,
or by removing files in the cache directory.

"""
import eons
import os
import sys
import fuse
import logging

from .staticfs import TahoeStaticFS

fuse.fuse_python_api = (0, 2)

class TRUCKEEFS(eons.Executor):
	def __init__(this):
		super().__init__()

	def Function(this):
		usage = __doc__.strip()
		usage += "".join(fuse.Fuse.fusage.splitlines(1)[2:])
		fs = TahoeStaticFS(version="0.0.1", usage=usage, dash_s_do='undef')
		fs.parse(errex=1)
		fs.main()

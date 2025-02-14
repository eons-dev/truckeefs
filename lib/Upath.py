"""
lib/Upath.py

Purpose:
Implements a universal path class that normalizes file system paths (upaths) in a consistent manner across platforms.

Place in Architecture:
Used throughout the code to ensure that paths are always handled in a uniform “universal” (forward-slash) format.

Interface:

	__init__(path=""): Constructs a UniversalPath from a string or another UniversalPath.
	__str__(): Returns the normalized upath as a string.
	FromPath(path): Normalizes a given system path.
	AsPath(): Returns the upath in system-path format.
	GetParent(): Returns the parent upath.
	encode(...): Compatibility for string encoding.

TODOs/FIXMEs:
None noted.
"""

import os

class UniversalPath:
	def __init__(this, path=""):
		if (isinstance(path, UniversalPath)):
			this.upath = path.upath
		elif (isinstance(path, str)):
			this.FromPath(path)

	def __str__(this):
		return this.upath

	def FromPath(this, path):
		assert isinstance(path, str)
		try:
			path = os.path.normpath(path)
			this.upath = path.replace(os.sep, "/").lstrip('/')
		except UnicodeError:
			raise IOError(errno.ENOENT, "file does not exist")

	def AsPath(this):
		return this.upath.replace(os.sep, "/")

	def GetParent(this):
		return upath(os.path.dirname(this.upath))

	# Compatibility for str method.
	def encode(this, encoding='utf-8'):
		return this.upath.encode(encoding)
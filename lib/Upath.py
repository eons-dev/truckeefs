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
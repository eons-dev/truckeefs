import errno

def FuseMethod(func):
	def wrapper(*a, **kw):
		try:
			return func(*a, **kw)
		except (IOError, OSError) as e:
			logging.debug("Failed operation", exc_info=True)
			
			if hasattr(e, 'errno') and isinstance(e.errno, int):
				# Standard operation
				return -e.errno
			return -errno.EACCES

		except:
			logging.warning("Unexpected exception", exc_info=True)
			return -errno.EIO

	wrapper.__name__ = func.__name__
	wrapper.__doc__ = func.__doc__
	return wrapper
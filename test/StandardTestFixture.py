import logging
import pytest
import tempfile
import shutil
import os
from Includes import Include, GetIncludePath

class StandardTestFixture(object):

	@staticmethod
	def assert_equal(a, b, msg=""):
		assert a == b, msg


	@staticmethod
	def assert_raises(exc, func, *a, **kw):
		with pytest.raises(exc):
			func(*a, **kw)


	# Pytest skips classes with __init__ methods.
	# That's dumb.
	# It seems like the best we can do atm is add our members as class members which should be re-instantiated before every test.
	@classmethod
	def setup_class(cls):
		cls.Constructor()

	
	# Also supply destructor.
	@classmethod
	def teardown_class(cls):
		cls.Destructor()


	@classmethod # this is a lie.
	def Constructor(this):
		logging.debug(f"Constructing {this.__name__}")
		this.tempdir = tempfile.mkdtemp()
		this.file_name = os.path.join(this.tempdir, 'test.dat')		

	
	@classmethod # this is a lie.
	def Destructor(this):
		logging.debug(f"Destructing {this.__name__}")
		shutil.rmtree(this.tempdir)

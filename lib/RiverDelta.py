"""
lib/RiverDelta.py

Purpose:
Tracks changes in the local/cache file system relative to the remote Tahoe state. It uses SQLAlchemy for persistent metadata storage and Redis for fast, transient state (such as semaphore locks).

Place in Architecture:
Acts as the “delta” engine that decides what needs syncing. It’s responsible for conflict detection and ensuring that the local authoritative state is maintained.

Interface:

	__init__(...): Sets up required SQL and Redis connection parameters.
	Function(): Initializes SQLAlchemy and Redis connections.
	Methods:
		GetState(inode, process) / SetState(inode, process, state, expectedState=None): Get/set process states (read, write, sync).
		GetRedisInodeValue(inode, key, coerceType=None) / SetRedisInodeValue(inode, key, value, expectedValue=None): Low-level access to ephemeral inode data in Redis.

TODOs/FIXMEs:

	The comments warn that conflicts with remote changes are not resolved and that remote changes may be clobbered—this is an area for future improvement if needed.
"""


import eons
import logging
import sqlalchemy
import redis

# The River Delta tracks what has been changed on the local / cache filesystem compared to the remote.
# It is backed by a remote database for persistence and scalability.
# NOTE: This class assumes that it is the authority on what the remote state should be.
#   If there are changes on the remote, this class WILL NOT resolve the conflict & the behavior of the resulting filesystem will be undefined.
#   Likely, *this will simply clobber the remote changes.
#
# RiverFS uses 3 distinct processes to manage concurrent inode operations: Reads, Writes, and Syncs.
# Reading and writing these semaphores needs to be fast, so we use Redis, instead of mysql.
# To query one of these semaphores, use GetState(), below.
# To set one of these semaphores, use SetState(), below.
#
class RiverDelta(eons.Functor):
	def __init__(this, name="River Delta"):
		super().__init__(name)

		this.arg.kw.required.append("sql_host")
		this.arg.kw.required.append("sql_db")
		this.arg.kw.required.append("sql_user")
		this.arg.kw.required.append("sql_pass")

		this.arg.kw.required.append("redis_host")
		
		this.arg.kw.optional["sql_engine"] = "mysql"
		this.arg.kw.optional["sql_port"] = 3306
		this.arg.kw.optional["sql_ssl"] = False

		this.arg.kw.optional["redis_port"] = 6379
		this.arg.kw.optional["redis_db"] = 0
		this.arg.kw.optional["redis_semaphore_timeout"] = 1800 # Timeout for semaphore locks (seconds). Should only be used if a server crashed, etc.

		this.sql = None
		this.redis = None

	def Function(this):
		this.sql = sqlalchemy.create_engine(f"{this.sql_engine}://{this.sql_user}:{this.sql_pass}@{this.sql_host}:{this.sql_port}/{this.sql_db}?ssl={this.sql_ssl}")
		this.redis = redis.Redis(host=this.redis_host, port=this.redis_port, db=this.redis_db)
		
	
	# Get the state of a process on an inode.
	# RETURNS the state for the given process on the given inode or None if there was an error.
	def GetState(this, inode, process):
		try:
			return ProcessState(int(this.GetRedisInodeValue(inode, process)))
		except Exception as e:
			logging.error(f"Error getting state for {inode}:{process}: {e}")
			return None

	# Set the state of a process on an inode.
	# For extra safety, you can pass in what you think the current state is. If it's not what you expect, the state will not be set.
	def SetState(this, inode, process, state, expectedState=None):
		stateValue = str(state.value)  # Store the enum value as a string
		expectedStateValue = str(expectedState.value) if expectedState is not None else None
		return this.SetRedisInodeValue(inode, process, stateValue, expectedStateValue)


	# Get a value for a key on an inode in Redis.
	# If you need to coerce the value to a specific type, pass in the type as coerceType.
	# RETURNS the value for the given key on the given inode or None if there was an error.
	def GetRedisInodeValue(this, inode, key, coerceType=None):
		try:
			ret = this.redis.get(f"{inode}:{key}")
			if (coerceType is not None):
				ret = coerceType(ret)
		except Exception as e:
			logging.error(f"Error getting value for {inode}:{key}: {e}")
			return None

	# Set a value for a key on an inode in Redis.
	# For extra safety, you can pass in what you think the current value is. If it's not what you expect, the value will not be set.
	# For example, if a long time has passed between when you last checked the value and when you set it, you can use the return value of this method let you know if you need to recheck your data.
	# RETURNS True if the value was set, False otherwise.
	def SetRedisInodeValue(this, inode, key, value, expectedValue=None):
		ret = False

		if (expectedValue is not None):
			lua = """\
if redis.call('GET', KEYS[1]) == ARGV[1] then
	result = redis.call('SET', KEYS[1], ARGV[2])
	if (result == 'OK') then
		redis.call('PEXPIRE', KEYS[1], ARGV[3])
		return 1
	end
else
	return 0
end
"""
			try:
				result = this.redis.eval(lua, 1, f"{inode}:{key}", expectedValue, value, this.redis_semaphore_timeout * 1000)
				ret = result == 1 and this.GetRedisInodeValue(inode, key) == value
			except Exception as e:
				logging.error(f"Error setting value for {inode}:{key} to {value}: {e}")
				ret = False

		else:
			try:
				this.redis.set(f"{inode}:{key}", value, ex=this.redis_semaphore_timeout)
				ret = this.GetRedisInodeValue(inode, key) == value
			except Exception as e:
				logging.error(f"Error setting value for {inode}:{key} to {value}: {e}")
				ret = False

		return ret
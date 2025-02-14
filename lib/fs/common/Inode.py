"""
lib/fs/common/Inode.py

Purpose:
Provides the base class for all filesystem inodes (files and directories). It handles persistence, caching, state management, and synchronization hooks.

Place in Architecture:
A core part of the FS layer. It abstracts common functionality for both file and directory inodes, including process state management (using Redis via RiverDelta) and database interactions.

Interface:

	__init__(upath, name): Initializes an inode with a path and a name.
	Methods for state: GetState(), SetState(), WaitForState(), etc.
	Ephemeral state access: GetEphemeral(), SetEphemeral(), InitializeEphemerals().
	Class methods for creation: From(), GetId(), UpdateDatabaseWithPath(), GetPathInfoFromAuthority().
	Methods for persistence: PopulateFSMethods(), ValidateArgs(), AddUpath(), IsFresh(), GetDataToSave(), LoadFromData(), Freeze(), Save(), Load(), and a factory method Create().
	Stubs for synchronization: BeforePushUpstream(), PushUpstream(), AfterPushUpstream(), BeforePullDownstream(), PullDownstream(), and AfterPullDownstream().

TODOs/FIXMEs:

	TODO: Improve error handling around multi-threaded database and authority lookups (see comments about halting threads).
	TODO: Further refine ephemeral state management for multi-server environments.
"""

import eons
import threading
import multiprocessing
import json
import logging
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound
# from truckeefs.lib.db.InodeModel import InodeModel #implicit per build system.

class Inode (eons.Functor):
	def __init__(this, upath=None, name="UNKNOWN FILESYSTEM OBJECT"):
		super().__init__(name)

		this.id = None
		this.meta = None # Metadata for the object
		this.upaths = []
		this.parents = [] # numeric ids of parents
		if (upath and type(upath) is str):
			this.upaths.append(upath)

		this.arg.kw.required.append('db')

		this.stateRetries = 15 # Number of times to retry a state check. Uses ExponentialSleep between tries.

		# Temporary Sync operational members.
		# Do not modify these directly.
		this.frozen = None # Data to be synced to Tahoe as JSON. Do not modify directly.
		this.sync_again = False # Whether to sync this object again.
		this.lastRead = None # Last time the object was read.


	# Get the state of a process on an inode.
	# Use this method for semaphore operations.
	# Processes include 'read', 'write', and 'sync'.
	def GetState(this, process):
		return this.executor.delta.GetState(this.id, process)

	# Get the state of a process on an inode.
	# Use this method for semaphore operations.
	# Processes include 'read', 'write', and 'sync'.
	# For extra safety, you can pass in what you think the current state is. If it's not what you expect, the state will not be set.
	# RETURNS True if the state has been set. False otherwise.
	def SetState(this, process, state, expectedState=None):
		return this.executor.delta.SetState(this.id, process, state)

	# Wait for a process to reach a certain state.
	def WaitForState(this, process, state):
		for i in range(this.stateRetries):
			if (this.GetState(process) == state):
				return True

			ExponentialSleep(i)

		return False
	
	# Wait for a process to reach a state besides the one provided.
	# RETURNS the new state if it changes. False otherwise.
	def WaitForStateBesides(this, process, state):
		for i in range(this.stateRetries):
			newState = this.GetState(process)
			if (newState != state):
				return newState

			ExponentialSleep(i)

		return False

	# Wait for a process to change state.
	# RETURNS the new state if it changes. False otherwise.
	def WaitForStateChange(this, process):
		return this.WaitForStateBesides(process, this.GetState(process))

	# Check if the process states for this inode have been initialized.
	# RETURNS True if the states have been initialized. False otherwise.
	def AreProcessStatesInitialized(this):
		readState = this.GetState('read')
		writeState = this.GetState('write')
		syncState = this.GetState('sync')

		# If any one state is None, there might just be a network error (though we hope not!)
		# If all states are None, the object should be initialized.
		if (readState is None and writeState is None and syncState is None):
			return False
		
		return True

	# Initialize the process states for this inode.
	def InitializeProcessStates(this):
		this.SetState('read', ProcessState.IDLE)
		this.SetState('write', ProcessState.IDLE)
		this.SetState('sync', ProcessState.IDLE)


	# Get a value for a key on an Inode in Redis.
	# If you need to coerce the value to a specific type, pass in the type as coerceType.
	# RETURNS the value, as a string, for the given key on the given Inode or None if there was an error.
	def GetEphemeral(this, key, coerceType=None):
		return this.executor.delta.GetRedisInodeValue(this.id, key, coerceType)

	# Set a value for a key on an Inode in Redis.
	# RETURNS True if the value was set, False otherwise.
	# Use the expectedValue parameter for extra safety. If the value is not what you expect, the value will not be set.
	def SetEphemeral(this, key, value, expectedValue=None):
		return this.executor.delta.SetRedisInodeValue(this.id, key, value, expectedValue)

	# Set any temporary values to their default values.
	# NOTE: These are less structerd than the process states, so there's no Are...Initialized methods.
	# Because of that, this method should be called wherever InitializeProcessStates is called.
	def InitializeEphemerals(this):
		this.SetEphemeral('sync_pid', "")
		this.SetEphemeral('sync_host', "")
		this.SetEphemeral('sync_again', False)
		this.SetEphemeral('last_written', "")


	# Factory method to create a new Inode from a given upath
	# This method will simultaneously check:
	# 1. If the object is already in the Executor's cache
	# 2. If the object is in the database
	# 3. If the object is in the TahoeLAFS authority
	# If the object is not in the cache, it will be created and added to the cache.
	# If the object is not in the database, it will be created and added to the database.
	# The same applies for any missing parents in the given upath which are not already in the database.
	# Missing parents will not be cached.
	@classmethod
	def From(cls, executor, upath):

		# Define thread variables
		existingByUpathResult = [None]
		existingByIdResult = [None]
		idResult = [None]
		authorityResult = [None]

		# Define thread events
		authorityHaltEvent = threading.Event()
		idHaltEvent = threading.Event()

		# Define thread functions
		def GetCachedObjectByUpathThread():
			existingByUpathResult[0] = executor.GetCachedObjectByUpath(upath)

		def GetCachedObjectByIdThread():
			existingByIdResult[0] = executor.GetCachedObjectById(idResult[0])

		def GetIdThread():
			idResult[0] = cls.GetId(executor, upath, idHaltEvent)

		def AuthorityCheckThread():
			authorityResult[0] = cls.GetPathInfoFromAuthority(executor, upath, authorityHaltEvent)

		# Create and start threads
		cachedUpathThread = threading.Thread(target=GetCachedObjectByUpathThread, name=f"GetCachedObjectByUpathThread for {upath}")
		dbThread = threading.Thread(target=getIdThread, name=f"GetIdThread for {upath}")
		authorityThread = threading.Thread(target=authorityCheckThread, name=f"AuthorityCheckThread for {upath}")

		cachedUpathThread.start()
		dbThread.start()
		authorityThread.start()

		# Wait for the DB lookup to finish
		cachedUpathThread.join()

		# Check if the object is already in the cache
		if (existingByUpathResult[0]):
			# Signal the db thread to terminate
			if (dbThread.is_alive()):
				logging.info("Cache lookup succeeded; terminating database lookup.")
				idHaltEvent.set()

			return existingByUpathResult[0]

		# Wait for the DB lookup to finish
		dbThread.join()

		if (idResult[0]):
			cachedIdThread = threading.Thread(target=GetCachedObjectByIdThread, name=f"GetCachedObjectByIdThread for {idResult[0]}")
			cachedIdThread.start()

			# If DB lookup succeeded, terminate authority thread if still running
			if (authorityThread.is_alive()):
				logging.info("Database lookup succeeded; terminating authority lookup.")
				authorityHaltEvent.set() # NOTE: at time of writing, this is a nop.

			# Check if the object is already in the cache
			cachedIdThread.join()
			if (existingByIdResult[0]):
				existingByIdResult[0].AddUpath(upath)
				return existingByIdResult[0]

		else:
			# Wait for authority thread to finish if DB lookup failed
			authorityThread.join()

			if (authorityResult[0]):
				# If authority lookup succeeded but DB lookup failed
				idResult[0] = cls.UpdateDatabaseWithPath(executor, upath, authorityResult[0])
			else:
				raise ValueError(f"Path {upath} is invalid and could not be found in TahoeLAFS")

		# If we reach this point, the object is not in the cache and we need to create a new one
		ret = cls(upath)
		ret.id = idResult[0]
		executor.CacheObject(ret)
		return ret


	@classmethod
	def GetId(cls, executor, upath, haltEvent=None):
		def RecursiveResolve(pathSegments, parent):
			if (haltEvent and haltEvent.is_set()):
				return None

			# Base case: no more segments left to resolve
			if not pathSegments:
				return parent

			segment = pathSegments.pop(0)
			resolvedIds = []
			session: Session = executor.GetDatabaseSession()

			try:
				obj = session.query(InodeModel).filter_by(name=segment).filter(InodeModel.parents.contains([parent])).one()
				resolvedIds.extend(RecursiveResolve(pathSegments, [obj.id]))
			except NoResultFound:
				pass

			if not resolvedIds:
				raise ValueError(f"Inode not found for segment '{segment}' under parent {parent}")

			return resolvedIds

		pathSegments = upath.strip('/').split('/')
		initialParent = executor.GetUpathRootId()

		# Resolve ID directly, without threading
		try:
			return RecursiveResolve(pathSegments, initialParent)[-1]
		except ValueError as e:
			logging.error(f"Error resolving path {upath}: {e}")
			return None


	@classmethod
	def UpdateDatabaseWithPath(cls, executor, upath, pathInfo):
		session: Session = executor.GetDatabaseSession()

		# Split the upath into its segments
		pathSegments = upath.strip('/').split('/')

		# Base case: If there are no more segments to process, return
		if (not pathSegments):
			return None

		# Process the parent path first (all segments except the last one)
		parentPath = '/'.join(pathSegments[:-1])
		parentId = None

		if (parentPath):
			# Check if the parent already exists in the database
			parentId = cls.GetId(executor, parentPath)
			if (not parentId):
				# Recursively create the parent path if it doesn't exist
				parentInfo = cls.GetPathInfoFromAuthority(executor, parentPath)
				if (parentInfo):
					parentId = cls.UpdateDatabaseWithPath(executor, parentPath, parentInfo)
				else:
					raise ValueError(f"Parent path {parentPath} is invalid and could not be found in TahoeLAFS")

		# Create the current InodeModel entry using the resolved parent ID
		newObj = InodeModel(
			name=pathSegments[-1], 
			parents=[parentId] if parentId else None
		)
		session.add(newObj)
		session.commit()

		return newObj.id

	@classmethod
	def GetPathInfoFromAuthority(cls, executor, upath, haltEvent=None):
		# TODO: implement the haltEvent somehow? It should just be a kill signal but Python doesn't support that.
		return executor.GetSourceConnection().GetPathInfo(upath)


	# Populate the Inode with the methods that are available to it.
	# This allows us to utilize the FSOp class to perform operations on the object.
	def PopulateFSMethods(this, name, fsMethods):
		for method in fsMethods:
			this.methods[method] = eons.SelfRegistering(f"{name.lower()}_{method.lower()}")(name=method)


	# Validate the arguments provided to the object.
	# eons.Functor method. See that class for more information.
	def ValidateArgs(this):
		super().ValidateArgs()

		if (not this.id):
			if (not len(this.upaths)):
				raise eons.MissingArgumentError(f"No upath provided for Inode {this.name}")


	# Add a new upath to *this.
	def AddUpath(this, upath):
		this.upaths.append(upath)


	# Check if the object is still fresh in the cache
	def IsFresh(this):
		return this.pendingSync or (this.last_accessed > datetime.now().timestamp() - this.executor.cache_ttl)
	

	# RETURNS a dictionary of the data that should be saved to the database.
	# You should override this method in your child class to save additional data.
	def GetDataToSave(this):
		return {
			name: this.name,
			kind: this.__class__.__name__,
			parents: this.parents,
			meta: this.meta,
		}

	# Load the data from the database into *this.
	# You should override this method in your child class to load additional data.
	def LoadFromData(this, data):
		this.name = data['name']
		this.parents = json.loads(data['parents'])
		this.meta = data['meta']

	
	# Save the state of *this.
	# RETURNS a dictionary of the data that should be saved to the database and/or Tahoe.
	# You should override this method in your child class to save additional data.
	# Will only be called when *this has been mutated, before being Saved.
	def Freeze(this):
		this.frozen = this.GetDataToSave()
		return this.frozen


	# Commit the data in *this to the database.
	# You should NOT need to override this method.
	# To change the data that are saved, override GetDataToSave instead.
	#
	# The mutated parameter is used to determine if the object needs to be updated in Tahoe.
	# If *this is mutated, it will be marked as pending_sync in the database, and a new process will be spawned to sync the object to Tahoe.
	def Save(this, mutated=True):
		session: Session = this.executor.GetDatabaseSession()

		try:
			# Check if the object already exists in the database
			obj = session.query(InodeModel).filter_by(id=this.id).one()

			# Update the existing object's fields
			for key, value in this.GetDataToSave().items():
				setattr(obj, key, value)

			if (mutated):
				# Check if there's an active sync process
				syncPid = this.GetEphemeral('sync_pid')
				syncHost = this.GetEphemeral('sync_host')
				if (syncPid and syncHost):
					# Verify that the process is still running
					try:
						if (socket.gethostname() == syncHost):
							os.kill(int(syncPid), 0)
							logging.info(f"Sync process already running for Inode {this.name} (PID: {syncPid}).")

							this.SetEphemeral('sync_again', True)

					except OSError:
						# Process not running, clear the PID
						syncPid = None
						syncHost = None

					# TODO: Handle cases where the data needs to be synced but the sync_host is not the current host
					# For example, what happens in a multi-server environment where a sync_host goes down? How would we even know?

				if (not syncPid):
					# Collect sync args
					kwargs = this.executor.kwargs.copy()
					frozenData = this.Freeze()
					kwargs.update({
						'inode_id': this.id,
						'frozen_data': frozenData,
					})

					# Spawn a new sync process
					# This new process will wait for us to put the sync_pid in the database before beginning it's work.
					sync_process = multiprocessing.Process(target=TahoeSyncWorker.UpstreamSyncWorker, args=(kwargs,))
					sync_process.start()

					# Save the PID and hostname in the Redis database
					obj.SetEphemeral('sync_pid', sync_process.pid, expectedValue="")
					obj.SetEphemeral('sync_host', socket.gethostname(), expectedValue="")

			session.commit()
			logging.info(f"Inode {this.name} (ID: {this.id}) updated in the database.")

		except NoResultFound:
			logging.error(f"Inode with ID {this.id} not found in the database.")
			raise ValueError(f"Inode with ID {this.id} does not exist in the database.")


	# Load the data from the database into *this.
	# You should NOT need to override this method.
	# To change the data that are loaded, override LoadFromData instead.
	def Load(this):
		session: Session = this.executor.GetDatabaseSession()

		try:
			# Load the object from the database using its ID
			obj = session.query(InodeModel).filter_by(id=this.id).one()

			# Populate the object's attributes with the loaded data
			this.LoadFromData(obj)

			logging.info(f"Inode {this.name} (ID: {this.id}) loaded from the database.")

		except NoResultFound:
			logging.error(f"Inode with ID {this.id} not found in the database.")
			raise ValueError(f"Inode with ID {this.id} does not exist in the database.")


	# Create a new Inode from the database using the given ID.
	# Will use the class name stored in the database to instantiate the appropriate type.
	@staticmethod
	def Create(executor, id):
		session: Session = this.executor.GetDatabaseSession()

		try:
			# Load the object from the database using its ID
			obj = session.query(InodeModel).filter_by(id=id).one()

			# Create the object
			ret = eons.SelfRegistering(obj.kind)()
			ret.executor = executor
			ret.id = obj.id
			ret.LoadFromData(obj)

			return ret

		except NoResultFound:
			logging.error(f"Inode with ID {id} not found in the database.")
			raise ValueError(f"Inode with ID {id} does not exist in the database.")


	# Anything you'd like to do before *this is synced to Tahoe.
	# Will be called by TahoeSyncWorker.PushUpstreamToSource.
	# Should ONLY operate on this.frozen data
	def BeforePushUpstream(this):
		pass

	# Update some data in the source (i.e. Tahoe).
	# Please override for your child class.
	# Will be called by TahoeSyncWorker.PushUpstreamToSource.
	# Should ONLY operate on this.frozen data
	def PushUpstream(this):
		pass

	# Anything you'd like to do after *this is synced to Tahoe.
	# Will be called by TahoeSyncWorker.PushUpstreamToSource.
	# Should ONLY operate on this.frozen data
	def AfterPushUpstream(this):
		pass

	
	# Anything you'd like to do before *this is synced from Tahoe.
	# Will be called by TahoeSyncWorker.PullDownstreamFromSource.
	def BeforePullDownstream(this):
		pass
	
	# Update our local cache with data from Tahoe.
	# Please override for your child class.	
	# Will be called by TahoeSyncWorker.PullDownstreamFromSource.
	def PullDownstream(this):
		pass
	
	# Anything you'd like to do after *this is synced from Tahoe.
	# Will be called by TahoeSyncWorker.PullDownstreamFromSource.
	def AfterPullDownstream(this):
		pass
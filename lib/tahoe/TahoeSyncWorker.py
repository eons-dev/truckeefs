"""
lib/tahoe/TahoeSyncWorker.py

Purpose:
Implements synchronization workers for pushing changes upstream (to Tahoe) and pulling changes downstream (from Tahoe). Runs as separate processes to offload synchronization.

Place in Architecture:
Bridges the local inode state (from the caching layer) with the remote Tahoe state. It is invoked for both file and directory sync operations.

Interface:

	Static methods:
		SyncWorkerCommon(kwargs): Common initialization for sync workers.
		DownstreamSyncWorker(kwargs): Worker for downloading updates from Tahoe.
		UpstreamSyncWorker(kwargs): Worker for uploading changes to Tahoe.
		GetInode(executor, inodeId): Retrieves the inode object for a given ID.
		PushUpstreamToSource(executor, inode): Invokes the inode’s upstream methods.
		PullDownstreamFromSource(executor, inode): Invokes the inode’s downstream methods.
		CompleteSync(executor, inode, successful=True): Finalizes a sync operation.

TODOs/FIXMEs:

	NOTE: Comments indicate that these workers run on demand and do not continuously poll for changes; may need further work for multi-region synchronization.
"""

import eons
from ..RiverFS import RiverFS

class TahoeSyncWorker(eons.Functor):

	# Common tasks for SyncWorkers.
	# If any of these fail, the system (e.g. database) will be in a bad state.
	@staticmethod
	def SyncWorkerCommon(kwargs):

		# Get our initial data.
		try:
			inodeId = kwargs.pop('inode_id')
		except Exception as e:
			logging.error(f"Error syncing: {e}")
			# Can't clear database. This is also bad.
			raise e

		# Initialize the Executor.
		try:
			executor = RiverFS(f"RiverFS Sync for {inodeId}")
			executor(**kwargs)
		except Exception as e:
			logging.error(f"Error syncing {inodeId}: {e}")
			# Can't clear database. This is bad.
			raise e

		return executor, inodeId


	# Sync an Inode downstream from the source (i.e. Tahoe).
	# This is a high(ish) priority background task.
	# It should be spawned from multiprocessing.Process.
	# Most logic is implemented in the Inode subclass.
	# To modify the sync behavior, override the following methods in your Inode subclass:
	# - BeforePullDownstream
	# - PullDownstream
	# - AfterPullDownstream
	#
	# NOTE: THIS RUNS ON DEMAND! It DOES NOT regularly check for changes upstream.
	# This means you cannot use Tahoe to synchronize data across regions / geo-distributed servers.
	#
	@staticmethod
	def DownstreamSyncWorker(kwargs):
		executor, inodeId = TahoeSyncWorker.SyncWorkerCommon(kwargs)

		try:
			inode = TahoeSyncWorker.GetInode(executor, inodeId)
			TahoeSyncWorker.PullDownstreamFromSource(executor, inode)
		except Exception as e:
			logging.error(f"Error syncing {inodeId}: {e}")
			TahoeSyncWorker.CompleteSync(executor, inodeId, False)
			raise e
		
		TahoeSyncWorker.CompleteSync(executor, inodeId)


	# Sync an Inode upstream toward the source (i.e. Tahoe).
	# This is a background task that runs with the lowest possible priority.
	# It should be spawned from multiprocessing.Process.
	# See Inode.Save for an example.
	#
	# To modify the sync behavior, override the following methods in your Inode subclass:
	# - Freeze
	# - BeforePushUpstream
	# - PushUpstream
	# - AfterPushUpstream
	@staticmethod
	def UpstreamSyncWorker(kwargs):
		# Set the process priority to low (nice level 19)
		# In Unix-like systems, the nice level ranges from -20 (highest priority) to 19 (lowest priority). 
		# Setting the nice level to 19 ensures that the sync process runs with the lowest possible priority, which is ideal for background tasks that shouldn't interfere with the performance of higher-priority tasks, like user interactions or other critical processes.
		os.nice(19)

		executor, inodeId = TahoeSyncWorker.SyncWorkerCommon(kwargs)
		
		try:
			frozen_data = json.loads(kwargs.pop('frozen_data'))
		except Exception as e:
			try: 
				frozen_data = TahoeSyncWorker.GetInode(executor, inodeId).Freeze()
			except Exception as e:
				logging.error(f"Error syncing {inodeId}: {e}")
				TahoeSyncWorker.CompleteSync(executor, inodeId, False)
				raise e

		# Startup loop.
		# Make sure any async tasks have finished writing before we begin syncing.
		# 5 minute timeout.
		inode = TahoeSyncWorker.GetInode(executor, inodeId)
		for i in range(300):
			syncPid = inode.GetEphemeral('sync_pid')
			syncHost = inode.GetEphemeral('sync_host')
			if ((syncPid is None or not len(syncPid) or (syncHost is None or not len(syncHost)))):
				if (i == 299):
					logging.error(f"Sync process for {inodeId} ({obj.name}) timed out.")
					TahoeSyncWorker.CompleteSync(executor, inodeId, False)
					raise Exception(f"Sync process for {inodeId} ({obj.name}) timed out.")
				else:
					sleep(1)
					continue
			elif (syncPid != os.getpid() or syncHost != socket.gethostname()):
				raise Exception(f"Sync process already running for {inodeId} ({syncPid} is running on {syncHost}, but I am {os.getpid()} on {socket.gethostname()}).")
			else:
				break

		# Sync loop.
		while (True):
			try:
				# Get the Inode we'll be syncing.
				inode = TahoeSyncWorker.GetInode(executor, inodeId)

				# Check if the data changed while we were syncing the object.
				# Doing this here (rather than spawning a new process) helps conserve resources and should make syncing faster overall.
				if (frozen_data is None):
					syncAgain = inode.GetEphemeral('sync_again', coerceType=bool)
					if (not syncAgain):
						logging.info(f"Inode {inode.name} (ID: {inodeId}) has no frozen data to sync. Exiting.")
						break

					frozen_data = inode.Freeze()

				# Refresh the Ephemeral values.
				inode.SetEphemeral('sync_again', False)
				if (inode.SetEphemeral('sync_pid', os.getpid(), os.getpid()) is None
					or inode.SetEphemeral('sync_host', socket.gethostname(), socket.gethostname()) is None
				):
					logging.error(f"Sync process for {inodeId} ({inode.name}) was interrupted.")
					TahoeSyncWorker.CompleteSync(executor, inodeId, False)
					raise Exception(f"Sync process for {inodeId} ({inode.name}) was interrupted.")

				# Perform the Sync.
				if (inode.frozen is None or not len(inode.frozen)):
					inode.frozen = frozen_data
				TahoeSyncWorker.PushUpstreamToSource(executor, inode)
				frozen_data = None

			except Exception as e:
				logging.error(f"Error syncing {inodeId}: {e}")
				TahoeSyncWorker.CompleteSync(executor, inodeId, False)
				raise e

		# Cleanup			
		TahoeSyncWorker.CompleteSync(executor, inodeId)


	# Get the Inode we'll be syncing.
	@staticmethod
	def GetInode(executor, inodeId):
		try:
			inode = Inode.Create(executor, inodeId)
			logging.info(f"Inode {inode.name} (ID: {inodeId}) ready to sync.")
		except Exception as e:
			logging.error(f"Error syncing {inodeId}: {e}")
			raise e
		return inode


	# Perform the upstream Sync.
	# Update some data in the source (i.e. Tahoe).
	@staticmethod
	def PushUpstreamToSource(executor, inode):
		try:
			inode.BeforePushUpstream()
			inode.PushUpstream()
			inode.AfterPushUpstream()
			logging.info(f"Inode {inode.name} (ID: {inode.id}) pushed upstream.")
		except Exception as e:
			logging.error(f"Error syncing {inodeId}: {e}")
			raise e

	# Perform the downstream Sync.
	# Update some data in the local cache.
	@staticmethod
	def PullDownstreamFromSource(executor, inode):
		try:
			inode.BeforePullDownstream()
			inode.PullDownstream()
			inode.AfterPullDownstream()
			logging.info(f"Inode {inode.name} (ID: {inode.id}) pulled downstream.")
		except Exception as e:
			logging.error(f"Error syncing {inodeId}: {e}")
			raise e


	# Release databse locks
	@staticmethod
	def CompleteSync(executor, inode, successful=True):
		inode.SetEphemeral('sync_pid', "", os.getpid()
		inode.SetEphemeral('sync_host', "", socket.gethostname())
		success = "successful" if successful else "unsuccessful"
		logging.info(f"Inode {obj.name} (ID: {inodeId}) sync {success}.")

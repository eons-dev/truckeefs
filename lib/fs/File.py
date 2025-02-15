"""
lib/fs/File.py

Purpose:
Implements a File inode (subclass of Inode) specialized for files.

Place in Architecture:
Represents files in the local cache. It holds a reference to the local data (path to the cached file) and coordinates file-level syncing.

Interface:

	__init__(upath, name="File"): Initializes a File inode.
	GetDataToSave(): Returns file-specific metadata (e.g. the data file path).
	LoadFromData(data): Loads file metadata.
	Stub methods for: Freeze(), BeforePushUpstream(), PushUpstream(), AfterPushUpstream(), BeforePullDownstream(), PullDownstream(), and AfterPullDownstream().

TODOs/FIXMEs:

	TODO: Implement the file synchronization methods. The comment in Freeze() indicates the need to wait for writes to finish before freezing.
"""

import eons
from .common.Inode import *

class File (Inode):
	def __init__(this, upath="", name="File"):
		super().__init__(name, upath)

		this.data = None # The filesystem path to the data of the file.
	
		this.PopulateFSMethods('File', [
			'GetAttributes',
			'Unlink',
			'Read',
			'Write',
			'Truncate',
			'Append',
			'Copy',
			'Move'
		])

	def GetDataToSave(this):
		ret = super().GetDataToSave()
		ret.update({
			'data': this.data
		})
		return ret

	def LoadFromData(this, data):
		super().LoadFromData(data)
		this.data = data['data']

	def Freeze(self):
		"""Capture a snapshot of the file's state for syncing."""
		try:
			# Flush any pending writes to disk.
			self.Flush()  # (Assuming Flush() is implemented to call FileOnDisk.flush())
			freeze_info = {
				'local_path': self.data,
				'size': self.GetSize(),  # Your method to get file size
				'timestamp': time.time(),
				'metadata': self.info
			}
			logging.info(f"Freeze: File {self.upath} frozen with info {freeze_info}")
			return freeze_info
		except Exception as e:
			logging.error(f"Freeze error for file {self.upath}: {str(e)}")
			raise IOError(errno.EFAULT, f"Error freezing file {self.upath}")

	def BeforePushUpstream(self):
		"""Prepare file for upload by flushing and checking consistency."""
		try:
			self.Flush()
			logging.info(f"BeforePushUpstream: File {self.upath} flushed successfully.")
		except Exception as e:
			logging.error(f"BeforePushUpstream error for file {self.upath}: {str(e)}")
			raise IOError(errno.EFAULT, f"BeforePushUpstream error for file {self.upath}")

	def PushUpstream(self):
		"""Upload the file's data to Tahoe and return the remote capability."""
		source_conn = self.executor.GetSourceConnection()
		local_path = self.data  # Local cached file
		try:
			parent_cap = self.executor.LookupCap(udirname(self.upath), source_conn)
			upload_path = parent_cap + "/" + ubasename(self.upath)
			with open(local_path, 'rb') as f:
				filecap = source_conn.put_file(upload_path, f, iscap=True)
			logging.info(f"PushUpstream: File {self.upath} uploaded to {upload_path} with filecap {filecap}")
			return filecap
		except Exception as e:
			logging.error(f"PushUpstream error for file {self.upath} (local path {local_path}): {str(e)}")
			raise IOError(errno.EFAULT, f"Error uploading file {self.upath}")

	def AfterPushUpstream(self):
		"""Update local metadata after a successful upload."""
		try:
			filecap = self.PushUpstream()  # In some flows, PushUpstream may have already been run.
			self.info[1]['ro_uri'] = filecap
			self.info[1]['size'] = self.GetSize()
			self.dirty = False
			logging.info(f"AfterPushUpstream: File {self.upath} metadata updated; file is now clean.")
		except Exception as e:
			logging.error(f"AfterPushUpstream error for file {self.upath}: {str(e)}")
			raise IOError(errno.EFAULT, f"AfterPushUpstream error for file {self.upath}")

	def BeforePullDownstream(self):
		"""Determine if the local cache is stale and a download is needed."""
		ttl = self.executor.cache_ttl  # Cache time-to-live in seconds.
		retrieved = self.info[1].get('retrieved', 0)
		age = time.time() - retrieved
		if age < ttl:
			logging.info(f"BeforePullDownstream: File {self.upath} cache is fresh (age {age:.2f}s); no download needed.")
			return False
		logging.info(f"BeforePullDownstream: File {self.upath} cache is stale (age {age:.2f}s); proceeding to download.")
		return True

	def PullDownstream(self):
		"""Download the file from Tahoe and update the local cache."""
		source_conn = self.executor.GetSourceConnection()
		ro_uri = self.info[1].get('ro_uri')
		if not ro_uri:
			logging.error(f"PullDownstream: No remote URI for file {self.upath}")
			raise IOError(errno.ENOENT, "No remote URI available for file")
		try:
			remote_stream = source_conn.get_content(ro_uri)
		except Exception as e:
			logging.error(f"PullDownstream: Failed to open remote stream for file {self.upath}: {str(e)}")
			raise IOError(errno.EREMOTEIO, f"Error downloading file {self.upath}")
		local_path = self.data
		try:
			with open(local_path, 'wb') as local_f:
				while True:
					chunk = remote_stream.read(131072)
					if not chunk:
						break
					local_f.write(chunk)
			remote_stream.close()
			logging.info(f"PullDownstream: File {self.upath} downloaded and local cache updated.")
		except Exception as e:
			logging.error(f"PullDownstream error for file {self.upath}: {str(e)}")
			raise IOError(errno.EREMOTEIO, f"Error writing file {self.upath} from remote stream")

	def AfterPullDownstream(self):
		"""Update metadata after download to mark the file as fresh."""
		try:
			self.info[1]['retrieved'] = time.time()
			logging.info(f"AfterPullDownstream: File {self.upath} metadata updated with new retrieval time.")
		except Exception as e:
			logging.error(f"AfterPullDownstream error for file {self.upath}: {str(e)}")
			raise IOError(errno.EFAULT, f"AfterPullDownstream error for file {self.upath}")

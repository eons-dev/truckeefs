"""
lib/fs/Directory.py

Purpose:
Implements a Directory inode (subclass of Inode) specialized for directories.

Place in Architecture:
Represents directories in the local cache. It stores a list of children and provides methods for syncing directory metadata.

Interface:

	__init__(upath, name="Directory"): Initializes a Directory inode.
	GetDataToSave(): Returns a dictionary of data (including children) for persistence.
	LoadFromData(data): Loads metadata from persistent storage.
	Freeze(): (No special action; may remain as pass.)
	Stub methods for: BeforePushUpstream(), PushUpstream(), AfterPushUpstream(), BeforePullDownstream(), PullDownstream(), and AfterPullDownstream().

TODOs/FIXMEs:

	TODO: Implement the sync methods if directory-specific sync is needed.
"""


import eons
from .Inode import *

class Directory (Inode):
	def __init__(this, upath="", name="Directory"):
		super().__init__(name, upath)

		this.children = [] # List of numeric ids of all the children of *this.
	
		this.PopulateFSMethods('Directory', [
			'GetInode',
			'GetAttributes',
			'Unlink',
			'Open',
			'Close',
			'Make'
		])

	def GetDataToSave(this):
		ret = super().GetDataToSave()
		ret.update({
			'children': this.children
		})
		return ret
	
	def LoadFromData(this, data):
		super().LoadFromData(data)
		this.children = data['children']

	# No special action is needed to freeze a directory.
	def Freeze(this):
		return super().Freeze()


	def BeforePushUpstream(this):
		pass

	def PushUpstream(this):
		source_conn = this.executor.GetSourceConnection()
		# For directories, assume we “upload” a JSON representation of the directory info.
		import json
		dir_metadata = json.dumps(this.info)
		# Determine the upload path for the directory.
		parent_cap = this.executor.LookupCap(udirname(this.upath), source_conn)
		upload_path = parent_cap + "/" + ubasename(this.upath)
		try:
			# Use put_file to upload the metadata.
			new_cap = source_conn.put_file(upload_path, data=dir_metadata.encode('utf-8'), iscap=True)
		except Exception as e:
			raise IOError(errno.EREMOTEIO, f"Error uploading directory metadata: {str(e)}")
		# Save the new capability.
		this.info[1]['ro_uri'] = new_cap

	def AfterPushUpstream(this):
		# Mark directory as synced.
		this.dirty = False

	
	def BeforePullDownstream(this):
		pass
	
	def PullDownstream(this):
		source_conn = this.executor.GetSourceConnection()
		ro_uri = this.info[1].get('ro_uri')
		if not ro_uri:
			raise IOError(errno.ENOENT, "No remote URI available for directory")
		
		try:
			# Get the JSON metadata for the directory.
			remote_stream = source_conn.get_content(ro_uri)
			import json
			new_info = json.load(remote_stream)
		except Exception as e:
			raise IOError(errno.EREMOTEIO, f"Error downloading directory metadata: {str(e)}")
		finally:
			remote_stream.close()
		
		# Update local metadata.
		this.info = new_info
	
	def AfterPullDownstream(this):
		# Update retrieval time.
		this.info[1]['retrieved'] = time.time()
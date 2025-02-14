import eons
from .Inode import *

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

	def Freeze(this):
		# Copy the file data to a temporary file.
		# Wait for any writes to finish first.
	

	def BeforePushUpstream(this):
		# Ensure all pending writes are flushed.
		# (If using block cache, flush it to disk.)
		this.Flush()  # Assume this flushes the FileOnDisk object.

	def PushUpstream(this):
		# Use the TahoeConnection (obtained from executor) to upload the file.
		# We'll assume that 'this.executor.GetSourceConnection()' returns a TahoeConnection.
		source_conn = this.executor.GetSourceConnection()
		
		# Get the local file path that holds our file data.
		local_path = this.data  # Assuming `data` holds the path to our FileOnDisk cache file.
		
		# Open the file for reading.
		with open(local_path, 'rb') as f:
			# The Tahoe API expects to receive a file-like object.
			try:
				# Construct an upload path based on the file's upath.
				# For example, use the parent directory's capability.
				parent_cap = this.executor.LookupCap(udirname(this.upath), source_conn)
				upload_path = parent_cap + "/" + ubasename(this.upath)
				
				# Upload file.
				filecap = source_conn.put_file(upload_path, f, iscap=True)
			
			except Exception as e:
				raise IOError(errno.EFAULT, f"Error uploading file: {str(e)}")
		
		# Save the returned capability for future use.
		this.info[1]['ro_uri'] = filecap
		this.info[1]['size'] = this.GetSize()  # Assuming GetSize returns the file size.
		

	def AfterPushUpstream(this):
		# Mark the file as clean.
    	this.dirty = False


	def BeforePullDownstream(this):
		pass
	
	def PullDownstream(this):
		source_conn = this.executor.GetSourceConnection()
		ro_uri = this.info[1].get('ro_uri')
		if not ro_uri:
			raise IOError(errno.ENOENT, "No remote URI available for download")
		
		# Open a stream from Tahoe:
		try:
			# Assume get_content returns a file-like object for the remote file.
			remote_stream = source_conn.get_content(ro_uri)
		except Exception as e:
			raise IOError(errno.EREMOTEIO, f"Error downloading file: {str(e)}")
		
		# Overwrite the local file with the downloaded data.
		# (You might open the local cache file in write mode and copy data.)
		with open(this.data, 'wb') as local_f:
			while True:
				chunk = remote_stream.read(131072)  # Read in block-size chunks.
				if not chunk:
					break
				local_f.write(chunk)
		remote_stream.close()
	
	def AfterPullDownstream(this):
		# Update metadata (e.g., refresh timestamps, mark as fresh)
    	this.info[1]['retrieved'] = time.time()
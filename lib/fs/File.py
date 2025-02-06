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
		pass

	def PushUpstream(this):
		pass

	def AfterPushUpstream(this):
		pass


	def BeforePullDownstream(this):
		pass
	
	def PullDownstream(this):
		pass
	
	def AfterPullDownstream(this):
		pass
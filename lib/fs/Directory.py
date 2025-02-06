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
		pass

	def AfterPushUpstream(this):
		pass

	
	def BeforePullDownstream(this):
		pass
	
	def PullDownstream(this):
		pass
	
	def AfterPullDownstream(this):
		pass
import sys
from pathlib import Path

class Includes(object):
	def __init__(this):
		this.incPath = Path(__file__).parent.resolve().joinpath('inc')
		this.includes = [
			# Includes go here.
		]
		for i in this.includes:
			setattr(this, f"{i}_path", str(this.incPath.joinpath(i)))

	def GetIncludePath(this, include):
		return getattr(this, f"{include}_path")

	def Include(this, include):
		sys.path.append(this.GetIncludePath(include))

inc = Includes()

def Include(include):
	inc.Include(include)

def GetIncludePath(include):
	return inc.GetIncludePath(include)

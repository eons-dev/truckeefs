import eons
import os
import sys
import fuse
import logging

from libtruckeefs import TruckeeFS

fuse.fuse_python_api = (0, 2)

class TRUCKEEFS(eons.Executor):
	def __init__(this, name="TruckeeFS"):
		super().__init__(name)

		this.arg.kw.required.append("rootcap")
		this.arg.kw.required.append("mount")
		this.arg.kw.optional["node_url"] = "http://127.0.0.1:3456"

		this.arg.mapping.append("rootcap")
		this.arg.mapping.append("mount")
		this.arg.mapping.append("node_url")


	def Function(this):
		fs = TruckeeFS()
		fs(
			rootcap = this.rootcap,
			mount = this.mount,
			node_url = this.node_url
		)

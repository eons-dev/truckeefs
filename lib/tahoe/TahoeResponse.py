"""
lib/tahoe/TahoeResponse.py

Purpose:
Wraps the HTTP response from a Tahoe API call.
Place in Architecture:
Acts as a thin wrapper around the response stream from urllib, allowing for standardized reading and closing of responses for Tahoe operations.

Interface:

	__init__(connection, req, is_put, timeout): Initializes the response.
	read(size=None): Reads data from the response.
	close(): Closes the response and notifies the connection.

TODOs/FIXMEs:

	TODO: Consider improving timeout handling for PUT requests (see comment about using default timeout for PUT).
"""

from urllib.request import Request, urlopen


class TahoeResponse(object):
	def __init__(this, connection, req, is_put, timeout):
		this.connection = connection

		# XXX: We use default timeout for PUT requests, for now:
		#	  Solution would be to limit send buffer size, but urllib2
		#	  doesn't easily allow this Switching to requests module probably
		#	  would help.
		#
		# We recv data in relatively small blocks, so that blocking
		# for recv corresponds roughly to network activity. POST
		# requests are also small, so that the situation is the same.
		#
		# However, PUT requests may upload large amounts of data. The
		# send buffer can also be fairly large, so that all the data
		# may fit into it. In this case, we end up blocking on reading
		# the server response, which arrives only after the data in
		# the buffer is sent. In this case, timeout can arrive even if
		# the computer is still successfully uploading data ---
		# blocking does not correspond to network activity.
		#
		if is_put:
			this.response = urlopen(req)
		else:
			this.response = urlopen(req, timeout=timeout)
		this.is_put = is_put

	def read(this, size=None):
		return this.response.read(size)

	def close(this):
		this.response.close()
		this.connection._release_response(this, this.is_put)

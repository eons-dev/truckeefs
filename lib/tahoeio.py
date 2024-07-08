from urllib.request import Request, urlopen
from urllib.parse import quote
from urllib.error import HTTPError
import json
import threading
import shutil
import logging


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


class TahoeConnection(object):
	def __init__(this, base_url, rootcap, timeout, max_connections=10):
		assert isinstance(base_url, str)
		assert isinstance(rootcap, str)

		this.base_url = base_url.rstrip('/') + '/uri'
		this.rootcap = rootcap.encode('utf-8')

		this.connections = []
		this.lock = threading.Lock()

		put_conns = max(1, max_connections//2)
		get_conns = max(1, max_connections - put_conns)

		this.get_semaphore = threading.Semaphore(get_conns)
		this.put_semaphore = threading.Semaphore(put_conns)
		this.timeout = timeout

	def _get_response(this, req, is_put):
		semaphore = this.put_semaphore if is_put else this.get_semaphore

		semaphore.acquire()
		try:
			response = TahoeResponse(this, req, is_put, this.timeout)
			with this.lock:
				this.connections.append(response)
				return response
		except:
			semaphore.release()
			raise

	def _release_response(this, response, is_put):
		semaphore = this.put_semaphore if is_put else this.get_semaphore

		with this.lock:
			if response in this.connections:
				semaphore.release()
				this.connections.remove(response)

	def wait_until_write_allowed(this):
		# Force wait if put queue is full
		this.put_semaphore.acquire()
		this.put_semaphore.release()

	def _url(this, path, params={}, iscap=False):
		assert isinstance(path, str), path

		path = quote(path).lstrip('/')
		if iscap:
			path = this.base_url + '/' + path
		else:
			path = this.base_url + '/' + this.rootcap.decode('ascii') + '/' + path

		if params:
			path += '?'

			for k, v in list(params.items()):
				assert isinstance(k, str), k
				assert isinstance(v, str), v
				if not path.endswith('?'):
					path += '&'
				k = quote(k, safe='')
				v = quote(v, safe='')
				path += k
				path += '='
				path += v

		return path

	def _get_request(this, method, path, offset=None, length=None, data=None, params={}, iscap=False):
		headers = {'Accept': 'text/plain'}

		if offset is not None or length is not None:
			if offset is None:
				start = "0"
				offset = 0
			else:
				start = str(offset)
			if length is None:
				end = ""
			else:
				end = str(offset + length - 1)
			headers['Range'] = 'bytes=' + start + '-' + end

		req = Request(this._url(path, params, iscap=iscap),
					  data=data,
					  headers=headers)
		req.get_method = lambda: method
		return req

	def _get(this, path, params={}, offset=None, length=None, iscap=False):
		req = this._get_request("GET", path, params=params, offset=offset, length=length, iscap=iscap)
		return this._get_response(req, False)

	def _post(this, path, data=None, params={}, iscap=False):
		req = this._get_request("POST", path, data=data, params=params, iscap=iscap)
		return this._get_response(req, False)

	def _put(this, path, data=None, params={}, iscap=False):
		req = this._get_request("PUT", path, data=data, params=params, iscap=iscap)
		return this._get_response(req, True)

	def _delete(this, path, params={}, iscap=False):
		req = this._get_request("DELETE", path, params=params, iscap=iscap)
		return this._get_response(req, False)

	def get_info(this, path, iscap=False):
		f = this._get(path, {'t': 'json'}, iscap=iscap)
		try:
			data = json.load(f)
		finally:
			f.close()
		return data

	def get_content(this, path, offset=None, length=None, iscap=False):
		return this._get(path, offset=offset, length=length, iscap=iscap)

	def put_file(this, path, f, iscap=False):
		f = this._put(path, data=f, iscap=iscap)
		try:
			return f.read().decode('utf-8').strip()
		finally:
			f.close()

	def delete(this, path, iscap=False):
		f = this._delete(path, iscap=iscap)
		try:
			return f.read().decode('utf-8').strip()
		finally:
			f.close()

	def mkdir(this, path, iscap=False):
		f = this._post(path, params={'t': 'mkdir'}, iscap=iscap)
		try:
			return f.read().decode('utf-8').strip()
		finally:
			f.close()

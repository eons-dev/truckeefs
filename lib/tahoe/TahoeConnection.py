"""
lib/tahoe/TahoeConnection.py

Purpose:
Provides an API client for interacting with a Tahoe LAFS node. It wraps HTTP methods (GET, POST, PUT, DELETE) and constructs appropriate URLs based on a root capability.

Place in Architecture:
The primary interface between your local file system and the remote Tahoe backend. All remote file operations (e.g. file upload/download, directory creation) go through this connection.

Interface:

	__init__(base_url, rootcap, timeout, max_connections=10): Initializes the connection parameters and semaphores for concurrency.
	Internal methods: _get_response(), _release_response(), wait_until_write_allowed(), _url(), _get_request().
	Public methods:
		_get(), _post(), _put(), _delete()
		get_info(path, iscap=False), get_content(path, offset, length, iscap=False), put_file(path, file, iscap=False), delete(path, iscap=False), mkdir(path, iscap=False).

TODOs/FIXMEs:
Minor comments regarding PUT request timeout and send-buffer size; consider switching to a more flexible HTTP client library (e.g. requests) if needed.
"""

from urllib.request import Request, urlopen
from urllib.parse import quote
from urllib.error import HTTPError
import json
import threading
import shutil
import logging
from .TahoeResponse import *

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
		file = this._get(path, {'t': 'json'}, iscap=iscap)
		try:
			data = json.load(file)
		finally:
			file.close()
		return data

	def get_content(this, path, offset=None, length=None, iscap=False):
		return this._get(path, offset=offset, length=length, iscap=iscap)

	def put_file(this, path, file, iscap=False):
		file = this._put(path, data=file, iscap=iscap)
		try:
			return file.read().decode('utf-8').strip()
		finally:
			file.close()

	def delete(this, path, iscap=False):
		file = this._delete(path, iscap=iscap)
		try:
			return file.read().decode('utf-8').strip()
		finally:
			file.close()

	def mkdir(this, path, iscap=False):
		file = this._post(path, params={'t': 'mkdir'}, iscap=iscap)
		try:
			return file.read().decode('utf-8').strip()
		finally:
			file.close()

import time

# Sleep for exponentially increasing time. `n` is the number of times
# sleep has been called.
def ExponentialSleep(n, start=0.1, max_sleep=60):
	sleep_time = min(start * (2**n), max_sleep)
	time.sleep(sleep_time)

class RandomString(object):
	def __init__(this, size):
		this.size = size

	def __len__(this):
		return this.size

	def __getitem__(this, k):
		if isinstance(k, slice):
			return os.urandom(len(range(*k.indices(this.size))))
		else:
			raise IndexError("invalid index")


def json_zlib_dump(obj, fp):
	try:
		fp.write(zlib.compress(json.dumps(obj).encode('utf-8'), 3))
	except zlib.error:
		raise ValueError("compression error")


def json_zlib_load(fp):
	try:
		return json.load(ZlibDecompressor(fp))
	except zlib.error:
		raise ValueError("invalid compressed stream")


class ZlibDecompressor(object):
	def __init__(this, fp):
		this.fp = fp
		this.decompressor = zlib.decompressobj()
		this.buf = b""
		this.eof = False

	def read(this, sz=None):
		if sz is not None and not (sz > 0):
			return b""

		while not this.eof and (sz is None or sz > len(this.buf)):
			block = this.fp.read(131072)
			if not block:
				this.buf += this.decompressor.flush()
				this.eof = True
				break
			this.buf += this.decompressor.decompress(block)

		if sz is None:
			block = this.buf
			this.buf = b""
		else:
			block = this.buf[:sz]
			this.buf = this.buf[sz:]
		return block


def udirname(upath):
	return "/".join(upath.split("/")[:-1])


def ubasename(upath):
	return upath.split("/")[-1]


# constants for cache score calculation
_DOWNLOAD_SPEED = 1e6  # byte/sec
_LATENCY = 1.0 # sec

def _access_rate(size, t):
	"""Return estimated access rate (unit 1/sec). `t` is time since last access"""
	if t < 0:
		return 0.0
	size_unit = 100e3
	size_prob = 1 / (1 + (size/size_unit)**2)
	return size_prob / (_LATENCY + t)

def cache_score(size, t):
	"""
	Return cache score for file with size `size` and time since last access `t`.
	Bigger number means higher priority.
	"""

	# Estimate how often it is downloaded
	rate = _access_rate(size, t)

	# Maximum size up to this time
	dl_size = _DOWNLOAD_SPEED * max(0, t - _LATENCY)

	# Time cost for re-retrieval
	return rate * (_LATENCY + min(dl_size, size) / _DOWNLOAD_SPEED)


import re

def parse_size(size_str):
	multipliers = {
		't': 1000**4,
		'g': 1000**3,
		'm': 1000**2,
		'k': 1000**1,
		'tb': 1000**4,
		'gb': 1000**3,
		'mb': 1000**2,
		'kb': 1000**1,
		'tib': 1024**4,
		'gib': 1024**3,
		'mib': 1024**2,
		'kib': 1024**1,
	}
	size_re = re.compile(r'^\s*(\d+)\s*(%s)?\s*$' % ("|".join(list(multipliers.keys())),), 
						 re.I)

	m = size_re.match(size_str)
	if not m:
		raise ValueError("not a valid size specifier")

	size = int(m.group(1))
	multiplier = m.group(2)
	if multiplier is not None:
		try:
			size *= multipliers[multiplier.lower()]
		except KeyError:
			raise ValueError("invalid size multiplier")

	return size


def parse_lifetime(lifetime_str):
	if (type(lifetime_str) == int):
		return lifetime_str

	if lifetime_str.lower() in ('inf', 'infinity', 'infinite'):
		return 100*365*24*60*60

	try:
		return int(lifetime_str)
	except ValueError:
		raise ValueError("invalid lifetime specifier")
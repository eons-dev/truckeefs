import sqlalchemy as sql
import sqlalchemy.orm as orm

# Inodes store the usable metadata for files and directories.
# Each has an id that is the primary key and path-idependent means of access. The id makes it possible to rename the object without having to change the upath of children. Essentially the id is a mock inode number.
class InodeModel(orm.declarative_base()):
	__tablename__ = 'fs'

	# Lookup info.
	id = sql.Column(sql.Integer, primary_key=True)
	name = sql.Column(sql.String, nullable=False)
	kind = sql.Column(sql.String, nullable=False) # Python class name for re-creating the right Inode subclass.

	# Filesystem data.
	meta = Column(sql.JSON) # For storing metadata, e.g. xattrs
	last_accessed = sql.Column(sql.Integer, default=0) # Access time is used in caching, so stored separately / doubly from `meta`.
	parents = Column(sql.JSON)
	children = Column(sql.JSON) # Only for directories
	data = Column(sql.String) # Only for files. Path to file on disk, not actual file data.

	def __repr__(this):
		return f"<{this.name} ({this.id}) @ {this.upath}>"

	def __init__(this):
		pass
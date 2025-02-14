Big Picture Overview
Architecture and Workflow
Layered Design

	Caching Layer:
		Files:
		Uses a robust local caching mechanism that stores file data in fixed-size blocks.
		Implements logical inodes via CachedFileInode and file handles via CachedFileHandle.
		Directories:
		Manages directory metadata with CachedDirInode and CachedDirHandle.
		Purpose:
		Reduces the need to constantly interact with the remote backend by serving local requests quickly.

	Block Storage & File I/O:
		Originally built with encryption (using Crypt.py), now replaced by a plain I/O layer (renamed to FileOnDisk).
		Allows efficient random access and partial reads/writes while preserving metadata.

	Tahoe Integration:
		The remote storage backend is provided by Tahoe LAFS.
		Modules such as TahoeConnection, TahoeResponse, and TahoeSyncWorker translate local operations (uploads, downloads, directory creation/deletion) into remote API calls.

	Core Filesystem Operations:
		RiverFS Executor:
		Located in RiverFS.py, it coordinates operations using the asynchronous framework provided by the eons library.
		FS Operations (FSOps):
		Implemented as stateless functors that wrap typical POSIX actions (open, read, write, unlink, mkdir, etc.) and interact with both the local caching layer and the remote Tahoe backend.

	State Management & Persistence:
		Persistent Storage:
		Inode metadata is stored in a SQL database using SQLAlchemy (via InodeModel).
		Ephemeral State:
		Redis is used (via the RiverDelta module) to manage transient state such as semaphore locks and process states.
		Purpose:
		Ensures consistency and manages concurrency for operations like uploads and downloads.

Operation Flow

	Local Requests:
	When a file operation is initiated (e.g., via FUSE), the corresponding FSOp is invoked. The system checks the local cache for data and returns it immediately if fresh.

	Synchronization:
		If the local cache is stale or data has been modified, synchronization hooks are triggered:
			Upstream Hooks: BeforePushUpstream(), PushUpstream(), and AfterPushUpstream() upload changes to Tahoe.
			Downstream Hooks: BeforePullDownstream(), PullDownstream(), and AfterPullDownstream() download updated content.

	Concurrency:
	The system is designed for high concurrency:
		Multithreading is used for downloads.
		A single-process model is used for uploads.
		Semaphores and locks ensure thread safety across the caching and state management layers.

Strengths

	High Performance & Concurrency:
		Local caching minimizes remote calls.
		Asynchronous FSOps (backed by eons) support high concurrency, especially for download-intensive workloads.

	Modularity & Extensibility:
		The layered architecture allows independent evolution of components (e.g., caching, Tahoe integration, FSOps).
		It’s easier to update or replace individual components.

	Distributed Storage via Tahoe LAFS:
		Leverages a distributed, fault-tolerant storage backend, ensuring data redundancy and availability even if some nodes fail.

	Robust State Management:
		Uses persistent SQL storage for inode metadata.
		Utilizes Redis for fast, transient state management.
		This dual approach minimizes data conflicts and handles concurrent operations effectively.

	Customizability:
		Configurable cache size, TTL, and network timeouts allow tuning for different environments and performance needs.
		Designed to favor speed (e.g., for local mounts) while still providing POSIX compliance.

Requirements to Use

	Python Environment:
		Requires a suitable Python version with dependencies such as:
			fuse-python (FUSE binding for Python)
			eons (for asynchronous functor support)
			SQLAlchemy (for ORM and persistent storage)
			A Redis client library (for ephemeral state management)
			Other standard libraries (e.g., os, struct, fcntl)

	FUSE Installation:
		FUSE must be installed on the system with appropriate permissions for mounting file systems.

	Tahoe LAFS Node:
		A Tahoe LAFS node must be running (e.g., on http://localhost:3456) to serve as the remote backend.

	Cache Directory and Database Setup:
		A local cache directory (e.g., .tahoe-cache) needs to be set up.
		SQL and Redis connections (configured via RiverDelta) must be established for persistent and ephemeral state.

	MinIO (Optional):
		If using MinIO on top of the FUSE mount, ensure that the mount behaves as a standard POSIX file system that meets MinIO’s requirements.
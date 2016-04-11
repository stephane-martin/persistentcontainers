
cdef class LmdbOptions(object):
    def __init__(self, fixed_map=False, no_subdir=False, read_only=False, write_map=False, no_meta_sync=False,
                 no_sync=False, map_async=False, no_tls=True, no_lock=False, no_read_ahead=False, no_mem_init=False,
                 map_size=10485760, max_readers=126, max_dbs=16):

        self.fixed_map = fixed_map
        self.no_subdir = no_subdir
        self.read_only = read_only
        self.write_map = write_map
        self.no_meta_sync = no_meta_sync
        self.no_sync = no_sync
        self.map_async = map_async
        self.no_tls = no_tls
        self.no_lock = no_lock
        self.no_read_ahead = no_read_ahead
        self.no_mem_init = no_mem_init
        self.map_size = map_size
        self.max_readers = max_readers
        self.max_dbs = max_dbs

    @staticmethod
    cdef from_cpp(lmdb_options opts):
        return LmdbOptions(opts.fixed_map, opts.no_subdir, opts.read_only, opts.write_map, opts.no_meta_sync,
                           opts.no_sync, opts.map_async, opts.no_tls, opts.no_lock, opts.no_read_ahead, opts.no_mem_init,
                           opts.map_size, opts.max_readers, opts.max_dbs)

    property fixed_map:
        def __get__(self):
            return self.opts.fixed_map
        def __set__(self, fixed_map):
            self.opts.fixed_map = bool(fixed_map)

    property no_subdir:
        def __get__(self):
            return self.opts.no_subdir
        def __set__(self, no_subdir):
            self.opts.no_subdir = bool(no_subdir)

    property read_only:
        def __get__(self):
            return self.opts.read_only
        def __set__(self, read_only):
            self.opts.read_only = bool(read_only)

    property write_map:
        def __get__(self):
            return self.opts.write_map
        def __set__(self, write_map):
            self.opts.fixed_map = bool(write_map)

    property no_meta_sync:
        def __get__(self):
            return self.opts.no_meta_sync
        def __set__(self, no_meta_sync):
            self.opts.no_meta_sync = bool(no_meta_sync)

    property no_sync:
        def __get__(self):
            return self.opts.no_sync
        def __set__(self, no_sync):
            self.opts.no_sync = bool(no_sync)

    property map_async:
        def __get__(self):
            return self.opts.map_async
        def __set__(self, map_async):
            self.opts.map_async = bool(map_async)

    property no_tls:
        def __get__(self):
            return self.opts.no_tls
        def __set__(self, no_tls):
            self.opts.no_tls = bool(no_tls)

    property no_lock:
        def __get__(self):
            return self.opts.no_lock
        def __set__(self, no_lock):
            self.opts.no_lock = bool(no_lock)

    property no_read_ahead:
        def __get__(self):
            return self.opts.no_read_ahead
        def __set__(self, no_read_ahead):
            self.opts.no_read_ahead = bool(no_read_ahead)

    property no_mem_init:
        def __get__(self):
            return self.opts.no_mem_init
        def __set__(self, no_mem_init):
            self.opts.no_mem_init = bool(no_mem_init)

    property map_size:
        def __get__(self):
            return self.opts.map_size
        def __set__(self, map_size):
            map_size = int(map_size)
            if map_size <= 0:
                raise ValueError()
            self.opts.map_size = map_size

    property max_readers:
        def __get__(self):
            return self.opts.max_readers
        def __set__(self, max_readers):
            max_readers = int(max_readers)
            if max_readers <= 0:
                raise ValueError()
            self.opts.max_readers = max_readers

    property max_dbs:
        def __get__(self):
            return self.opts.max_dbs
        def __set__(self, max_dbs):
            max_dbs = int(max_dbs)
            if max_dbs <= 0:
                raise ValueError()
            self.opts.max_dbs = max_dbs

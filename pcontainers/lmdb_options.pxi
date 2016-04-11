cdef extern from "lmdb_options.h" namespace "lmdb" nogil:
    # noinspection PyPep8Naming
    cdef cppclass lmdb_options:
        lmdb_options()
        cpp_bool fixed_map;
        cpp_bool no_subdir;
        cpp_bool read_only;
        cpp_bool write_map;
        cpp_bool no_meta_sync;
        cpp_bool no_sync;
        cpp_bool map_async;
        cpp_bool no_tls;
        cpp_bool no_lock;
        cpp_bool no_read_ahead;
        cpp_bool no_mem_init;
        size_t map_size;
        unsigned int max_readers;
        unsigned int max_dbs;


cdef class LmdbOptions(object):
    cdef lmdb_options opts

    @staticmethod
    cdef from_cpp(lmdb_options opts)

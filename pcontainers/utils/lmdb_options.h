#pragma once

#include "lmdb.h"

namespace lmdb {

class lmdb_options {
public:
    bool fixed_map;
    bool no_subdir;
    bool read_only;
    bool write_map;
    bool no_meta_sync;
    bool no_sync;
    bool map_async;
    bool no_tls;
    bool no_lock;
    bool no_read_ahead;
    bool no_mem_init;
    size_t map_size;
    unsigned int max_readers;
    unsigned int max_dbs;

    lmdb_options() BOOST_NOEXCEPT_OR_NOTHROW {
        fixed_map = false;
        no_subdir = false;
        read_only = false;
        write_map = false;
        no_meta_sync = false;
        no_sync = false;
        map_async = false;
        no_tls = true;
        no_lock = false;
        no_read_ahead = false;
        no_mem_init = false;
        map_size = 10485760;
        max_readers = 126;
        max_dbs = 16;
    }

    unsigned int get_flags() const BOOST_NOEXCEPT_OR_NOTHROW {
        unsigned int flags = 0;
        if (fixed_map) {
            flags |= MDB_FIXEDMAP;
        }
        if (no_subdir) {
            flags |= MDB_NOSUBDIR;
        }
        if (read_only) {
            flags |= MDB_RDONLY;
        }
        if (write_map) {
            flags |= MDB_WRITEMAP;
        }
        if (no_meta_sync) {
            flags |= MDB_NOMETASYNC;
        }
        if (no_sync) {
            flags |= MDB_NOSYNC;
        }
        if (map_async) {
            flags |= MDB_MAPASYNC;
        }
        if (no_tls) {
            flags |= MDB_NOTLS;
        }
        if (no_lock) {
            flags |= MDB_NOLOCK;
        }
        if (no_read_ahead) {
            flags |= MDB_NORDAHEAD;
        }
        if (no_mem_init) {
            flags |= MDB_NOMEMINIT;
        }
        return flags;

    }
};




}

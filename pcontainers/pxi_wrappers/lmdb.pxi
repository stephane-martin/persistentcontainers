cdef extern from "lmdb.h" nogil:
    ctypedef struct MDB_txn:
        pass

    ctypedef struct MDB_val:
        size_t mv_size
        void* mv_data

    int mdb_txn_commit(MDB_txn *txn)
    void mdb_txn_abort(MDB_txn *txn)

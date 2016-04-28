# -*- coding: utf-8 -*-

# noinspection PyUnresolvedReferences
from cpython.ref cimport PyObject

cdef public PyObject* py_lmdb_error
cdef public PyObject* py_not_initialized
cdef public PyObject* py_access_error
cdef public PyObject* py_key_exist
cdef public PyObject* py_not_found
cdef public PyObject* py_empty_key
cdef public PyObject* py_empty_database
cdef public PyObject* py_page_not_found
cdef public PyObject* py_corrupted
cdef public PyObject* py_panic
cdef public PyObject* py_version_mismatch
cdef public PyObject* py_invalid
cdef public PyObject* py_map_full
cdef public PyObject* py_dbs_full
cdef public PyObject* py_readers_full
cdef public PyObject* py_tls_full
cdef public PyObject* py_txn_full
cdef public PyObject* py_cursor_full
cdef public PyObject* py_page_full
cdef public PyObject* py_map_resized
cdef public PyObject* py_incompatible
cdef public PyObject* py_bad_rslot
cdef public PyObject* py_bad_txn
cdef public PyObject* py_bad_val_size
cdef public PyObject* py_bad_dbi
cdef public PyObject* py_expired
cdef public PyObject* py_stopping
cdef public void custom_handler()

cdef extern from "custom_exc.h" namespace "quiet":
    cdef void custom_exception_handler()



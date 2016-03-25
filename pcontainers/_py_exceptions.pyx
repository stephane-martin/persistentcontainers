# -*- coding: utf-8 -*-

# noinspection PyUnresolvedReferences
from cpython.ref cimport PyObject


class LmdbError(RuntimeError):
    """
    Base exception class for LMDB errors
    """

class NotInitialized(LmdbError):
    pass

class AccessError(LmdbError, IOError):
    pass

class KeyExist(LmdbError, KeyError):
    pass

class NotFound(LmdbError, KeyError):
    pass

class EmptyKey(LmdbError, ValueError):
    pass

class EmptyDatabase(LmdbError):
    pass

class PageNotFound(LmdbError):
    pass

class Corrupted(LmdbError):
    pass

class Panic(LmdbError):
    pass

class VersionMismatch(LmdbError):
    pass

class Invalid(LmdbError, ValueError):
    pass

class MapFull(LmdbError):
    pass

class DbsFull(LmdbError):
    pass

class ReadersFull(LmdbError):
    pass

class TlsFull(LmdbError):
    pass

class TxnFull(LmdbError):
    pass

class CursorFull(LmdbError):
    pass

class PageFull(LmdbError):
    pass

class MapResized(LmdbError):
    pass

class Incompatible(LmdbError):
    pass

class BadRslot(LmdbError):
    pass

class BadTxn(LmdbError):
    pass

class BadValSize(LmdbError, ValueError):
    pass

class BadDbi(LmdbError, ValueError):
    pass

cdef public PyObject* py_lmdb_error = <PyObject*>LmdbError
cdef public PyObject* py_not_initialized = <PyObject*>NotInitialized
cdef public PyObject* py_access_error = <PyObject*>AccessError
cdef public PyObject* py_key_exist = <PyObject*>KeyExist
cdef public PyObject* py_not_found = <PyObject*>NotFound
cdef public PyObject* py_empty_key = <PyObject*>EmptyKey
cdef public PyObject* py_empty_database = <PyObject*>EmptyDatabase
cdef public PyObject* py_page_not_found = <PyObject*>PageNotFound
cdef public PyObject* py_corrupted = <PyObject*>Corrupted
cdef public PyObject* py_panic = <PyObject*>Panic
cdef public PyObject* py_version_mismatch = <PyObject*>VersionMismatch
cdef public PyObject* py_invalid = <PyObject*>Invalid
cdef public PyObject* py_map_full = <PyObject*>MapFull
cdef public PyObject* py_dbs_full = <PyObject*>DbsFull
cdef public PyObject* py_readers_full = <PyObject*>ReadersFull
cdef public PyObject* py_tls_full = <PyObject*>TlsFull
cdef public PyObject* py_txn_full = <PyObject*>TxnFull
cdef public PyObject* py_cursor_full = <PyObject*>CursorFull
cdef public PyObject* py_page_full = <PyObject*>PageFull
cdef public PyObject* py_map_resized = <PyObject*>MapResized
cdef public PyObject* py_incompatible = <PyObject*>Incompatible
cdef public PyObject* py_bad_rslot = <PyObject*>BadRslot
cdef public PyObject* py_bad_txn = <PyObject*>BadTxn
cdef public PyObject* py_bad_val_size = <PyObject*>BadValSize
cdef public PyObject* py_bad_dbi = <PyObject*>BadDbi

cdef void custom_handler():
    custom_exception_handler()

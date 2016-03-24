# -*- coding: utf-8 -*-

# noinspection PyUnresolvedReferences
from cpython.ref cimport PyObject

from cpython.unicode cimport PyUnicode_AsUTF8String, PyUnicode_Check
from cpython.bytes cimport PyBytes_Check, PyBytes_FromStringAndSize

# noinspection PyUnresolvedReferences
from libcpp cimport bool as cpp_bool
from libcpp.vector cimport vector
from libcpp.utility cimport pair
# noinspection PyUnresolvedReferences
from libc.time cimport time_t
from libc.stdlib cimport malloc
from libc.string cimport memcpy
from cpython.buffer cimport PyBUF_SIMPLE, PyBuffer_FillInfo
from cpython.mem cimport PyMem_Malloc

# noinspection PyPackageRequirements
from mbufferio import MBufferIO
# noinspection PyUnresolvedReferences
from ._py_exceptions cimport custom_handler


cdef extern from "Python.h":
    # noinspection PyPep8Naming
    int PyByteArray_Resize(object ba, Py_ssize_t l)
    # noinspection PyPep8Naming
    object PyMemoryView_FromBuffer(Py_buffer *view)

include "scoped_ptr.pxi"
include "shared_ptr.pxi"
include "cbstring.pxi"

cdef extern from "lmdb.h" nogil:
    ctypedef struct MDB_txn:
        pass

    ctypedef struct MDB_val:
        size_t mv_size
        void* mv_data

    int mdb_txn_commit(MDB_txn *txn)
    void mdb_txn_abort(MDB_txn *txn)

include "pyfunctor.pxi"
include "logging.pxi"

cdef extern from "pyutils.h" namespace "utils":
    # noinspection PyPep8Naming
    cppclass PyBufferWrap:
        PyBufferWrap() nogil
        PyBufferWrap(object o) except +custom_handler
        Py_buffer* get() nogil
        MDB_val get_mdb_val() nogil
        Py_ssize_t length() nogil
        void* buf() nogil
        void close()

    cdef PyBufferWrap move "boost::move"(PyBufferWrap other)

include "lmdb_options.pxi"
include "persistentdict.pxi"
include "persistentqueue.pxi"


cdef extern from "utils.h" namespace "utils" nogil:

    # noinspection PyPep8Naming
    cppclass TempDirectory:
        TempDirectory()
        TempDirectory(cpp_bool create)
        TempDirectory(cpp_bool create, cpp_bool destroy)
        TempDirectory(cpp_bool create, cpp_bool destroy, const CBString& tmpl)
        CBString get_path()

    shared_ptr[TempDirectory] make_temp_directory "utils::TempDirectory::make"()
    shared_ptr[TempDirectory] make_temp_directory "utils::TempDirectory::make"(cpp_bool create)
    shared_ptr[TempDirectory] make_temp_directory "utils::TempDirectory::make"(cpp_bool create, cpp_bool destroy)

cpdef set_logger(int level=?)
cpdef set_python_logger(name)

include "pdict.pxi"
include "pqueue.pxi"
include "expirydict.pxi"
include "helpers.pxi"
include "filters.pxi"

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
from libc.stdint cimport uint64_t
from libc.stdlib cimport malloc
from libc.string cimport memcpy
from cpython.buffer cimport PyBUF_SIMPLE, PyBuffer_FillInfo
from cpython.mem cimport PyMem_Malloc
from cpython.ref cimport Py_INCREF, Py_DECREF, Py_CLEAR

# noinspection PyPackageRequirements
from mbufferio import MBufferIO
# noinspection PyUnresolvedReferences
from ._py_exceptions cimport custom_handler


cdef extern from "Python.h":
    # noinspection PyPep8Naming
    int PyByteArray_Resize(object ba, Py_ssize_t l)
    # noinspection PyPep8Naming
    object PyMemoryView_FromBuffer(Py_buffer *view)

include "pxi_wrappers/scoped_ptr.pxi"
include "pxi_wrappers/shared_ptr.pxi"
include "pxi_wrappers/shared_future.pxi"
include "pxi_wrappers/cbstring.pxi"
include "pxi_wrappers/lmdb.pxi"
include "pxi_wrappers/pyfunctor.pxi"
include "pxi_wrappers/logging.pxi"
include "pxi_wrappers/pyutils.pxi"
include "pxi_wrappers/lmdb_options.pxi"
include "pxi_wrappers/persistentdict.pxi"
include "pxi_wrappers/persistentqueue.pxi"
include "pxi_wrappers/bufferedpersistentdict.pxi"
include "pxi_wrappers/utils.pxi"
include "pxi_wrappers/mutex.pxi"


cpdef set_logger(int level=?)
cpdef set_python_logger(name)

include "pdict.pxi"
include "pqueue.pxi"
include "cpp_future_wrapper.pxi"
include "buffered_pdict.pxi"
include "expiry_dict.pxi"
include "threadsafe_queue.pxi"
include "monotonic.pxi"
include "helpers.pxi"
include "filters.pxi"

# -*- coding: utf-8 -*-

# noinspection PyUnresolvedReferences
from cpython.ref cimport PyObject

from cpython.unicode cimport PyUnicode_AsUTF8String, PyUnicode_Check
from cpython.bytes cimport PyBytes_Check

from libcpp.string cimport string
# noinspection PyUnresolvedReferences
from libcpp cimport bool as cpp_bool
from libcpp.vector cimport vector
from libcpp.utility cimport pair

# noinspection PyUnresolvedReferences
from ._py_exceptions cimport custom_handler

cdef extern from "Python.h":
    # noinspection PyPep8Naming
    int PyByteArray_Resize(object ba, Py_ssize_t l)
    # noinspection PyPep8Naming
    object PyMemoryView_FromBuffer(Py_buffer *view)

cdef extern from "lmdb.h" nogil:
    ctypedef struct MDB_txn:
        pass

    ctypedef struct MDB_val:
        size_t mv_size
        void* mv_data

    int mdb_txn_commit(MDB_txn *txn)
    void mdb_txn_abort(MDB_txn *txn)

cdef extern from "pyfunctor.h" namespace "quiet" nogil:
    # noinspection PyPep8Naming
    cppclass unary_predicate:
        pass
    # noinspection PyPep8Naming
    cppclass binary_predicate:
        pass
    # noinspection PyPep8Naming
    cppclass unary_functor:
        pass
    # noinspection PyPep8Naming
    cppclass binary_functor:
        pass

    cdef unary_predicate make_unary_predicate "quiet::PyPredicate::make_unary_predicate"(PyObject* obj)
    cdef binary_predicate make_binary_predicate "quiet::PyPredicate::make_binary_predicate"(PyObject* obj)
    cdef unary_functor make_unary_functor "quiet::PyFunctor::make_unary_functor"(PyObject* obj)
    cdef binary_functor make_binary_functor "quiet::PyFunctor::make_binary_functor"(PyObject* obj)

    # noinspection PyPep8Naming
    cdef cppclass PyStringInputIterator:
        PyStringInputIterator()
        PyStringInputIterator(PyObject* obj)


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

cdef extern from "persistentdict.h" namespace "quiet" nogil:

    # noinspection PyPep8Naming
    cdef cppclass cppPersistentDict "quiet::PersistentDict":
        cppPersistentDict() except +custom_handler
        cppPersistentDict(const string& directory_name) except +custom_handler
        cppPersistentDict(const string& directory_name, const string& database_name) except +custom_handler
        cppPersistentDict(const string& directory_name, const string& database_name, const lmdb_options& opts) except +custom_handler
        cppPersistentDict(const cppPersistentDict& other)
        (cppPersistentDict&) operator=(cppPersistentDict other)

        int get_maxkeysize() except +custom_handler
        string get_dirname()
        string get_dbname()

        void insert(const string& key, const string& value) except +custom_handler
        void insert(MDB_val k, MDB_val v) except +custom_handler

        string at(const string& key) except +custom_handler
        string at(MDB_val k) except +custom_handler
        string get "quiet::PersistentDict::operator[]" (const string& key) except +custom_handler
        string pop(const string& key) except +custom_handler
        string pop(MDB_val k) except +custom_handler
        pair[string, string] popitem() except +custom_handler
        vector[string] get_all_keys() except +custom_handler
        vector[string] get_all_values() except +custom_handler
        vector[pair[string, string]] get_all_items() except +custom_handler

        void erase(const string& key) except +custom_handler
        vector[string] erase(const string& first, const string& last) except +custom_handler

        cpp_bool empty() except +custom_handler
        size_t size() except +custom_handler
        void clear() except +custom_handler
        cpp_bool contains(const string& key) except +custom_handler

        void transform_values(binary_functor binary_funct) except +custom_handler
        void remove_if(binary_predicate binary_pred) except +custom_handler

        cppPersistentDict copy_to(const string& directory_name)
        cppPersistentDict copy_to(const string& directory_name, const string& database_name)
        cppPersistentDict copy_to(const string& directory_name, const string& database_name, const lmdb_options& opts)
        cppPersistentDict copy_to(const string& directory_name, const string& database_name, const lmdb_options& opts, long chunk_size)

        cppIterator before() except +custom_handler
        cppIterator before(cpp_bool readonly) except +custom_handler
        cppConstIterator cbefore() except +custom_handler
        cppIterator begin() except +custom_handler
        cppIterator begin(cpp_bool readonly) except +custom_handler
        cppConstIterator cbegin() except +custom_handler
        cppIterator end() except +custom_handler
        cppIterator end(cpp_bool readonly) except +custom_handler
        cppConstIterator cend() except +custom_handler
        cppIterator last() except +custom_handler
        cppIterator last(cpp_bool readonly) except +custom_handler
        cppConstIterator clast() except +custom_handler


    # noinspection PyPep8Naming
    cdef cppclass cppIterator "quiet::PersistentDict::iterator":
        cppIterator() except +custom_handler
        cppIterator(cppPersistentDict* d) except +custom_handler
        cppIterator(cppPersistentDict* d, int pos) except +custom_handler
        cppIterator(cppPersistentDict* d, int pos, cpp_bool readonly) except +custom_handler
        cppIterator(cppPersistentDict* d, const string& key) except +custom_handler
        cppIterator(cppPersistentDict* d, MDB_val key) except +custom_handler
        cppIterator(cppPersistentDict* d, const string& key, cpp_bool readonly) except +custom_handler
        cppIterator(cppPersistentDict* d, MDB_val key, cpp_bool readonly) except +custom_handler

        cppIterator(const cppIterator& other) except +custom_handler
        cppIterator(const cppConstIterator& other) except +custom_handler

        string get_key() except +custom_handler
        MDB_val get_key_buffer() except +custom_handler
        string get_value() except +custom_handler
        MDB_val get_value_buffer() except +custom_handler
        pair[string, string] get_item() except +custom_handler
        pair[MDB_val, MDB_val] get_item_buffer() except +custom_handler
        void set_value(const string& value) except +custom_handler
        void set_key_value(const string& key, const string& value) except +custom_handler
        void set_key_value(MDB_val key, const MDB_val value) except +custom_handler

        cpp_bool has_reached_end() except +custom_handler
        cpp_bool has_reached_beginning() except +custom_handler

        (cppIterator&) incr "quiet::PersistentDict::iterator::operator++"() except +custom_handler
        (cppIterator&) decr "quiet::PersistentDict::iterator::operator--"() except +custom_handler
        void dlte "quiet::PersistentDict::iterator::del"() except +custom_handler

        cpp_bool operator==(const cppIterator& other) except +custom_handler
        cpp_bool operator!=(const cppIterator& other) except +custom_handler

        void set_rollback()
        void set_rollback(cpp_bool val)

    # noinspection PyPep8Naming
    cdef cppclass cppConstIterator "quiet::PersistentDict::const_iterator":
        cppConstIterator() except +custom_handler
        cppConstIterator(cppPersistentDict* d) except +custom_handler
        cppConstIterator(cppPersistentDict* d, int pos) except +custom_handler
        cppConstIterator(cppPersistentDict* d, const string& key) except +custom_handler
        cppConstIterator(cppPersistentDict* d, MDB_val key) except +custom_handler

        cppConstIterator(const cppConstIterator& other) except +custom_handler

        string get_key() except +custom_handler
        MDB_val get_key_buffer() except +custom_handler
        string get_value() except +custom_handler
        MDB_val get_value_buffer() except +custom_handler
        pair[string, string] get_item() except +custom_handler
        pair[MDB_val, MDB_val] get_item_buffer() except +custom_handler

        cpp_bool has_reached_end() except +custom_handler
        cpp_bool has_reached_beginning() except +custom_handler

        (cppConstIterator&) incr "quiet::PersistentDict::const_iterator::operator++"() except +custom_handler
        (cppConstIterator&) decr "quiet::PersistentDict::const_iterator::operator--"() except +custom_handler

        cpp_bool operator==(const cppConstIterator& other) except +custom_handler
        cpp_bool operator!=(const cppConstIterator& other) except +custom_handler

        void set_rollback()
        void set_rollback(cpp_bool val)

    cdef cppIterator move "boost::move"(cppIterator other)
    cdef cppConstIterator move "boost::move"(cppConstIterator other)


cdef extern from "persistentqueue.h" namespace "quiet" nogil:
    # noinspection PyPep8Naming
    cdef cppclass cppPersistentQueue "quiet::PersistentQueue":
        cppPersistentQueue() except +custom_handler
        cppPersistentQueue(const string& directory_name) except +custom_handler
        cppPersistentQueue(const string& directory_name, const string& database_name) except +custom_handler
        cppPersistentQueue(const string& directory_name, const string& database_name, const lmdb_options& opts) except +custom_handler
        cppPersistentQueue(const cppPersistentQueue& other)
        (cppPersistentQueue&) operator=(cppPersistentQueue other)

        string get_dirname()
        string get_dbname()

        void push_front(const string& val) except +custom_handler
        void push_front(size_t n, const string& val) except +custom_handler
        void push_front[InputIterator](InputIterator first, InputIterator last) except +custom_handler

        void push_back(const string& val) except +custom_handler
        void push_back(size_t n, const string& val) except +custom_handler
        void push_back[InputIterator](InputIterator first, InputIterator last) except +custom_handler

        string pop_back() except +custom_handler
        string pop_front() except +custom_handler
        vector[string] pop_all() except +custom_handler

        size_t size() except +custom_handler
        cpp_bool empty() except +custom_handler
        void clear() except +custom_handler

        cppPersistentQueue move_to(const string& directory_name) except +custom_handler
        cppPersistentQueue move_to(const string& directory_name, const string& database_name) except +custom_handler
        cppPersistentQueue move_to(const string& directory_name, const string& database_name, const lmdb_options& opts) except +custom_handler
        cppPersistentQueue move_to(const string& directory_name, const string& database_name, const lmdb_options& opts, long chunk_size) except +custom_handler

        void remove_if(unary_predicate unary_pred) except +custom_handler
        void transform_values(unary_functor unary_funct) except +custom_handler


cdef class PersistentStringDict(object):
    cdef cppPersistentDict* ptr
    cpdef noiterkeys(self)
    cpdef noitervalues(self)
    cpdef noiteritems(self)
    cpdef erase(self, first, last)
    cpdef get(self, key, default=?)
    cpdef pop(self, key, default=?)
    cpdef popitem(self)
    cpdef clear(self)
    cpdef has_key(self, key)
    cpdef transform_values(self, binary_funct)
    cpdef remove_if(self, binary_pred)
    cpdef iterkeys(self, reverse=?)
    cpdef itervalues(self, reverse=?)
    cpdef iteritems(self, reverse=?)
    cpdef copy_to(self, dirname, dbname=?, opts=?, chunk_size=?)
    cdef _getkey(self, cppConstIterator& it)
    cdef _getkey_buf(self, cppConstIterator& it)
    cdef _getvalue(self, cppConstIterator& it)
    cdef _getvalue_buf(self, cppConstIterator& it)
    cdef _getitem(self, cppConstIterator& it)
    cdef _getitem_buf(self, cppConstIterator& it)



cdef class PersistentDict(PersistentStringDict):
    pass


cdef class PersistentStringQueue(object):
    cdef cppPersistentQueue* ptr

    cpdef push_front(self, val)
    cpdef push_back(self, val)
    cpdef pop_back(self)
    cpdef pop_front(self)
    cpdef pop_all(self)

    cpdef clear(self)
    cpdef empty(self)
    cpdef qsize(self)
    cpdef full(self)
    cpdef put(self, item, block=?, timeout=?)
    cpdef get(self, block=?, timeout=?)
    cpdef put_nowait(self, item)
    cpdef get_nowait(self)

    cpdef transform_values(self, unary_funct)
    cpdef remove_if(self, unary_pred)
    cpdef move_to(self, dirname, dbname=?, opts=?, chunk_size=?)



cdef class PersistentQueue(PersistentStringQueue):
    pass


cdef class LmdbOptions(object):
    cdef lmdb_options opts


cdef inline unicode make_unicode(s):
    if s is None:
        return u''
    if PyUnicode_Check(s):
        return s
    if PyBytes_Check(s):
        return s.decode('utf-8')
    if hasattr(s, '__unicode__'):
        return s.__unicode__()
    if hasattr(s, '__bytes__'):
        return s.__bytes__().decode('utf-8')
    s = str(s)
    if PyUnicode_Check(s):
        return s
    return s.decode('utf-8')


cdef inline bytes make_utf8(s):
    if s is None:
        return b''
    if PyBytes_Check(s):
        return s
    if PyUnicode_Check(s):
        return PyUnicode_AsUTF8String(s)
    if hasattr(s, '__bytes__'):
        return s.__bytes__()
    if hasattr(s, '__unicode__'):
        return PyUnicode_AsUTF8String(s.__unicode__())
    s = str(s)
    if PyUnicode_Check(s):
        return PyUnicode_AsUTF8String(s)
    return s

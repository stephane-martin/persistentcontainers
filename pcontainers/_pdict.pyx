# -*- coding: utf-8 -*-

# noinspection PyUnresolvedReferences
from cpython.ref cimport PyObject
# noinspection PyUnresolvedReferences
from cython.operator cimport dereference as deref
from cpython.buffer cimport PyObject_GetBuffer, PyBuffer_Release, PyBUF_SIMPLE, PyBuffer_FillInfo
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.string cimport memcpy
from libc.stdlib cimport malloc
from libc.time cimport time as c_time

from time import sleep
try:
    import cPickle as pickle
except ImportError:
    import pickle
from queue import Empty

from mbufferio import MBufferIO

from ._py_exceptions import EmptyDatabase, NotFound


cdef class PersistentStringDict(object):
    def __cinit__(self, dirname=None, dbname="", opts=None, mapping=None, **kwarg):
        cdef string dirn = make_utf8(dirname)
        cdef string dbn = make_utf8(dbname)
        if dirn.empty():
            self.ptr = new cppPersistentDict()
            return
        if opts is None:
            opts = LmdbOptions()
        if not isinstance(opts, LmdbOptions):
            raise TypeError
        self.ptr = new cppPersistentDict(dirn, dbn, (<LmdbOptions> opts).opts)
        if mapping is not None:
            self.update(e=mapping)
        if kwarg:
            self.update(e=kwarg)

    def __init__(self, dirname=None, dbname="", opts=None, mapping=None, **kwarg):
        pass

    def __dealloc__(self):
        if self.ptr is not NULL:
            del self.ptr
            self.ptr = NULL

    def __repr__(self):
        return u"PersistentStringDict(dbname='{}', dirname='{}')".format(
            make_unicode(self.dbname), make_unicode(self.dirname)
        )

    def __reduce__(self):
        return dict, (self.noiteritems(), )

    property dirname:
        def __get__(self):
            return self.ptr.get_dirname()

    property dbname:
        def __get__(self):
            return self.ptr.get_dbname()

    # the following _get* functions allow us to release the GIL when retrieving data from LMDB
    # (we use memcpy to retrieve data from LMDB, which could block in case of page faults)
    # as we don't know the size of data before calling mdb_cursor_get, first we optimisticly try with a
    # preallocated 4096 bytes long buffer, so that in most cases we don't have to release the GIL twice

    # the _get*_buf functions avoid one memory copy to feed pickle

    # all this can be pretty useful when dealing with big values from LMDB

    cdef _getkey(self, cppConstIterator& it):
        cdef int keysize = self.ptr.get_maxkeysize()
        cdef MDB_val k
        cdef bytearray obj = bytearray(keysize)
        cdef Py_buffer* view = <Py_buffer*> PyMem_Malloc(sizeof(Py_buffer))
        PyObject_GetBuffer(obj, view, PyBUF_SIMPLE)
        try:
            with nogil:
                k = it.get_key_buffer()
                memcpy(view.buf, k.mv_data, k.mv_size)
        finally:
            PyBuffer_Release(view)
            PyMem_Free(view)
        PyByteArray_Resize(obj, k.mv_size)
        return obj

    cdef _getkey_buf(self, cppConstIterator& it):
        cdef MDB_val k
        cdef char* ptr
        with nogil:
            k = it.get_key_buffer()
            ptr = <char*> malloc(k.mv_size)
            memcpy(ptr, k.mv_data, k.mv_size)
        cdef Py_buffer* key_view = <Py_buffer*> PyMem_Malloc(sizeof(Py_buffer))
        PyBuffer_FillInfo(key_view, None, <void*> ptr, k.mv_size, 1,  PyBUF_SIMPLE)
        key_mview = PyMemoryView_FromBuffer(key_view)   # the new mview object now owns the view buffer, no need to free view
        return MBufferIO.from_mview(key_mview, True)    # the free(ptr) operation will be taken cared by MBufferIO

    cdef _getvalue(self, cppConstIterator& it):
        cdef MDB_val v
        cdef cpp_bool done = 0
        cdef bytearray obj = bytearray(4096)
        cdef Py_buffer* view = <Py_buffer*> PyMem_Malloc(sizeof(Py_buffer))
        PyObject_GetBuffer(obj, view, PyBUF_SIMPLE)
        try:
            with nogil:
                v = it.get_value_buffer()
                if v.mv_size <= 4096:
                    memcpy(view.buf, v.mv_data, v.mv_size)
                    done = 1
        except:
            PyBuffer_Release(view)
            PyMem_Free(view)
            raise

        PyBuffer_Release(view)
        if done:
            PyMem_Free(view)
            PyByteArray_Resize(obj, v.mv_size)
            return obj

        obj = bytearray(v.mv_size)
        PyObject_GetBuffer(obj, view, PyBUF_SIMPLE)
        with nogil:
            memcpy(view.buf, v.mv_data, v.mv_size)
        PyBuffer_Release(view)
        PyMem_Free(view)
        return obj

    cdef _getvalue_buf(self, cppConstIterator& it):
        cdef MDB_val v
        cdef char* ptr
        with nogil:
            v = it.get_value_buffer()
            ptr = <char*> malloc(v.mv_size)
            memcpy(ptr, v.mv_data, v.mv_size)
        cdef Py_buffer* value_view = <Py_buffer*> PyMem_Malloc(sizeof(Py_buffer))
        PyBuffer_FillInfo(value_view, None, <void*> ptr, v.mv_size, 1,  PyBUF_SIMPLE)
        value_mview = PyMemoryView_FromBuffer(value_view)   # the new mview object now owns the view buffer, no need to free view
        return MBufferIO.from_mview(value_mview, True)    # the free(ptr) operation will be taken cared by MBufferIO


    cdef _getitem(self, cppConstIterator& it):
        cdef int keysize = self.ptr.get_maxkeysize()
        cdef pair[MDB_val, MDB_val] kv
        cdef cpp_bool done = 0
        cdef bytearray key_obj = bytearray(keysize)
        cdef bytearray value_obj = bytearray(4096)
        cdef Py_buffer* key_view = <Py_buffer*> PyMem_Malloc(sizeof(Py_buffer))
        cdef Py_buffer* value_view = <Py_buffer*> PyMem_Malloc(sizeof(Py_buffer))
        PyObject_GetBuffer(key_obj, key_view, PyBUF_SIMPLE)
        PyObject_GetBuffer(value_obj, value_view, PyBUF_SIMPLE)

        try:
            with nogil:
                kv = it.get_item_buffer()
                memcpy(key_view.buf, kv.first.mv_data, kv.first.mv_size)
                if kv.second.mv_size <= 4096:
                    memcpy(value_view.buf, kv.second.mv_data, kv.second.mv_size)
                    done = 1
        except:
            PyBuffer_Release(value_view)
            PyBuffer_Release(key_view)
            PyMem_Free(value_view)
            PyMem_Free(key_view)
            raise

        PyBuffer_Release(value_view)
        PyBuffer_Release(key_view)
        PyMem_Free(key_view)
        PyByteArray_Resize(key_obj, kv.first.mv_size)
        if done:
            PyMem_Free(value_view)
            PyByteArray_Resize(value_obj, kv.second.mv_size)
            return key_obj, value_obj

        value_obj = bytearray(kv.second.mv_size)
        PyObject_GetBuffer(value_obj, value_view, PyBUF_SIMPLE)
        with nogil:
            memcpy(value_view.buf, kv.second.mv_data, kv.second.mv_size)
        PyBuffer_Release(value_view)
        PyMem_Free(value_view)
        return key_obj, value_obj

    cdef _getitem_buf(self, cppConstIterator& it):
        cdef pair[MDB_val, MDB_val] kv
        cdef char* ptr
        with nogil:
            kv = it.get_item_buffer()
            key_ptr = <char*> malloc(kv.first.mv_size)
            value_ptr = <char*> malloc(kv.second.mv_size)
            memcpy(key_ptr, kv.first.mv_data, kv.first.mv_size)
            memcpy(value_ptr, kv.second.mv_data, kv.second.mv_size)
        cdef Py_buffer* key_view = <Py_buffer*> PyMem_Malloc(sizeof(Py_buffer))
        cdef Py_buffer* value_view = <Py_buffer*> PyMem_Malloc(sizeof(Py_buffer))
        PyBuffer_FillInfo(key_view, None, <void*> key_ptr, kv.first.mv_size, 1, PyBUF_SIMPLE)
        PyBuffer_FillInfo(value_view, None, <void*> value_ptr, kv.second.mv_size, 1, PyBUF_SIMPLE)
        key_mview = PyMemoryView_FromBuffer(key_view)   # the new mview object now owns the view buffer, no need to free view
        value_mview = PyMemoryView_FromBuffer(value_view)   # the new mview object now owns the view buffer, no need to free view
        # the free(ptr) operations will be taken cared by MBufferIO
        return MBufferIO.from_mview(key_mview, True), MBufferIO.from_mview(value_mview, True)

    def __getitem__(self, item):
        item = make_utf8(item)
        cdef MDB_val val
        val.mv_size = len(item)
        val.mv_data = <void*> (<char*> item)
        cdef cppConstIterator it = move(cppConstIterator(self.ptr, val))      # keep a ref to the iterator so that the txn stays active
        if it.has_reached_end():
            raise NotFound()
        return self._getvalue(it)

    cpdef get(self, item, default=''):
        item = make_utf8(item)
        cdef MDB_val val
        val.mv_size = len(item)
        val.mv_data = <void*> (<char*> item)
        cdef cppConstIterator it = move(cppConstIterator(self.ptr, val))
        if it.has_reached_end():
            return default
        return self._getvalue(it)

    def __setitem__(self, key, value):
        key = make_utf8(key)
        value = make_utf8(value)
        cdef MDB_val k, v
        k.mv_size = len(key)
        k.mv_data = <void*> (<char*> key)
        v.mv_size = len(value)
        v.mv_data = <void*> (<char*> value)
        with nogil:
            self.ptr.insert(k, v)

    def __delitem__(self, key):
        cdef string k = make_utf8(key)
        with nogil:
            self.ptr.erase(k)

    cpdef erase(self, first, last):
        cdef string f = make_utf8(first)
        cdef string l = make_utf8(last)
        with nogil:
            self.ptr.erase(f, l)

    cpdef noiterkeys(self):
        cdef vector[string] k
        with nogil:
            k = self.ptr.get_all_keys()
        return k

    cpdef noitervalues(self):
        cdef vector[string] k
        with nogil:
            k = self.ptr.get_all_keys()
        return k

    cpdef noiteritems(self):
        cdef vector[pair[string, string]] kv
        with nogil:
            kv = self.ptr.get_all_items()
        return kv

    cpdef pop(self, key, default=None):
        key = make_utf8(key)
        cdef MDB_val k
        k.mv_size = len(key)
        k.mv_data = <void*> (<char*> key)
        cdef string val
        try:
            with nogil:
                val = self.ptr.pop(k)
        except NotFound:
            if default is None:
                raise
            return default
        return val

    cpdef popitem(self):
        cdef pair[string, string] p
        with nogil:
            p = self.ptr.popitem()
        return p

    def __iter__(self):
        return self.keys()

    def keys(self, reverse=False):
        cdef int keysize = self.ptr.get_maxkeysize()
        cdef cppConstIterator it
        cdef MDB_val k
        cdef bytearray obj = bytearray(keysize)
        cdef Py_buffer* view = <Py_buffer*> PyMem_Malloc(sizeof(Py_buffer))
        PyObject_GetBuffer(obj, view, PyBUF_SIMPLE)
        try:
            if reverse:
                it = move(self.ptr.clast())
                while not it.has_reached_beginning():
                    with nogil:
                        k = it.get_key_buffer()
                        memcpy(view.buf, k.mv_data, k.mv_size)
                    yield obj[:k.mv_size]
                    it.decr()
            else:
                it = move(self.ptr.cbegin())
                while not it.has_reached_end():
                    with nogil:
                        k = it.get_key_buffer()
                        memcpy(view.buf, k.mv_data, k.mv_size)
                    yield obj[:k.mv_size]
                    it.incr()
        finally:
            PyBuffer_Release(view)
            PyMem_Free(view)


    cpdef iterkeys(self, reverse=False):
        return self.keys(reverse)

    def values(self, reverse=False):
        cdef cppConstIterator it
        if reverse:
            it = move(self.ptr.clast())
            while not it.has_reached_beginning():
                yield self._getvalue(it)
                it.decr()
        else:
            it = move(self.ptr.cbegin())
            while not it.has_reached_end():
                yield self._getvalue(it)
                it.incr()

    cpdef itervalues(self, reverse=False):
        return self.values(reverse)

    def items(self, reverse=False):
        cdef cppConstIterator it
        if reverse:
            it = move(self.ptr.clast())
            while not it.has_reached_beginning():
                yield self._getitem(it)
                it.decr()
        else:
            it = move(self.ptr.cbegin())
            while not it.has_reached_end():
                yield self._getitem(it)
                it.incr()

    cpdef iteritems(self, reverse=False):
        return self.items(reverse)

    def __nonzero__(self):
        return not self.ptr.empty()

    def __len__(self):
        return self.ptr.size()

    cpdef clear(self):
        self.ptr.clear()

    def __contains__(self, item):
        return self.ptr.contains(make_utf8(item))

    cpdef has_key(self, key):
        return key in self

    def update(self, e=None, **kwds):
        cdef MDB_val k
        cdef MDB_val v
        cdef cppIterator it = move(cppIterator(self.ptr, 0, 0))
        if e is not None:
            if hasattr(e, 'keys'):
                for key in e.keys():
                    skey = make_utf8(key)
                    value = make_utf8(e[key])
                    k.mv_size = len(skey)
                    k.mv_data = <void*> (<char*> skey)
                    v.mv_size = len(value)
                    v.mv_data = <void*> (<char*> value)
                    with nogil:
                        it.set_key_value(k, v)
            else:
                for (key, value) in e:
                    skey = make_utf8(key)
                    value = make_utf8(value)
                    k.mv_size = len(skey)
                    k.mv_data = <void*> (<char*> skey)
                    v.mv_size = len(value)
                    v.mv_data = <void*> (<char*> value)
                    with nogil:
                        it.set_key_value(k, v)

        for key in kwds:
            skey = make_utf8(key)
            value = make_utf8(value)
            k.mv_size = len(skey)
            k.mv_data = <void*> (<char*> skey)
            v.mv_size = len(value)
            v.mv_data = <void*> (<char*> value)
            with nogil:
                it.set_key_value(k, v)


    # noinspection PyPep8Naming
    @classmethod
    def fromkeys(cls, dbname=None, dirname=None, opts=None, S=None, v=None):
        d = cls(dbname, dirname, opts=opts)
        v = b'' if v is None else make_utf8(v)
        if S is not None:
            d.update((make_utf8(key), v) for key in S)
        return d

    cpdef transform_values(self, binary_funct):
        #cdef PyObject* obj = <PyObject*> binary_funct
        with nogil:
            self.ptr.transform_values(make_binary_functor(binary_funct))

    cpdef remove_if(self, binary_pred):
        #cdef PyObject* obj = <PyObject*> binary_pred
        with nogil:
            self.ptr.remove_if(make_binary_predicate(binary_pred))

    cpdef copy_to(self, dirname, dbname="", opts=None, chunk_size=-1):
        cdef string dirn = make_utf8(dirname)
        cdef string dbn = make_utf8(dbname)
        if not dirname:
            raise ValueError()
        cdef long csize = int(chunk_size)
        if opts is None:
            opts = LmdbOptions()
        if not isinstance(opts, LmdbOptions):
            raise TypeError
        cdef lmdb_options cpp_opts = (<LmdbOptions> opts).opts
        cdef cppPersistentDict cpp_other
        with nogil:
            cpp_other = self.ptr.copy_to(dirn, dbn, cpp_opts, csize)
        py_other = PersistentStringDict()
        del (<PersistentStringDict> py_other).ptr
        (<PersistentStringDict> py_other).ptr = new cppPersistentDict(cpp_other)
        return py_other


cdef class PersistentDict(PersistentStringDict):
    def __cinit__(self, dirname=None, dbname=None, opts=None, mapping=None, **kwarg):
        pass

    def __init__(self, dirname=None, dbname=None, opts=None, mapping=None, **kwarg):
        super(PersistentDict, self).__init__(dirname, dbname, opts, mapping, **kwarg)

    def __dealloc__(self):
        pass

    def __repr__(self):
        return u"PersistentDict(dbname='{}', dirname='{}')".format(make_unicode(self.dbname), make_unicode(self.dirname))

    def __reduce__(self):
        return dict, (self.noiteritems(), )

    def __getitem__(self, key):
        cdef MDB_val k
        key = pickle.dumps(key, protocol=2)
        k.mv_size = len(key)
        k.mv_data = <void*> (<char*> key)

        cdef cppConstIterator it = move(cppConstIterator(self.ptr, k))  # keep a ref to the iterator so that the txn stays active
        if it.has_reached_end():
            raise NotFound()
        return pickle.load(self._getvalue_buf(it))

    cpdef get(self, key, default=None):
        cdef MDB_val k
        key = pickle.dumps(key, protocol=2)
        k.mv_size = len(key)
        k.mv_data = <void*> (<char*> key)
        cdef cppConstIterator it = move(cppConstIterator(self.ptr, k))  # keep a ref to the iterator so that the txn stays active
        if it.has_reached_end():
            return default
        return pickle.load(self._getvalue_buf(it))

    def __setitem__(self, key, value):
        cdef string k = pickle.dumps(key, protocol=2)
        cdef string v = pickle.dumps(value, protocol=2)
        with nogil:
            self.ptr.insert(k, v)

    def __delitem__(self, key):
        cdef string k = pickle.dumps(key, protocol=2)
        with nogil:
            self.ptr.erase(k)

    cpdef erase(self, first, last):
        raise NotImplementedError()

    cpdef noiterkeys(self):
        cdef vector[string] keylist
        with nogil:
            keylist = self.ptr.get_all_keys()
        return [pickle.loads(key) for key in keylist]

    cpdef noitervalues(self):
        cdef vector[string] valuelist
        with nogil:
            valuelist = self.ptr.get_all_values()
        return [pickle.loads(val) for val in valuelist]

    cpdef noiteritems(self):
        cdef vector[pair[string, string]] kvlist
        with nogil:
            kvlist = self.ptr.get_all_items()
        return [(pickle.loads(key), pickle.loads(val)) for (key, val) in kvlist]


    cpdef pop(self, key, default=None):
        cdef string k = pickle.dumps(key, protocol=2)
        cdef string v
        try:
            with nogil:
                v = self.ptr.pop(k)
        except NotFound:
            if default is None:
                raise
            return default
        return pickle.loads(v)

    cpdef popitem(self):
        cdef pair[string, string] p
        with nogil:
            p = self.ptr.popitem()
        return pickle.loads(p.first), pickle.loads(p.second)

    def __iter__(self):
        return self.keys()

    # MBufferIO is used from here to avoid any more copy of bytestring arguments

    def keys(self, reverse=False):
        cdef cppConstIterator it
        if reverse:
            it = move(self.ptr.clast())
            while not it.has_reached_beginning():
                yield pickle.load(self._getkey_buf(it))
                it.decr()
        else:
            it = move(self.ptr.cbegin())
            while not it.has_reached_end():
                yield pickle.load(self._getkey_buf(it))
                it.incr()

    def values(self, reverse=False):
        cdef cppConstIterator it
        if reverse:
            it = move(self.ptr.clast())
            while not it.has_reached_beginning():
                yield pickle.load(self._getvalue_buf(it))
                it.decr()
        else:
            it = move(self.ptr.cbegin())
            while not it.has_reached_end():
                yield pickle.load(self._getvalue_buf(it))
                it.incr()

    def items(self, reverse=False):
        cdef cppConstIterator it
        if reverse:
            it = move(self.ptr.clast())
            while not it.has_reached_beginning():
                key, val = self._getitem_buf(it)
                yield (pickle.load(key), pickle.load(val))
                it.decr()
        else:
            it = move(self.ptr.cbegin())
            while not it.has_reached_end():
                key, val = self._getitem_buf(it)
                yield (pickle.load(key), pickle.load(val))
                it.incr()

    def __contains__(self, item):
        return self.ptr.contains(<string> pickle.dumps(item, protocol=2))

    def update(self, e=None, **kwds):
        cdef cppIterator it = move(cppIterator(self.ptr, 0, 0))
        #cdef MDB_txn* txn = self.ptr.txn_begin(False)
        cdef string k
        cdef string v
        if e is not None:
            if hasattr(e, 'keys'):
                for key in e.keys():
                    k = pickle.dumps(key, protocol=2)
                    v = pickle.dumps(e[key], protocol=2)
                    with nogil:
                        it.set_key_value(k, v)
            else:
                for (key, value) in e:
                    k = pickle.dumps(key, protocol=2)
                    v = pickle.dumps(value, protocol=2)
                    with nogil:
                        it.set_key_value(k, v)

        for key in kwds:
            k = pickle.dumps(key, protocol=2)
            v = pickle.dumps(kwds[key], protocol=2)
            with nogil:
                it.set_key_value(k, v)


    # noinspection PyPep8Naming
    @classmethod
    def fromkeys(cls, dbname=None, dirname=None, opts=None, S=None, v=None):
        d = cls(dbname, dirname, opts=opts)
        v = pickle.dumps(b'', protocol=2) if v is None else pickle.dumps(v, protocol=2)
        if S is not None:
            d.update((pickle.dumps(key, protocol=2), v) for key in S)
        return d

    cpdef transform_values(self, binary_funct):
        binary_funct = adapt_binary_functor(binary_funct)
        #cdef PyObject* obj = <PyObject*> binary_funct
        with nogil:
            self.ptr.transform_values(make_binary_functor(binary_funct))

    cpdef remove_if(self, binary_pred):
        binary_pred = adapt_binary_predicate(binary_pred)
        #cdef PyObject* obj = <PyObject*> binary_pred
        with nogil:
            self.ptr.remove_if(make_binary_predicate(binary_pred))

    cpdef copy_to(self, dirname, dbname="", opts=None, chunk_size=-1):
        cdef string dirn = make_utf8(dirname)
        cdef string dbn = make_utf8(dbname)
        if not dirname:
            raise ValueError()
        cdef long csize = int(chunk_size)
        if opts is None:
            opts = LmdbOptions()
        if not isinstance(opts, LmdbOptions):
            raise TypeError
        cdef lmdb_options cpp_opts = (<LmdbOptions> opts).opts
        cdef cppPersistentDict other
        with nogil:
            other = self.ptr.copy_to(dirn, dbn, cpp_opts, csize)
        py_other = PersistentDict()
        del (<PersistentDict> py_other).ptr
        (<PersistentDict> py_other).ptr = new cppPersistentDict(other)
        return py_other


cdef class PersistentStringQueue(object):
    def __cinit__(self, dirname=None, dbname=None, opts=None):
        cdef string dirn = make_utf8(dirname)
        cdef string dbn = make_utf8(dbname)
        if dirn.empty():
            self.ptr = new cppPersistentQueue()
            return
        if opts is None:
            opts = LmdbOptions()
        if not isinstance(opts, LmdbOptions):
            raise TypeError
        self.ptr = new cppPersistentQueue(dirn, dbn, (<LmdbOptions> opts).opts)

    def __init__(self, dirname=None, dbname=None, opts=None):
        pass

    def __dealloc__(self):
        if self.ptr is not NULL:
            del self.ptr
            self.ptr = NULL

    def __repr__(self):
        return u"PersistentStringQueue(dbname='{}', dirname='{}')".format(
            make_unicode(self.dbname), make_unicode(self.dirname)
        )

    property dirname:
        def __get__(self):
            return self.ptr.get_dirname()

    property dbname:
        def __get__(self):
            return self.ptr.get_dbname()

    cpdef empty(self):
        return self.ptr.empty()

    cpdef qsize(self):
        return self.ptr.size()

    cpdef full(self):
        return False

    cpdef clear(self):
        self.ptr.clear()

    cpdef push_front(self, val):
        cdef string v = make_utf8(val)
        with nogil:
            self.ptr.push_front(v)

    cpdef push_back(self, val):
        cdef string v = make_utf8(val)
        with nogil:
            self.ptr.push_back(v)

    cpdef put(self, item, block=True, timeout=None):
        self.push_back(item)

    cpdef put_nowait(self, item):
        self.push_back(item)

    cpdef pop_back(self):
        cdef string v
        with nogil:
            v = self.ptr.pop_back()
        return v

    cpdef pop_front(self):
        cdef string v
        with nogil:
            v = self.ptr.pop_front()
        return v

    cpdef pop_all(self):
        cdef vector[string] v
        with nogil:
            v = self.ptr.pop_all()
        return v

    cpdef get(self, block=True, timeout=None):
        cdef double delay
        cdef double t_out
        cdef double endtime
        if not block:
            return self.get_nowait()
        elif timeout is None:
            # wait until there is an available element
            delay = 0.0005
            while True:
                try:
                    return self.get_nowait()
                except Empty:
                    delay = min(delay * 2.0, 0.1)
                    sleep(delay)

        elif timeout < 0:
            raise ValueError("'timeout' must be a non-negative number")
        else:
            t_out = timeout
            # wait at maximum 'timeout' seconds
            endtime = c_time(NULL) + t_out
            delay = 0.0005
            while True:
                try:
                    return self.get_nowait()
                except Empty:
                    remaining = endtime - c_time(NULL)
                    if remaining <= 0.0:
                        raise
                    delay = min(delay * 2.0, remaining, 0.1)
                    sleep(delay)

    cpdef get_nowait(self):
        try:
            return self.pop_front()
        except EmptyDatabase:
            raise Empty()

    def push_front_many(self, vals):
        vals = iter(vals)
        #cdef PyObject* v = <PyObject*> vals
        with nogil:
            self.ptr.push_front(PyStringInputIterator(vals), PyStringInputIterator())

    def push_back_many(self, vals):
        vals = iter(vals)
        #cdef PyObject* v = <PyObject*> vals
        with nogil:
            self.ptr.push_back(PyStringInputIterator(vals), PyStringInputIterator())

    def push_many(self, vals):
        self.push_back_many(vals)

    cpdef transform_values(self, unary_funct):
        #cdef PyObject* obj = <PyObject*> unary_funct
        with nogil:
            self.ptr.transform_values(make_unary_functor(unary_funct))

    cpdef remove_if(self, unary_pred):
        #cdef PyObject* obj = <PyObject*> unary_pred
        with nogil:
            self.ptr.remove_if(make_unary_predicate(unary_pred))

    cpdef move_to(self, dirname, dbname="", opts=None, chunk_size=-1):
        cdef string dirn = make_utf8(dirname)
        cdef string dbn = make_utf8(dbname)
        if not dirname:
            raise ValueError()
        cdef long csize = int(chunk_size)
        if opts is None:
            opts = LmdbOptions()
        if not isinstance(opts, LmdbOptions):
            raise TypeError
        cdef lmdb_options cpp_opts = (<LmdbOptions> opts).opts
        cdef cppPersistentQueue cpp_other
        with nogil:
            cpp_other = self.ptr.move_to(dirn, dbn, cpp_opts, csize)
        py_other = PersistentStringQueue()
        del (<PersistentStringQueue> py_other).ptr
        (<PersistentStringQueue> py_other).ptr = new cppPersistentQueue(cpp_other)
        return py_other


cdef class PersistentQueue(PersistentStringQueue):
    def __init__(self, dirname=None, dbname=None, opts=None):
        super(PersistentQueue, self).__init__(dirname, dbname, opts)

    def __repr__(self):
        return u"PersistentQueue(dbname='{}', dirname='{}')".format(
            make_unicode(self.dbname), make_unicode(self.dirname)
        )

    cpdef push_front(self, val):
        cdef string v = pickle.dumps(val, protocol=2)
        with nogil:
            self.ptr.push_front(v)

    cpdef push_back(self, val):
        cdef string v = pickle.dumps(val, protocol=2)
        with nogil:
            self.ptr.push_back(v)

    cpdef pop_back(self):
        cdef string v
        with nogil:
            v = self.ptr.pop_back()
        return pickle.loads(v)

    cpdef pop_front(self):
        cdef string v
        with nogil:
            v = self.ptr.pop_front()
        return pickle.loads(v)

    cpdef pop_all(self):
        cdef vector[string] vect
        with nogil:
            vect = self.ptr.pop_all()
        return [pickle.loads(v) for v in vect]

    def push_front_many(self, vals):
        values_iter = (pickle.dumps(val, protocol=2) for val in vals)
        super(PersistentQueue, self).push_front_many(values_iter)

    def push_back_many(self, vals):
        values_iter = (pickle.dumps(val, protocol=2) for val in vals)
        super(PersistentQueue, self).push_back_many(values_iter)

    cpdef transform_values(self, unary_funct):
        unary_funct = adapt_unary_functor(unary_funct)
        #cdef PyObject* obj = <PyObject*> unary_funct
        with nogil:
            self.ptr.transform_values(make_unary_functor(unary_funct))

    cpdef remove_if(self, unary_pred):
        unary_pred = adapt_unary_predicate(unary_pred)
        #cdef PyObject* obj = <PyObject*> unary_pred
        with nogil:
            self.ptr.remove_if(make_unary_predicate(unary_pred))

    cpdef move_to(self, dirname, dbname="", opts=None, chunk_size=-1):
        cdef string dirn = make_utf8(dirname)
        cdef string dbn = make_utf8(dbname)
        if not dirname:
            raise ValueError()
        cdef long csize = int(chunk_size)
        if opts is None:
            opts = LmdbOptions()
        if not isinstance(opts, LmdbOptions):
            raise TypeError
        cdef lmdb_options cpp_opts = (<LmdbOptions> opts).opts
        cdef cppPersistentQueue cpp_other
        with nogil:
            cpp_other = self.ptr.move_to(dirn, dbn, cpp_opts, csize)
        py_other = PersistentQueue()
        del (<PersistentQueue> py_other).ptr
        (<PersistentQueue> py_other).ptr = new cppPersistentQueue(cpp_other)
        return py_other


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


def adapt_unary_functor(unary_funct):
    def functor(x):
        return pickle.dumps(unary_funct(pickle.loads(x)), protocol=2)
    return functor

def adapt_binary_functor(binary_funct):
    def functor(x, y):
        return pickle.dumps(binary_funct(pickle.loads(x), pickle.loads(y)), protocol=2)
    return functor

def adapt_unary_predicate(unary_pred):
    def predicate(x):
        return bool(unary_pred(pickle.loads(x)))
    return predicate

def adapt_binary_predicate(binary_pred):
    def predicate(x, y):
        return bool(binary_pred(pickle.loads(x), pickle.loads(y)))
    return predicate


cpdef add_console_logging():
    add_console()

cpdef add_python_logging(name):
    set_logger(<bytes> name)

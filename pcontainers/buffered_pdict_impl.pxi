# -*- coding: utf-8 -*-

# noinspection PyPep8Naming
cdef class BufferedPDictWrapper(object):
    def __cinit__(self, PRawDict d, uint64_t ms_interval):
        if d is None:
            raise ValueError()
        self.the_dict = d
        self.ptr = shared_ptr[cppBufferedPersistentDict](new cppBufferedPersistentDict(d.ptr, ms_interval))

    def __dealloc__(self):
        with nogil:
            self.ptr.reset()

    def __init__(self, PRawDict d, uint64_t ms_interval):
        pass

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    cpdef start(self):
        with nogil:
            self.ptr.get().start()

    cpdef stop(self):
        with nogil:
            self.ptr.get().stop()

    cpdef flush(self):
        with nogil:
            self.ptr.get().flush()

    cpdef getitem(self, key):
        encoded_key = self.the_dict.key_chain.dumps(key)
        cdef CBString result = self.ptr.get().at(
            PyBufferWrap(encoded_key).get_mdb_val()
        )
        return self.the_dict.value_chain.loads(make_mbufferio_from_cbstring(result))

    cpdef async_setitem(self, key, value):
        cdef BoolFutureWrapper py_future = BoolFutureWrapper()
        encoded_key = self.the_dict.key_chain.dumps(key)
        encoded_value = self.the_dict.key_chain.dumps(value)
        cdef MDB_val k = PyBufferWrap(encoded_key).get_mdb_val()
        cdef MDB_val v = PyBufferWrap(encoded_value).get_mdb_val()
        cdef shared_future[cpp_bool] cpp_future
        with nogil:
            cpp_future = self.ptr.get().insert_key_value(k, v)
        py_future.set_boost_future(cpp_future)
        return py_future

    cpdef async_delitem(self, key):
        cdef BoolFutureWrapper py_future = BoolFutureWrapper()
        encoded_key = self.the_dict.key_chain.dumps(key)
        cdef MDB_val k = PyBufferWrap(encoded_key).get_mdb_val()
        cdef shared_future[cpp_bool] cpp_future
        with nogil:
            cpp_future = self.ptr.get().erase(k)
        py_future.set_boost_future(cpp_future)
        return py_future

    cpdef async_getitem(self, key):
        cdef CBStringFutureWrapper py_future = CBStringFutureWrapper(self.the_dict.value_chain.pyloads)
        encoded_key = self.the_dict.key_chain.dumps(key)
        cdef MDB_val k = PyBufferWrap(encoded_key).get_mdb_val()
        cdef shared_future[CBString] cpp_future
        with nogil:
            cpp_future = self.ptr.get().async_at(k)
        py_future.set_boost_future(cpp_future)
        return py_future


# -*- coding: utf-8 -*-

# noinspection PyPep8Naming
cdef class BufferedPDictWrapper(object):
    def __cinit__(self, PRawDict d, interval):
        if d is None:
            raise ValueError()
        if not isinstance(interval, Number):
            raise TypeError()
        if interval <= 0:
            raise ValueError()
        cdef uint64_t ms_interval = int(interval * 1000)

        self.the_dict = d
        self.ptr = shared_ptr[cppBufferedPersistentDict](new cppBufferedPersistentDict(d.ptr, ms_interval))

    def __dealloc__(self):
        with nogil:
            self.ptr.reset()

    def __init__(self, PRawDict d, interval):
        pass

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


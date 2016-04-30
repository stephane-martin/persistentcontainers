# -*- coding: utf-8 -*-

# noinspection PyPep8Naming
cdef class BufferedPQueueWrapper(object):
    def __cinit__(self, PRawQueue q, uint64_t ms_interval):
        if q is None:
            raise ValueError()
        self.the_queue = q
        self.ptr = shared_ptr[cppBufferedPersistentQueue](new cppBufferedPersistentQueue(q.ptr, ms_interval))

    def __dealloc__(self):
        with nogil:
            self.ptr.reset()

    def __init__(self, PRawQueue q, uint64_t ms_interval):
        pass

    cpdef push_back(self, value):
        cdef BoolFutureWrapper py_future = BoolFutureWrapper()
        encoded_value = self.the_queue.value_chain.dumps(value)
        cdef MDB_val v = PyBufferWrap(encoded_value).get_mdb_val()
        cdef shared_future[cpp_bool] cpp_future
        with nogil:
            cpp_future = self.ptr.get().push_back(v)
        py_future.set_boost_future(cpp_future)
        return py_future

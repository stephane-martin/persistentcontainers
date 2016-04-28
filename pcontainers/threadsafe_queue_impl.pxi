# -*- coding: utf-8 -*-

cdef class ThreadsafeQueue(object):
    def __cinit__(self):
        self.cpp_queue_ptr.reset(new py_threadsafe_queue())

    def __dealloc__(self):
        self.cpp_queue_ptr.reset()

    def __init__(self):
        pass

    cpdef try_pop(self):
        cdef PyObject* ptr = self.cpp_queue_ptr.get().py_try_pop()
        if ptr:
            obj = <object> ptr  # calls INCREF
            Py_DECREF(obj)
            return obj
        return None

    cpdef wait_and_pop(self, timeout=None):
        cdef PyObject* ptr
        timeout = max(0, timeout) if timeout else 0
        if not isinstance(timeout, Number):
            raise TypeError()
        timeout = int(timeout * 1000)
        cdef long long ms = timeout
        if timeout <= 0:
            ptr = self.cpp_queue_ptr.get().py_wait_and_pop()
        else:
            ptr = self.cpp_queue_ptr.get().py_wait_and_pop(<long long> ms)
        if ptr:
            obj = <object> ptr
            Py_DECREF(obj)
            return obj
        return None

    cpdef push(self, obj):
        if obj is not None:
            self.cpp_queue_ptr.get().py_push(obj)

    cpdef empty(self):
        return self.cpp_queue_ptr.get().empty()


# -*- coding: utf-8 -*-

cdef class ThreadsafeQueue(object):
    cdef scoped_ptr[py_threadsafe_queue] cpp_queue_ptr
    cpdef try_pop(self)
    cpdef wait_and_pop(self, timeout=?)
    cpdef push(self, obj)
    cpdef empty(self)


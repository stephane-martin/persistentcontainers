# -*- coding: utf-8 -*-

cdef class BufferedPQueueWrapper(object):
    cdef shared_ptr[cppBufferedPersistentQueue] ptr
    cdef PRawQueue the_queue

    cpdef push_back(self, value)

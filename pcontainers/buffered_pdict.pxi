# -*- coding: utf-8 -*-

cdef class BufferedPDictWrapper(object):
    cdef shared_ptr[cppBufferedPersistentDict] ptr
    cdef PRawDict the_dict

    cpdef getitem(self, key)
    cpdef async_setitem(self, key, value)
    cpdef async_delitem(self, key)
    cpdef async_getitem(self, key)


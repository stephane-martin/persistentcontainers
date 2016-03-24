
cdef class DirectAccess(object):
    cdef cppConstIterator cpp_iterator
    cdef object buf
    cpdef read(self, ssize_t n=?)

cdef class PRawDict(object):
    cdef cppPersistentDict* ptr
    cdef bint rmrf_at_delete

    cpdef noiterkeys(self)
    cpdef noitervalues(self)
    cpdef noiteritems(self)
    cpdef erase(self, first, last)
    cpdef get(self, key, default=?)
    cpdef get_direct(self, item)
    cpdef setdefault(self, key, default=?)
    cpdef pop(self, key, default=?)
    cpdef popitem(self)
    cpdef clear(self)
    cpdef has_key(self, key)
    cpdef transform_values(self, binary_funct)
    cpdef remove_if(self, binary_pred)
    cpdef iterkeys(self, reverse=?)
    cpdef itervalues(self, reverse=?)
    cpdef iteritems(self, reverse=?)
    cpdef copy_to(self, other, ssize_t chunk_size=?)
    cpdef move_to(self, other, ssize_t chunk_size=?)
    cpdef remove_duplicates(self, first=?, last=?)

    cdef raw_get_key(self, cppConstIterator& it)
    cdef raw_get_key_buf(self, cppIterator& it)
    cdef raw_get_key_buf_const(self, cppConstIterator& it)
    cdef raw_get_value(self, cppConstIterator& it)
    cdef raw_get_value_buf(self, cppIterator& it)
    cdef raw_get_value_buf_const(self, cppConstIterator& it)
    cdef raw_get_item(self, cppConstIterator& it)
    cdef raw_get_item_buf(self, cppIterator& it)
    cdef raw_get_item_buf_const(self, cppConstIterator& it)
    cdef raw_set_item_buf(self, cppIterator& it, k, v)


cdef class PDict(PRawDict):
    cdef Chain key_chain
    cdef Chain value_chain


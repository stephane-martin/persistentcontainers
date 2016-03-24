cdef class PRawQueue(object):
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
    cpdef move_to(self, other, ssize_t chunk_size=?)
    cpdef remove_duplicates(self)


cdef class PQueue(PRawQueue):
    cdef bytes secret_key
    cdef Chain value_chain

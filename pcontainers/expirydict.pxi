

cdef class expiry_dict(object):
    cdef time_t default_expiry
    cdef time_t prune_period
    cdef PDict values_dict
    cdef PDict index_dict
    cpdef get(self, key, default=?)
    cpdef set(self, key, value, time_t expiry=?)
    cpdef pop(self, key, default=?)
    cpdef popitem(self)
    cpdef prune_expired(self)
    cdef object stopping
    cdef object pruning_thread

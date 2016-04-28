# -*- coding: utf-8 -*-

cdef class ExpiryDict(object):
    cdef time_t default_expiry
    cdef time_t prune_period
    cdef PDict values_dict
    cdef PDict index_dict
    cdef PDict metadata_dict
    cpdef get_metadata(self, key)
    cpdef set(self, key, value, time_t expiry=?, metadata=?)
    cpdef set_metadata(self, key, metadata)
    cpdef pop(self, key)
    cpdef popitem(self)
    cpdef prune_expired(self)

    cdef object stopping
    cdef object pruning_thread

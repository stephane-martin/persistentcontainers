# -*- coding: utf-8 -*-

cdef class PRawDictAbstractIterator(object):
    cdef PRawDict dict
    cdef int pos
    cdef object key
    cdef scoped_ptr[abstract_iterator] cpp_iterator_ptr
    cdef cpp_bool has_reached_end(self)
    cdef cpp_bool has_reached_beginning(self)
    cdef get_key_buf(self, cpp_bool incr=?, cpp_bool decr=?)
    cdef get_value_buf(self, cpp_bool incr=?, cpp_bool decr=?)
    cdef get_item_buf(self, cpp_bool incr=?, cpp_bool decr=?)
    cdef incr(self)
    cdef decr(self)


cdef class PRawDictConstIterator(PRawDictAbstractIterator):
    pass

cdef class PRawDictIterator(PRawDictAbstractIterator):
    cdef set_rollback(self)
    cdef incr(self)
    cdef decr(self)
    cdef set_item_buf(self, k, v)
    cdef dlte(self, key=?)

cdef class DirectAccess(object):
    cdef cppConstIterator cpp_iterator
    cdef object buf
    cpdef read(self, ssize_t n=?)

cdef class PRawDict(object):
    cdef shared_ptr[cppPersistentDict] ptr
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
    cpdef move_to(self, PRawDict other, ssize_t chunk_size=?)
    cpdef remove_duplicates(self, first=?, last=?)

    cdef readonly Chain key_chain
    cdef readonly Chain value_chain

cdef class PDict(PRawDict):
    pass

cdef class NonBlockingPDictWrapper(object):
    cdef PRawDict d
    cdef int n_readers
    cdef ThreadsafeQueue read_operations_queue
    cdef ThreadsafeQueue write_operations_queue
    cdef object starting
    cdef object stopping
    cdef object readers_threads
    cdef object writer_thread

    cdef _do_operation(self, op)
    cpdef start(self)
    cpdef stop(self)

    cpdef setitem(self, key, value)
    cpdef getitem(self, item)
    cpdef get(self, item, default=?)
    cpdef keys(self)
    cpdef values(self)
    cpdef items(self)
    cpdef pop(self, key, default=?)
    cpdef popitem(self)
    cpdef transform_values(self, binary_funct)
    cpdef remove_if(self, binary_pred)
    cpdef remove_duplicates(self)

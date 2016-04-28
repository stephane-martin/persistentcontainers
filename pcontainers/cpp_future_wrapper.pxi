# -*- coding: utf-8 -*-

cdef class BaseFutureWrapper(object):
    cdef atomic_int _state
    cdef object _result
    cdef object _exception
    cdef object _done_callbacks
    cdef object loads
    cpdef _invoke_callbacks(self)


cdef class CBStringFutureWrapper(BaseFutureWrapper):
    cdef shared_future[CBString] _boost_future_after_then
    cdef shared_future[CBString] _boost_future
    cdef set_boost_future(self, shared_future[CBString] f)
    cpdef _wait(self, timeout)

cdef class BoolFutureWrapper(BaseFutureWrapper):
    cdef shared_future[cpp_bool] _boost_future_after_then
    cdef shared_future[cpp_bool] _boost_future
    cdef set_boost_future(self, shared_future[cpp_bool] f)
    cpdef _wait(self, timeout)


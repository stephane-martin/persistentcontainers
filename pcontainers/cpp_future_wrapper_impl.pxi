# -*- coding: utf-8 -*-

_LOGGER = logging.getLogger("concurrent.futures")

cdef int _pending = 0
cdef int _running = 1
cdef int _cancelled = 2
cdef int _cancelled_and_notified = 3
cdef int _finished = 4


cdef class BaseFutureWrapper(object):
    def __cinit__(self):
        self._state.store(_pending)

    def __init__(self):
        self._done_callbacks = collections.deque()
        self._result = None
        self._exception = None

    cpdef _invoke_callbacks(self):
        while True:
            try:
                callback = self._done_callbacks.popleft()
            except IndexError:
                break
            try:
                callback(self)
            except Exception:
                _LOGGER.exception('exception calling callback')

    def cancel(self):
        cdef int st = self._state.load()
        if st in [_running, _finished]:
            return False

        if st in [_cancelled, _cancelled_and_notified]:
            return True

        self._state.store(_cancelled)
        self._invoke_callbacks()
        return True

    def cancelled(self):
        return self._state.load() in [_cancelled, _cancelled_and_notified]

    def running(self):
        return self._state.load() == _running

    def done(self):
        return self._state.load() in [_cancelled, _cancelled_and_notified, _finished]

    def add_done_callback(self, fn):
        if self._state.load() not in [_cancelled, _cancelled_and_notified, _finished]:
            self._done_callbacks.append(fn)
            return
        fn(self)

    def result(self, timeout=None):
        timeout = max(0, timeout) if timeout else 0
        if not isinstance(timeout, Number):
            raise TypeError()

        cdef future_status f_status
        cdef int st
        st = self._state.load()
        if st in [_cancelled, _cancelled_and_notified]:
            raise CancelledError()
        elif st == _finished:
            if self._exception:
                raise self._exception
            else:
                return self._result
        self._wait(timeout)
        st = self._state.load()
        if st in [_cancelled, _cancelled_and_notified]:
            raise CancelledError()
        if st == _finished:
            if self._exception:
                raise self._exception
            else:
                return self._result
        raise TimeoutError()

    def exception(self, timeout=None):
        timeout = max(0, timeout) if timeout else 0
        if not isinstance(timeout, Number):
            raise TypeError()

        cdef future_status f_status
        cdef int st
        st = self._state.load()
        if st in [_cancelled, _cancelled_and_notified]:
            raise CancelledError()
        elif st == _finished:
            return self._exception
        self._wait(timeout)
        st = self._state.load()
        if st in [_cancelled, _cancelled_and_notified]:
            raise CancelledError()
        if st == _finished:
            return self._exception
        raise TimeoutError()

cdef class CBStringFutureWrapper(BaseFutureWrapper):
    def __init__(self, loads):
        super(CBStringFutureWrapper, self).__init__()
        self.loads = loads

    cdef set_boost_future(self, shared_future[CBString] f):
        self._boost_future = f
        self._state.store(_running)
        callback = self._future_is_ready_callback
        with nogil:
            self._boost_future_after_then = then_(f, callback)

    def _future_is_ready_callback(self):
        # print("future is ready")
        cdef CBString result
        # _future_is_ready_callback will be called from a foreign C++ thread...
        try:
            result = self._boost_future.get()
        except Exception as ex:
            self._exception = ex
        else:
            try:
                self._result = self.loads(make_mbufferio_from_cbstring(result))
            except Exception as ex:
                self._exception = ex
        # ****
        self._state.store(_finished)
        # avoid circular references: here self._boost_future_after_then contains a reference to self
        self._boost_future = shared_future[CBString]()
        self._boost_future_after_then = shared_future[CBString]()
        self._invoke_callbacks()

    cpdef _wait(self, timeout):
        cdef shared_future[CBString] fut = self._boost_future_after_then
        if self._state.load() in (_finished, _cancelled, _cancelled_and_notified):
            return
        # if we get here, it means that 'fut' was copied before line **** in _future_is_ready_callback
        # so "self._boost_future_after_then = shared_future[CBString]()" was not yet executed
        # so "fut" contains a valid shared_future, and can be waited later
        # hence we avoid a race condition
        timeout = max(0, timeout) if timeout else 0
        if not isinstance(timeout, Number):
            raise TypeError()
        timeout = int(timeout * 1000)
        cdef int st
        cdef milliseconds ms
        if timeout > 0:
            while timeout > 0:
                ms = milliseconds(<uint64_t> timeout)
                start = monotonic()
                with nogil:
                    fut.wait_for(ms)
                if self._state.load() in [_cancelled, _cancelled_and_notified, _finished]:
                    return
                timeout -= int(monotonic() - start)
        else:
            with nogil:
                fut.wait()

cdef class BoolFutureWrapper(BaseFutureWrapper):
    def __init__(self):
        super(BoolFutureWrapper, self).__init__()

    cdef set_boost_future(self, shared_future[cpp_bool] f):
        # self._boost_future = f
        self._state.store(_running)
        callback = self._future_is_ready_callback
        with nogil:
            self._boost_future_after_then = then_(f, callback)

    def _future_is_ready_callback(self):
        # ready_callback will be called from a foreign C++ thread...
        try:
            self._result = bool(self._boost_future.get())
        except Exception as ex:
            self._exception = ex
        self._state.store(_finished)
        # avoid circular references
        self._boost_future = shared_future[cpp_bool]()
        self._boost_future_after_then = shared_future[cpp_bool]()
        self._invoke_callbacks()

    cpdef _wait(self, timeout):
        cdef shared_future[cpp_bool] fut = self._boost_future_after_then
        if self._state.load() in (_finished, _cancelled, _cancelled_and_notified):
            return

        timeout = max(0, timeout) if timeout else 0
        if not isinstance(timeout, Number):
            raise TypeError()
        timeout = int(timeout * 1000)
        cdef int st
        cdef milliseconds ms
        if timeout > 0:
            while timeout > 0:
                ms = milliseconds(<uint64_t> timeout)
                start = monotonic()
                with nogil:
                    self._boost_future_after_then.wait_for(ms)
                if self._state.load() in [_cancelled, _cancelled_and_notified, _finished]:
                    return
                timeout -= int(monotonic() - start)
        else:
            with nogil:
                self._boost_future_after_then.wait()

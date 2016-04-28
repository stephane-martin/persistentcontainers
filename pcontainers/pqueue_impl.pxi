# -*- coding: utf-8 -*-

cdef class PRawQueue(object):
    def __cinit__(self, dirname, dbname, LmdbOptions opts=None, Chain value_chain=None):
        cdef CBString dirn = tocbstring(dirname)
        cdef CBString dbn = tocbstring(dbname)
        if dirn.length() == 0:
            raise ValueError("empty dirname")
        if opts is None:
            opts = LmdbOptions()
        self.ptr = queue_factory(dirn, dbn, (<LmdbOptions> opts).opts)
        self.value_chain = NoneChain()

    def __init__(self, dirname, dbname, LmdbOptions opts=None, Chain value_chain=None):
        pass

    def __dealloc__(self):
        with nogil:
            self.ptr.reset()

    def __repr__(self):
        return u"PRawQueue(dbname='{}', dirname='{}')".format(
            make_unicode(self.dbname), make_unicode(self.dirname)
        )

    property dirname:
        def __get__(self):
            return topy(self.ptr.get().get_dirname())

    property dbname:
        def __get__(self):
            return topy(self.ptr.get().get_dbname())

    cpdef empty(self):
        return self.ptr.get().empty()

    cpdef qsize(self):
        return self.ptr.get().size()

    cpdef full(self):
        return False

    cpdef clear(self):
        self.ptr.get().clear()

    cpdef put(self, item, block=True, timeout=None):
        self.push_back(item)

    cpdef put_nowait(self, item):
        self.push_back(item)

    cpdef push_front(self, val):
        cdef PyBufferWrap view = move(PyBufferWrap(self.value_chain.dumps(val)))
        with nogil:
            self.ptr.get().push_front(view.get_mdb_val())

    cpdef push_back(self, val):
        cdef PyBufferWrap view = move(PyBufferWrap(self.value_chain.dumps(val)))
        with nogil:
            self.ptr.get().push_back(view.get_mdb_val())

    cpdef async_push_back(self, val):
        cdef PyBufferWrap view = move(PyBufferWrap(self.value_chain.dumps(val)))
        cdef BoolFutureWrapper py_future = BoolFutureWrapper()
        cdef shared_future[cpp_bool] cpp_fut
        with nogil:
            cpp_fut = self.ptr.get().async_push_back(view.get_mdb_val())
        py_future.set_boost_future(cpp_fut)
        return py_future

    cpdef pop_back(self):
        cdef CBString v
        with nogil:
            v = self.ptr.get().pop_back()
        return self.value_chain.loads(make_mbufferio_from_cbstring(v))

    cpdef pop_front(self):
        cdef CBString v
        with nogil:
            v = self.ptr.get().pop_front()
        return self.value_chain.loads(make_mbufferio_from_cbstring(v))

    cpdef async_pop_front(self, timeout=None):
        timeout = max(0, timeout) if timeout else 0
        if not isinstance(timeout, Number):
            raise TypeError()
        timeout = int(timeout * 1000)
        if timeout < 0:
            raise ValueError()

        cdef CBStringFutureWrapper py_future = CBStringFutureWrapper(self.value_chain.pyloads)
        cdef shared_future[CBString] cpp_future
        cdef milliseconds ms

        if not timeout:
            with nogil:
                cpp_future = self.ptr.get().async_wait_and_pop_front()
        else:
            ms = milliseconds(<long> timeout)
            with nogil:
                cpp_future = self.ptr.get().async_wait_and_pop_front(ms)

        py_future.set_boost_future(cpp_future)
        return py_future

    cpdef wait_and_pop_front(self, timeout=None):
        timeout = max(0, timeout) if timeout else 0
        if not isinstance(timeout, Number):
            raise TypeError()
        timeout = int(timeout * 1000)
        if timeout < 0:
            raise ValueError()

        cdef CBString v

        cdef milliseconds ms = milliseconds(<long> timeout)
        try:
            with nogil:
                v = self.ptr.get().wait_and_pop_front(ms)
        except EmptyDatabase:
            raise Empty()
        return self.value_chain.loads(make_mbufferio_from_cbstring(v))

    cpdef pop_all(self):
        l = []
        append = l.append
        with nogil:
            self.ptr.get().pop_all(PyFunctionOutputIterator(append))
        return l

    cpdef get(self, block=True, timeout=None):
        if not block:
            return self.get_nowait()
        elif timeout is None:
            return self.wait_and_pop_front()
        elif timeout < 0:
            raise ValueError("'timeout' must be a non-negative number")
        else:
            return self.wait_and_pop_front(timeout)

    cpdef get_nowait(self):
        try:
            return self.pop_front()
        except EmptyDatabase:
            raise Empty()

    def push_front_many(self, vals):
        vals = iter(vals)
        cdef PyStringInputIterator _begin = move(PyStringInputIterator(vals))
        cdef PyStringInputIterator _end
        with nogil:
            self.ptr.get().push_front(_begin, _end)

    def push_back_many(self, vals):
        vals = iter(vals)
        cdef PyStringInputIterator _begin = move(PyStringInputIterator(vals))
        cdef PyStringInputIterator _end
        with nogil:
            self.ptr.get().push_back(_begin, _end)

    def push_many(self, vals):
        self.push_back_many(vals)

    cpdef transform_values(self, unary_funct):
        #cdef PyObject* obj = <PyObject*> unary_funct
        with nogil:
            self.ptr.get().transform_values(make_unary_functor(unary_funct))

    cpdef remove_if(self, unary_pred):
        #cdef PyObject* obj = <PyObject*> unary_pred
        with nogil:
            self.ptr.get().remove_if(make_unary_predicate(unary_pred))

    cpdef move_to(self, other, ssize_t chunk_size=-1):
        if not isinstance(other, PRawQueue):
            raise TypeError()
        with nogil:
            self.ptr.get().move_to((<PRawQueue> other).ptr, chunk_size)

    cpdef remove_duplicates(self):
        with nogil:
            self.ptr.get().remove_duplicates()


cdef class PQueue(PRawQueue):
    def __cinit__(self, dirname, dbname, LmdbOptions opts=None, Chain value_chain=None):
        self.value_chain = value_chain if value_chain else Chain(PickleSerializer(), None, None)

    def __init__(self, dirname, dbname, LmdbOptions opts=None, Chain value_chain=None):
        super(PQueue, self).__init__(dirname, dbname, opts)

    def __repr__(self):
        return u"PQueue(dbname='{}', dirname='{}')".format(
            make_unicode(self.dbname), make_unicode(self.dirname)
        )

    cpdef pop_all(self):
        l = []
        append = _adapt_append_pqueue_pop_all(self, l)
        with nogil:
            self.ptr.get().pop_all(PyFunctionOutputIterator(append))
        return l

    def push_front_many(self, vals):
        values_iter = (self.value_chain.dumps(val).tobytes() for val in vals)
        super(PQueue, self).push_front_many(values_iter)

    def push_back_many(self, vals):
        values_iter = (self.value_chain.dumps(val).tobytes() for val in vals)
        super(PQueue, self).push_back_many(values_iter)

    cpdef transform_values(self, unary_funct):
        unary_funct = _adapt_unary_functor(unary_funct, self.secret_key)
        #cdef PyObject* obj = <PyObject*> unary_funct
        with nogil:
            self.ptr.get().transform_values(make_unary_functor(unary_funct))

    cpdef remove_if(self, unary_pred):
        unary_pred = _adapt_unary_predicate(unary_pred, self.secret_key)
        #cdef PyObject* obj = <PyObject*> unary_pred
        with nogil:
            self.ptr.get().remove_if(make_unary_predicate(unary_pred))

    cpdef move_to(self, other, ssize_t chunk_size=-1):
        if not isinstance(other, PQueue):
            raise TypeError()
        with nogil:
            self.ptr.get().move_to((<PQueue> other).ptr, chunk_size)


def _adapt_append_pqueue_pop_all(PQueue q, list l):
    def append(v):
        return l.append(q.value_chain.loads(v))
    return append


cdef class PRawQueue(object):
    def __cinit__(self, dirname, dbname, LmdbOptions opts=None, Chain value_chain=None):
        cdef CBString dirn = tocbstring(make_utf8(dirname))
        cdef CBString dbn = tocbstring(make_utf8(dbname))
        if dirn.length() == 0:
            raise ValueError("empty dirname")
        if opts is None:
            opts = LmdbOptions()
        self.ptr = new cppPersistentQueue(dirn, dbn, (<LmdbOptions> opts).opts)

    def __init__(self, dirname, dbname, LmdbOptions opts=None, Chain value_chain=None):
        pass

    def __dealloc__(self):
        if self.ptr is not NULL:
            del self.ptr
            self.ptr = NULL

    def __repr__(self):
        return u"PRawQueue(dbname='{}', dirname='{}')".format(
            make_unicode(self.dbname), make_unicode(self.dirname)
        )

    property dirname:
        def __get__(self):
            return topy(self.ptr.get_dirname())

    property dbname:
        def __get__(self):
            return topy(self.ptr.get_dbname())

    cpdef empty(self):
        return self.ptr.empty()

    cpdef qsize(self):
        return self.ptr.size()

    cpdef full(self):
        return False

    cpdef clear(self):
        self.ptr.clear()

    cpdef push_front(self, val):
        cdef PyBufferWrap view = move(PyBufferWrap(make_utf8(val)))
        with nogil:
            self.ptr.push_front(view.get_mdb_val())

    cpdef push_back(self, val):
        cdef PyBufferWrap view = move(PyBufferWrap(make_utf8(val)))
        with nogil:
            self.ptr.push_back(view.get_mdb_val())

    cpdef put(self, item, block=True, timeout=None):
        self.push_back(item)

    cpdef put_nowait(self, item):
        self.push_back(item)

    cpdef pop_back(self):
        cdef CBString v
        with nogil:
            v = self.ptr.pop_back()
        return topy(v)

    cpdef pop_front(self):
        cdef CBString v
        with nogil:
            v = self.ptr.pop_front()
        return topy(v)

    cpdef pop_all(self):
        cdef vector[CBString] vec
        with nogil:
            vec = self.ptr.pop_all()
        return [topy(val) for val in vec]

    cpdef get(self, block=True, timeout=None):
        cdef double delay
        cdef double t_out
        cdef double endtime
        if not block:
            return self.get_nowait()
        elif timeout is None:
            # wait until there is an available element
            delay = 0.0005
            while True:
                try:
                    return self.get_nowait()
                except Empty:
                    delay = min(delay * 2.0, 0.1)
                    sleep(delay)

        elif timeout < 0:
            raise ValueError("'timeout' must be a non-negative number")
        else:
            t_out = timeout
            # wait at maximum 'timeout' seconds
            endtime = c_time(NULL) + t_out
            delay = 0.0005
            while True:
                try:
                    return self.get_nowait()
                except Empty:
                    remaining = endtime - c_time(NULL)
                    if remaining <= 0.0:
                        raise
                    delay = min(delay * 2.0, remaining, 0.1)
                    sleep(delay)

    cpdef get_nowait(self):
        try:
            return self.pop_front()
        except EmptyDatabase:
            raise Empty()

    def push_front_many(self, vals):
        vals = iter(vals)
        #cdef PyObject* v = <PyObject*> vals
        with nogil:
            self.ptr.push_front(PyStringInputIterator(vals), PyStringInputIterator())

    def push_back_many(self, vals):
        vals = iter(vals)
        #cdef PyObject* v = <PyObject*> vals
        with nogil:
            self.ptr.push_back(PyStringInputIterator(vals), PyStringInputIterator())

    def push_many(self, vals):
        self.push_back_many(vals)

    cpdef transform_values(self, unary_funct):
        #cdef PyObject* obj = <PyObject*> unary_funct
        with nogil:
            self.ptr.transform_values(make_unary_functor(unary_funct))

    cpdef remove_if(self, unary_pred):
        #cdef PyObject* obj = <PyObject*> unary_pred
        with nogil:
            self.ptr.remove_if(make_unary_predicate(unary_pred))

    cpdef move_to(self, other, ssize_t chunk_size=-1):
        if not isinstance(other, PRawQueue):
            raise TypeError()
        with nogil:
            self.ptr.move_to(deref((<PRawQueue> other).ptr), chunk_size)

    cpdef remove_duplicates(self):
        with nogil:
            self.ptr.remove_duplicates()


cdef class PQueue(PRawQueue):
    def __cinit__(self, dirname, dbname, LmdbOptions opts=None, Chain value_chain=None):
        self.value_chain = value_chain if value_chain else Chain(PickleSerializer(), None, None)

    def __init__(self, dirname, dbname, LmdbOptions opts=None, Chain value_chain=None):
        super(PQueue, self).__init__(dirname, dbname, opts)

    def __repr__(self):
        return u"PQueue(dbname='{}', dirname='{}')".format(
            make_unicode(self.dbname), make_unicode(self.dirname)
        )

    cpdef push_front(self, val):
        cdef PyBufferWrap view = move(PyBufferWrap(self.value_chain.dumps(val)))
        with nogil:
            self.ptr.push_front(view.get_mdb_val())

    cpdef push_back(self, val):
        cdef PyBufferWrap view = move(PyBufferWrap(self.value_chain.dumps(val)))
        with nogil:
            self.ptr.push_back(view.get_mdb_val())

    cpdef pop_back(self):
        cdef CBString v
        with nogil:
            v = self.ptr.pop_back()
        return self.value_chain.loads(topy(v))

    cpdef pop_front(self):
        cdef CBString v
        with nogil:
            v = self.ptr.pop_front()
        return self.value_chain.loads(topy(v))

    cpdef pop_all(self):
        cdef vector[CBString] vec
        with nogil:
            vec = self.ptr.pop_all()
        return [self.value_chain.loads(topy(v)) for v in vec]

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
            self.ptr.transform_values(make_unary_functor(unary_funct))

    cpdef remove_if(self, unary_pred):
        unary_pred = _adapt_unary_predicate(unary_pred, self.secret_key)
        #cdef PyObject* obj = <PyObject*> unary_pred
        with nogil:
            self.ptr.remove_if(make_unary_predicate(unary_pred))

    cpdef move_to(self, other, ssize_t chunk_size=-1):
        if not isinstance(other, PQueue):
            raise TypeError()
        with nogil:
            self.ptr.move_to(deref((<PQueue> other).ptr), chunk_size)



cdef class expiry_dict(object):
    def __init__(self, PDict values_dict, PDict index_dict, time_t default_expiry=3600, time_t prune_period=5):
        if not values_dict:
            raise ValueError('values_dict is not initialized')
        if not index_dict:
            raise ValueError('index_dict is not initialized')
        if values_dict == index_dict:
            raise ValueError('values_dict and index_dict can not point to the same LMDB database')
        if prune_period <= 0:
            raise ValueError("prune_period must be strictly positive")
        self.values_dict = values_dict
        self.index_dict = index_dict
        self.default_expiry = default_expiry
        self.prune_period = prune_period
        self.stopping = threading.Event()
        self.prune_expired()

    def __enter__(self):
        self.pruning_thread = threading.Thread(target=self.pruning_thread_fun)
        self.pruning_thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stopping.set()
        self.pruning_thread.join()
        self.stopping.clear()

    def pruning_thread_fun(self):
        while not self.stopping.is_set():
            self.stopping.wait(self.prune_period)
            self.prune_expired()

    cpdef prune_expired(self):
        cdef time_t now = c_time(NULL)
        cdef time_t expiry
        cdef PyBufferWrap values_key_view

        cdef scoped_ptr[cppIterator] index_it
        cdef scoped_ptr[cppIterator] values_it
        index_it.reset(new cppIterator(self.index_dict.ptr, 0, 0))
        values_it.reset(new cppIterator(self.values_dict.ptr, 0, 0))

        try:
            while not index_it.get().has_reached_end():
                s_expiry = self.index_dict.loads(self.index_dict.raw_get_value_buf(deref(index_it)))
                if s_expiry == b"none":
                    continue
                expiry = int(s_expiry)
                if now >= expiry:
                    key = self.index_dict.key_chain.loads(self.index_dict.raw_get_key_buf(deref(index_it)))
                    values_key_view = move(PyBufferWrap(self.values_dict.key_chain.dumps(key)))
                    with nogil:
                        index_it.get().dlte()
                        values_it.get().dlte(values_key_view.get_mdb_val())

                index_it.get().incr()

        except:
            index_it.get().set_rollback()
            values_it.get().set_rollback()
            raise

        finally:
            values_it.reset()
            index_it.reset()


    def __getitem__(self, key):
        cdef time_t expiry
        if key in self.index_dict:
            s_expiry = self.index_dict[key]
            if s_expiry == b"none":
                return self.values_dict[key]
            else:
                expiry = int(s_expiry)
                if c_time(NULL) < expiry:
                    return self.values_dict[key]
                else:
                    raise NotFound()
        else:
            raise NotFound()

    cpdef get(self, key, default=None):
        try:
            return self[key]
        except NotFound:
            return default

    def __setitem__(self, key, value):
        self.set(key, value)

    cpdef set(self, key, value, time_t expiry=0):
        if expiry == 0:
            expiry = self.default_expiry

        if expiry > 0:
            index_v = self.index_dict.value_chain.dumps(bytes(expiry + c_time(NULL)))
        else:
            index_v = self.index_dict.value_chain.dumps(b'none')

        cdef scoped_ptr[cppIterator] index_it
        cdef scoped_ptr[cppIterator] values_it
        index_it.reset(new cppIterator(self.index_dict.ptr, 0, 0))
        values_it.reset(new cppIterator(self.values_dict.ptr, 0, 0))

        try:
            self.index_dict.raw_set_item_buf(deref(index_it), self.index_dict.key_chain.dumps(key), index_v)
            self.values_dict.raw_set_item_buf(deref(values_it), self.values_dict.key_chain.dumps(key), self.values_dict.value_chain.dumps(value))
        except:
            index_it.get().set_rollback()
            values_it.get().set_rollback()
            raise
        finally:
            values_it.reset()
            index_it.reset()

    def __delitem__(self, key):
        cdef time_t now = c_time(NULL)
        index_key = self.index_dict.key_chain.dumps(key)
        cdef cppIterator index_it = move(cppIterator(self.index_dict.ptr, PyBufferWrap(index_key).get_mdb_val(), 0))

        if index_it.has_reached_end():
            raise NotFound()
        s_expiry = self.index_dict.loads(self.index_dict.raw_get_value_buf(index_it))
        if s_expiry == b"none":
            self.index_dict.raw_set_item_buf(index_it, index_key, self.index_dict.value_chain.dumps(bytes(now - 1)))
            return
        cdef time_t expiry = int(s_expiry)
        if now < expiry:
            self.index_dict.raw_set_item_buf(index_it, index_key, self.index_dict.value_chain.dumps(bytes(now - 1)))
            return
        raise NotFound()

    cpdef pop(self, key, default=None):
        cdef time_t now = c_time(NULL)
        index_key = self.index_dict.key_chain.dumps(key)
        cdef cppIterator index_it = move(cppIterator(self.index_dict.ptr, PyBufferWrap(index_key).get_mdb_val(), 0))
        if index_it.has_reached_end():
            raise NotFound()
        s_expiry = self.index_dict.loads(self.index_dict.raw_get_value_buf(index_it))
        if s_expiry == b"none":
            self.index_dict.raw_set_item_buf(index_it, index_key, self.index_dict.value_chain.dumps(bytes(now - 1)))
            return self.values_dict[key]
        cdef time_t expiry = int(s_expiry)
        if now < expiry:
            self.index_dict.raw_set_item_buf(index_it, index_key, self.index_dict.value_chain.dumps(bytes(now - 1)))
            return self.values_dict[key]
        if default:
            return default
        raise NotFound()

    cpdef popitem(self):
        cdef time_t expiry
        cdef cppIterator index_it = move(cppIterator(self.index_dict.ptr, 0, 0))
        cdef time_t now = c_time(NULL)
        cdef PyBufferWrap index_value_view = move(PyBufferWrap(self.index_dict.value_chain.dumps(bytes(now - 1))))

        while not index_it.has_reached_end():
            key = self.index_dict.key_chain.loads(self.index_dict.raw_get_key_buf(index_it))
            s_expiry = self.index_dict.value_chain.loads(self.index_dict.raw_get_value_buf(index_it))
            if s_expiry == b"none":
                with nogil:
                    index_it.set_value(index_value_view.get_mdb_val())
                return key, self.values_dict[key]
            expiry = int(s_expiry)
            if now < expiry:
                with nogil:
                    index_it.set_value(index_value_view.get_mdb_val())
                return key, self.values_dict[key]
            index_it.incr()

        raise EmptyDatabase()

    def __iter__(self):
        cdef time_t expiry
        cdef time_t now = c_time(NULL)
        for key, s_expiry in self.index_dict.items():
            if s_expiry == b"none":
                yield key
            else:
                expiry = int(s_expiry)
                if now < expiry:
                    yield key

    def keys(self):
        return self.__iter__(self)

    def values(self):
        cdef time_t expiry
        cdef time_t now = c_time(NULL)
        for key, s_expiry in self.index_dict.items():
            if s_expiry == b"none":
                yield self.values_dict[key]
            else:
                expiry = int(s_expiry)
                if now < expiry:
                    yield self.values_dict[key]

    def items(self):
        cdef time_t expiry
        cdef time_t now = c_time(NULL)
        for key, s_expiry in self.index_dict.items():
            if s_expiry == b"none":
                yield (key, self.values_dict[key])
            else:
                expiry = int(s_expiry)
                if now < expiry:
                    yield (key, self.values_dict[key])

    def __contains__(self, key):
        cdef time_t expiry
        if key in self.index_dict:
            s_expiry = self.index_dict[key]
            if s_expiry == b"none":
                return True
            else:
                expiry = int(s_expiry)
                if c_time(NULL) < expiry:
                    return True
                else:
                    raise False
        else:
            return False

    def update(self, e=None, **kwds):
        future = self.index_dict.value_chain.dumps(bytes(c_time(NULL) + self.default_expiry))
        cdef scoped_ptr[cppIterator] index_it
        cdef scoped_ptr[cppIterator] values_it
        index_it.reset(new cppIterator(self.index_dict.ptr, 0, 0))
        values_it.reset(new cppIterator(self.values_dict.ptr, 0, 0))

        try:
            if e is not None:
                if hasattr(e, 'keys'):
                    for key in e.keys():
                        self.values_dict.raw_set_item_buf(deref(values_it), self.values_dict.key_chain.dumps(key), self.values_dict.value_chain.dumps(e[key]))
                        self.index_dict.raw_set_item_buf(deref(index_it), self.index_dict.key_chain.dumps(key), future)
                else:
                    for (key, value) in e:
                        self.values_dict.raw_set_item_buf(deref(values_it), self.values_dict.key_chain.dumps(key), self.values_dict.value_chain.dumps(value))
                        self.index_dict.raw_set_item_buf(deref(index_it), self.index_dict.key_chain.dumps(key), future)

            for key in kwds:
                self.values_dict.raw_set_item_buf(deref(values_it), self.values_dict.key_chain.dumps(key), self.values_dict.value_chain.dumps(kwds[key]))
                self.index_dict.raw_set_item_buf(deref(index_it), self.index_dict.key_chain.dumps(key), future)
        except:
            index_it.get().set_rollback()
            values_it.get().set_rollback()
            raise
        finally:
            values_it.reset()
            index_it.reset()

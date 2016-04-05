
cdef class ExpiryDict(object):
    def __init__(self, bytes dirname, time_t default_expiry=3600, time_t prune_period=5, LmdbOptions opts=None,
                 Chain key_chain=None, Chain value_chain=None):

        if opts is None:
            opts = LmdbOptions()
        if key_chain is None:
            key_chain = Chain(None, None, None)
        if value_chain is None:
            value_chain = Chain(PickleSerializer(), None, None)
        if prune_period <= 0:
            raise ValueError("prune_period must be strictly positive")

        nonechain = NoneChain()

        self.index_dict = PDict(dirname=join(dirname, 'index'), dbname=b'', opts=opts, mapping=None,
                                key_chain=key_chain, value_chain=nonechain)

        self.values_dict = PDict(dirname=join(dirname, 'values'), dbname=b'', opts=opts, mapping=None,
                                 key_chain=key_chain, value_chain=value_chain)

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

        cdef PRawDictIterator index_it = PRawDictIterator(self.index_dict)
        cdef PRawDictIterator values_it = PRawDictIterator(self.values_dict)

        with index_it:
            with values_it:
                try:
                    while not index_it.has_reached_end():
                        s_expiry = index_it.get_value_buf()
                        if s_expiry == b"none":
                            continue
                        expiry = int(s_expiry)
                        if now >= expiry:
                            key = index_it.get_key_buf()
                            index_it.dlte()
                            values_it.dlte(key)
                        index_it.incr()

                except:
                    index_it.set_rollback()
                    values_it.set_rollback()
                    raise


    def __getitem__(self, key):
        if not key:
            raise EmptyKey()
        cdef time_t expiry
        cdef PRawDictIterator index_it = PRawDictConstIterator(self.index_dict, key=key)
        cdef PRawDictIterator values_it = PRawDictConstIterator(self.values_dict, key=key)

        with index_it:
            with values_it:
                if index_it.has_reached_end():
                    raise NotFound()
                s_expiry = index_it.get_value_buf()
                if s_expiry == b"none":
                    return values_it.get_value_buf()
                expiry = int(s_expiry)
                if c_time(NULL) < expiry:
                    return values_it.get_value_buf()
                raise NotFound()

    cpdef get(self, key, default=None):
        try:
            return self[key]
        except NotFound:
            return default

    def __setitem__(self, key, value):
        self.set(key, value)

    cpdef set(self, key, value, time_t expiry=0):
        if not key:
            raise EmptyKey()
        if expiry == 0:
            expiry = self.default_expiry
        s_expiry = b"none" if expiry <= 0 else bytes(expiry + c_time(NULL))

        cdef PRawDictIterator index_it = PRawDictIterator(self.index_dict)
        cdef PRawDictIterator values_it = PRawDictIterator(self.values_dict)

        with index_it:
            with values_it:
                try:
                    index_it.set_item_buf(key, s_expiry)
                    values_it.set_item_buf(key, value)
                except:
                    index_it.set_rollback()
                    values_it.set_rollback()
                    raise

    def __delitem__(self, key):
        if not key:
            raise EmptyKey()
        cdef time_t now = c_time(NULL)
        before = bytes(now - 1)

        cdef PRawDictIterator index_it = PRawDictIterator(self.index_dict, key=key)
        cdef time_t expiry

        with index_it:
            if index_it.has_reached_end():
                raise NotFound()
            s_expiry = index_it.get_value_buf()
            if s_expiry == b"none":
                index_it.set_item_buf(key, before)
                return
            expiry = int(s_expiry)
            if now < expiry:
                index_it.set_item_buf(key, before)
                return
            raise NotFound()

    cpdef pop(self, key, default=None):
        if not key:
            raise EmptyKey()
        cdef time_t now = c_time(NULL)
        cdef time_t expiry
        before = bytes(now - 1)

        cdef PRawDictIterator index_it = PRawDictIterator(self.index_dict, key=key)
        cdef PRawDictIterator values_it = PRawDictConstIterator(self.values_dict, key=key)

        with index_it:
            with values_it:
                try:
                    if index_it.has_reached_end():
                        raise NotFound()
                    s_expiry = index_it.get_value_buf()
                    if s_expiry == b"none":
                        index_it.set_item_buf(key, before)
                        return values_it.get_value_buf()
                    expiry = int(s_expiry)
                    if now < expiry:
                        index_it.set_item_buf(key, before)
                        return values_it.get_value_buf()
                    if default:
                        return default
                    raise NotFound()
                except:
                    index_it.set_rollback()
                    raise


    cpdef popitem(self):
        cdef time_t expiry
        cdef time_t now = c_time(NULL)
        before = bytes(now - 1)
        cdef PRawDictIterator index_it = PRawDictIterator(self.index_dict)
        cdef PRawDictIterator values_it
        with index_it:
            while not index_it.has_reached_end():
                s_expiry = index_it.get_value_buf()
                key = index_it.get_key_buf()
                values_it = PRawDictConstIterator(self.values_dict, key=key)
                with values_it:
                    if s_expiry == b"none":
                        index_it.set_item_buf(key, before)
                        return key, values_it.get_value_buf()
                    expiry = int(s_expiry)
                    if now < expiry:
                        index_it.set_item_buf(key, before)
                        return key, values_it.get_value_buf()
                index_it.incr()
            raise EmptyDatabase()

    def __iter__(self):
        cdef time_t expiry
        cdef time_t now = c_time(NULL)
        cdef PRawDictIterator index_it = PRawDictConstIterator(self.index_dict)
        with index_it:
            while not index_it.has_reached_end():
                s_expiry = index_it.get_value_buf()
                if s_expiry == b"none":
                    yield index_it.get_key_buf()
                else:
                    expiry = int(s_expiry)
                    if now < expiry:
                        yield index_it.get_key_buf()
                index_it.incr()

    def keys(self):
        return self.__iter__(self)

    def values(self):
        cdef time_t expiry
        cdef time_t now = c_time(NULL)
        cdef PRawDictIterator index_it = PRawDictConstIterator(self.index_dict)
        cdef PRawDictIterator values_it = PRawDictConstIterator(self.index_dict)
        with index_it:
            with values_it:
                while not index_it.has_reached_end():
                    s_expiry = index_it.get_value_buf()
                    if s_expiry == b"none":
                        yield values_it.get_value_buf()
                    else:
                        expiry = int(s_expiry)
                        if now < expiry:
                            yield values_it.get_value_buf()
                    index_it.incr()
                    values_it.incr()

    def items(self):
        cdef time_t expiry
        cdef time_t now = c_time(NULL)
        cdef PRawDictIterator index_it = PRawDictConstIterator(self.index_dict)
        cdef PRawDictIterator values_it = PRawDictConstIterator(self.index_dict)
        with index_it:
            with values_it:
                while not index_it.has_reached_end():
                    s_expiry = index_it.get_value_buf()
                    if s_expiry == b"none":
                        yield values_it.get_item_buf()
                    else:
                        expiry = int(s_expiry)
                        if now < expiry:
                            yield values_it.get_item_buf()
                    index_it.incr()
                    values_it.incr()

    def __contains__(self, key):
        if not key:
            raise EmptyKey()
        cdef time_t expiry
        cdef PRawDictIterator index_it = PRawDictConstIterator(self.index_dict, key=key)
        with index_it:
            if index_it.has_reached_end():
                return False
            s_expiry = index_it.get_value_buf()
            if s_expiry == b"none":
                return False
            expiry = int(s_expiry)
            if c_time(NULL) < expiry:
                return True
            return False

    def update(self, e=None, **kwds):
        cdef PRawDictIterator index_it = PRawDictIterator(self.index_dict)
        cdef PRawDictIterator values_it = PRawDictIterator(self.values_dict)
        s_expiry = bytes(self.default_expiry + c_time(NULL))

        with index_it:
            with values_it:
                try:
                    if e is not None:
                        if hasattr(e, 'keys'):
                            for key in e.keys():
                                index_it.set_item_buf(key, s_expiry)
                                values_it.set_item_buf(key, e[key])
                        else:
                            for (key, value) in e:
                                index_it.set_item_buf(key, s_expiry)
                                values_it.set_item_buf(key, value)
                    for key in kwds:
                        index_it.set_item_buf(key, s_expiry)
                        values_it.set_item_buf(key, kwds[key])
                except:
                    index_it.set_rollback()
                    values_it.set_rollback()
                    raise


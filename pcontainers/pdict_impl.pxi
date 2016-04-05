# noinspection PyPep8Naming
cdef class PRawDictConstIterator(object):
    def __init__(self, PRawDict d, int pos=0, key=None):
        self.dict = d
        self.pos = pos
        self.key = d.key_chain.dumps(key) if key is not None else None

    def __enter__(self):
        if self.key is None:
            self.cpp_iterator = move(cppConstIterator(self.dict.ptr, self.pos))
        else:
            self.cpp_iterator = move(cppConstIterator(self.dict.ptr, PyBufferWrap(self.key).get_mdb_val()))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cpp_iterator = move(cppConstIterator())

    cdef cpp_bool has_reached_end(self):
        return self.cpp_iterator.has_reached_end()

    cdef cpp_bool has_reached_beginning(self):
        return self.cpp_iterator.has_reached_beginning()

    cdef incr(self):
        self.cpp_iterator.incr()

    cdef decr(self):
        self.cpp_iterator.decr()

    cdef get_key(self):
        cdef int keysize = self.dict.ptr.get_maxkeysize()
        cdef MDB_val k
        cdef bytearray obj = bytearray(keysize)
        cdef PyBufferWrap view = move(PyBufferWrap(obj))
        with nogil:
            k = self.cpp_iterator.get_key_buffer()
            memcpy(view.buf(), k.mv_data, k.mv_size)
        view.close()
        PyByteArray_Resize(obj, k.mv_size)
        return self.dict.key_chain.loads(obj)

    cdef get_key_buf(self):
        cdef MDB_val k
        cdef void* ptr
        with nogil:
            k = self.cpp_iterator.get_key_buffer()
            ptr = copy_mdb_val(k)
        return self.dict.key_chain.loads(make_mbufferio(ptr, k.mv_size, 1))

    cdef get_value(self):
        cdef MDB_val v
        cdef cpp_bool done = 0
        cdef bytearray obj = bytearray(4096)
        cdef PyBufferWrap view = move(PyBufferWrap(obj))
        with nogil:
            v = self.cpp_iterator.get_value_buffer()
            if v.mv_size <= 4096:
                memcpy(view.buf(), v.mv_data, v.mv_size)
                done = 1
        view.close()
        if done:
            PyByteArray_Resize(obj, v.mv_size)
            return self.dict.value_chain.loads(obj)

        obj = bytearray(v.mv_size)
        view = move(PyBufferWrap(obj))
        with nogil:
            memcpy(view.buf(), v.mv_data, v.mv_size)
        view.close()
        return self.dict.value_chain.loads(obj)

    cdef get_value_buf(self):
        cdef MDB_val v
        cdef void* ptr
        with nogil:
            v = self.cpp_iterator.get_value_buffer()
            ptr = copy_mdb_val(v)
        return self.dict.value_chain.loads(make_mbufferio(ptr, v.mv_size, 1))

    cdef get_item(self):
        cdef int keysize = self.dict.ptr.get_maxkeysize()
        cdef pair[MDB_val, MDB_val] kv
        cdef cpp_bool done = 0
        cdef bytearray key_obj = bytearray(keysize)
        cdef bytearray value_obj = bytearray(4096)

        cdef PyBufferWrap key_view = move(PyBufferWrap(key_obj))
        cdef PyBufferWrap value_view = move(PyBufferWrap(value_obj))

        with nogil:
            kv = self.cpp_iterator.get_item_buffer()
            memcpy(key_view.buf(), kv.first.mv_data, kv.first.mv_size)
            if kv.second.mv_size <= 4096:
                memcpy(value_view.buf(), kv.second.mv_data, kv.second.mv_size)
                done = 1
        key_view.close()
        value_view.close()
        PyByteArray_Resize(key_obj, kv.first.mv_size)
        if done:
            PyByteArray_Resize(value_obj, kv.second.mv_size)
            return self.dict.key_chain.loads(key_obj), self.dict.value_chain.loads(value_obj)

        value_obj = bytearray(kv.second.mv_size)
        value_view = move(PyBufferWrap(value_obj))
        with nogil:
            memcpy(value_view.buf(), kv.second.mv_data, kv.second.mv_size)
        value_view.close()
        return self.dict.key_chain.loads(key_obj), self.dict.value_chain.loads(value_obj)

    cdef get_item_buf(self):
        cdef pair[MDB_val, MDB_val] kv
        cdef void* key_ptr
        cdef void* value_ptr
        with nogil:
            kv = self.cpp_iterator.get_item_buffer()
            key_ptr = copy_mdb_val(kv.first)
            value_ptr = copy_mdb_val(kv.second)
        return self.dict.key_chain.loads(make_mbufferio(key_ptr, kv.first.mv_size, 1)), self.dict.value_chain.loads(make_mbufferio(value_ptr, kv.second.mv_size, 1))

# noinspection PyPep8Naming
cdef class PRawDictIterator(object):
    def __init__(self, PRawDict d, int pos=0, key=None, cpp_bool readonly=0):
        self.dict = d
        self.pos = pos
        self.key = None if not key else d.key_chain.dumps(key)
        self.readonly = readonly

    def __enter__(self):
        if self.key is None:
            self.cpp_iterator = move(cppIterator(self.dict.ptr, self.pos, self.readonly))
        else:
            self.cpp_iterator = move(cppIterator(self.dict.ptr, PyBufferWrap(self.key).get_mdb_val(), self.readonly))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cpp_iterator = move(cppIterator())

    cdef cpp_bool has_reached_end(self):
        return self.cpp_iterator.has_reached_end()

    cdef cpp_bool has_reached_beginning(self):
        return self.cpp_iterator.has_reached_beginning()

    cdef incr(self):
        self.cpp_iterator.incr()

    cdef decr(self):
        self.cpp_iterator.decr()

    cdef set_rollback(self):
        self.cpp_iterator.set_rollback()

    cdef set_item_buf(self, k, v):
        if not k:
            raise EmptyKey()
        cdef PyBufferWrap key_view = move(PyBufferWrap(self.dict.key_chain.dumps(k)))
        if key_view.length() > 511:
            raise BadValSize("key is too long")
        cdef PyBufferWrap value_view = move(PyBufferWrap(self.dict.value_chain.dumps(v)))
        with nogil:
            self.cpp_iterator.set_key_value(key_view.get_mdb_val(), value_view.get_mdb_val())

    cdef get_key_buf(self):
        cdef MDB_val k
        cdef void* ptr
        with nogil:
            k = self.cpp_iterator.get_key_buffer()
            ptr = copy_mdb_val(k)
        return self.dict.key_chain.loads(make_mbufferio(ptr, k.mv_size, 1))

    cdef get_value_buf(self):
        cdef MDB_val v
        cdef void* ptr
        with nogil:
            v = self.cpp_iterator.get_value_buffer()
            ptr = copy_mdb_val(v)
        return self.dict.value_chain.loads(make_mbufferio(ptr, v.mv_size, 1))

    cdef dlte(self, key=None):
        cdef PyBufferWrap key_view
        if not key:
            self.cpp_iterator.dlte()
        else:
            key_view = move(PyBufferWrap(self.dict.key_chain.dumps(key)))
            with nogil:
                self.cpp_iterator.dlte(key_view.get_mdb_val())

cdef class DirectAccess(object):
    def __cinit__(self, PRawDict d, bytes item):
        cdef CBString key = tocbstring(item)
        self.cpp_iterator = move(cppConstIterator(d.ptr, key))
        if self.cpp_iterator.has_reached_end():
            raise NotFound()
        cdef MDB_val v = self.cpp_iterator.get_value_buffer()
        self.buf = make_mbufferio(v.mv_data, v.mv_size, 0)

    def __dealloc__(self):
        self.buf = None

    cpdef read(self, ssize_t n=-1):
        return self.buf.read(n)

cdef class PRawDict(object):
    def __cinit__(self, bytes dirname, bytes dbname, LmdbOptions opts=None, mapping=None, Chain key_chain=None, Chain value_chain=None, **kwarg):

        cdef CBString dirn = tocbstring(dirname)
        cdef CBString dbn = tocbstring(dbname)
        if dirn.length() == 0:
            raise ValueError("empty dirname")
        if opts is None:
            opts = LmdbOptions()
        self.ptr = new cppPersistentDict(dirn, dbn, (<LmdbOptions> opts).opts)

        self.rmrf_at_delete = 0
        self.key_chain = NoneChain()
        self.value_chain = NoneChain()


    def __init__(self, bytes dirname, bytes dbname, LmdbOptions opts=None, mapping=None, Chain key_chain=None, Chain value_chain=None, **kwarg):
        if mapping is not None:
            self.update(e=mapping)
        if kwarg:
            self.update(e=kwarg)

    def __dealloc__(self):
        if self.ptr is not NULL:
            if self.rmrf_at_delete:
                shutil.rmtree(self.dirname)
                self.rmrf_at_delete = 0

            del self.ptr
            self.ptr = NULL

    def __repr__(self):
        return u"PRawDict(dbname='{}', dirname='{}')".format(
            make_unicode(self.dbname), make_unicode(self.dirname)
        )

    @classmethod
    def make_temp(cls, destroy=True, LmdbOptions opts=None, Chain key_chain=None, Chain value_chain=None):
        cdef shared_ptr[TempDirectory] temp_dir_ptr = make_temp_directory(True, False)
        d = cls(dirname=topy(temp_dir_ptr.get().get_path()), dbname=bytes(uuid.uuid1()), opts=opts, key_chain=key_chain, value_chain=value_chain)
        (<PRawDict>d).rmrf_at_delete = bool(destroy)
        return d

    def __reduce__(self):
        return dict, (self.noiteritems(), )

    def __richcmp__(self, other, op):
        # todo: simplify
        if op == 2:
            if not isinstance(other, PRawDict):
                return False
            if isinstance(self, PDict) and not isinstance(other, PDict):
                return False
            if isinstance(other, PDict) and not isinstance(self, PDict):
                return False
            return deref((<PRawDict> self).ptr) == deref((<PRawDict> other).ptr)
        elif op == 3:
            if not isinstance(other, PRawDict):
                return True
            if isinstance(self, PDict) and not isinstance(other, PDict):
                return True
            if isinstance(other, PDict) and not isinstance(self, PDict):
                return True
            return deref((<PRawDict> self).ptr) != deref((<PRawDict> other).ptr)
        raise ValueError("unsupported comparison")

    property dirname:
        def __get__(self):
            return topy(self.ptr.get_dirname())

    property dbname:
        def __get__(self):
            return topy(self.ptr.get_dbname())

    def __getitem__(self, item):
        if not item:
            raise EmptyKey()
        cdef PRawDictConstIterator it = PRawDictConstIterator(self, key=item)
        with it:
            if it.has_reached_end():
                raise NotFound()
            return it.get_value()

    cpdef get(self, item, default=b''):
        try:
            return self[item]
        except NotFound:
            return default

    cpdef get_direct(self, item):
        if not item:
            raise EmptyKey()
        return DirectAccess(self, item)

    cpdef setdefault(self, key, default=b''):
        if not key:
            raise EmptyKey()
        cdef PyBufferWrap key_view = move(PyBufferWrap(self.key_chain.dumps(key)))
        cdef PyBufferWrap default_view = move(PyBufferWrap(self.value_chain.dumps(default)))
        cdef CBString ret
        with nogil:
            ret = self.ptr.setdefault(key_view.get_mdb_val(), default_view.get_mdb_val())
        return self.value_chain.loads(topy(ret))

    def __setitem__(self, key, value):
        if not key:
            raise EmptyKey()
        cdef PRawDictIterator it = PRawDictIterator(self)
        with it:
            it.set_item_buf(key, value)

    def __delitem__(self, key):
        if not key:
            raise EmptyKey()
        cdef PyBufferWrap key_view = move(PyBufferWrap(self.key_chain.dumps(key)))
        with nogil:
            self.ptr.erase(key_view.get_mdb_val())

    cpdef erase(self, first, last):
        cdef CBString f = tocbstring(first)
        cdef CBString l = tocbstring(last)
        with nogil:
            self.ptr.erase_interval(f, l)

    cpdef noiterkeys(self):
        return list(self.keys())

    cpdef noitervalues(self):
        return list(self.values())

    cpdef noiteritems(self):
        return list(self.items())

    cpdef pop(self, key, default=None):
        cdef PyBufferWrap key_view = move(PyBufferWrap(self.key_chain.dumps(key)))
        cdef CBString v
        try:
            with nogil:
                v = self.ptr.pop(key_view.get_mdb_val())
        except NotFound:
            if default is None:
                raise
            return default
        return self.value_chain.loads(make_mbufferio_from_cbstring(v))

    cpdef popitem(self):
        cdef pair[CBString, CBString] p
        with nogil:
            p = self.ptr.popitem()
        return self.key_chain.loads(make_mbufferio_from_cbstring(p.first)), self.value_chain.loads(make_mbufferio_from_cbstring(p.second))

    def __iter__(self):
        return self.keys()

    def keys(self, reverse=False):
        cdef PRawDictConstIterator it
        if reverse:
            it = PRawDictConstIterator(self, pos=1)
            with it:
                it.decr()
                while not it.has_reached_beginning():
                    yield it.get_key()
                    it.decr()
        else:
            it = PRawDictConstIterator(self)
            with it:
                while not it.has_reached_end():
                    yield it.get_key()
                    it.incr()

    cpdef iterkeys(self, reverse=False):
        return self.keys(reverse)

    def values(self, reverse=False):
        cdef PRawDictConstIterator it
        if reverse:
            it = PRawDictConstIterator(self, pos=1)
            with it:
                it.decr()
                while not it.has_reached_beginning():
                    yield it.get_value()
                    it.decr()
        else:
            it = PRawDictConstIterator(self)
            with it:
                while not it.has_reached_end():
                    yield it.get_value()
                    it.incr()

    cpdef itervalues(self, reverse=False):
        return self.values(reverse)

    def items(self, reverse=False):
        cdef PRawDictConstIterator it
        if reverse:
            it = PRawDictConstIterator(self, pos=1)
            with it:
                it.decr()
                while not it.has_reached_beginning():
                    yield it.get_item()
                    it.decr()
        else:
            it = PRawDictConstIterator(self)
            with it:
                while not it.has_reached_end():
                    yield it.get_item()
                    it.incr()

    cpdef iteritems(self, reverse=False):
        return self.items(reverse)

    def __nonzero__(self):
        return self.ptr.is_initialized()

    def __len__(self):
        return self.ptr.size()

    cpdef clear(self):
        self.ptr.clear()

    def __contains__(self, key):
        if not key:
            return False
        cdef PyBufferWrap key_view = move(PyBufferWrap(self.key_chain.loads(key)))
        return self.ptr.contains(key_view.get_mdb_val())

    cpdef has_key(self, key):
        return key in self

    def update(self, e=None, **kwds):
        cdef PRawDictIterator it = PRawDictIterator(self)
        with it:
            if e is not None:
                if hasattr(e, 'keys'):
                    for key in e.keys():
                        it.set_item_buf(key, e[key])
                else:
                    for (key, value) in e:
                        it.set_item_buf(key, value)

            for key in kwds:
                it.set_item_buf(key, kwds[key])

    # noinspection PyPep8Naming
    @classmethod
    def fromkeys(cls, dirname, dbname, opts=None, S=None, v=None):
        d = cls(dirname, dbname, opts=opts)
        v = b'' if v is None else v
        if S is not None:
            d.update((key, v) for key in S)
        return d

    cpdef transform_values(self, binary_funct):
        with nogil:
            self.ptr.transform_values(make_binary_scalar_functor(binary_funct))

    cpdef remove_if(self, binary_pred):
        with nogil:
            self.ptr.remove_if(make_binary_predicate(binary_pred))

    cpdef move_to(self, other, ssize_t chunk_size=-1):
        if not isinstance(other, PRawDict):
            raise TypeError()
        cdef CBString empt
        with nogil:
            self.ptr.move_to(deref((<PRawDict> other).ptr), empt, empt, chunk_size)


    cpdef remove_duplicates(self, first="", last=""):
        cdef CBString firstkey = tocbstring(first)
        cdef CBString lastkey = tocbstring(last)
        with nogil:
            self.ptr.remove_duplicates(firstkey, lastkey)


cdef class PDict(PRawDict):
    def __cinit__(self, bytes dirname, bytes dbname, LmdbOptions opts=None, mapping=None, Chain key_chain=None, Chain value_chain=None, **kwarg):
        self.key_chain = key_chain if key_chain else Chain(None, None, None)
        self.value_chain = value_chain if value_chain else Chain(PickleSerializer(), None, None)

    def __init__(self, bytes dirname, bytes dbname, LmdbOptions opts=None, mapping=None, Chain key_chain=None, Chain value_chain=None, **kwarg):
        super(PDict, self).__init__(dirname, dbname, opts, mapping, **kwarg)

    def __dealloc__(self):
        pass

    def __repr__(self):
        return u"PDict(dbname='{}', dirname='{}')".format(make_unicode(self.dbname), make_unicode(self.dirname))

    def __reduce__(self):
        return dict, (self.noiteritems(), )

    cpdef get(self, key, default=None):
        try:
            return self[key]
        except NotFound:
            return default

    cpdef erase(self, first, last):
        raise NotImplementedError()

    cpdef transform_values(self, binary_funct):
        binary_funct = _adapt_binary_scalar_functor(binary_funct, self.key_chain, self.value_chain)
        with nogil:
            self.ptr.transform_values(make_binary_scalar_functor(binary_funct))

    cpdef remove_if(self, binary_pred):
        binary_pred = _adapt_binary_predicate(binary_pred, self.key_chain, self.value_chain)
        with nogil:
            self.ptr.remove_if(make_binary_predicate(binary_pred))

    cpdef move_to(self, other, ssize_t chunk_size=-1):
        if not isinstance(other, PDict):
            raise TypeError()
        cdef CBString empt
        with nogil:
            self.ptr.move_to(deref((<PDict> other).ptr), empt, empt, chunk_size)

    cpdef remove_duplicates(self, first="", last=""):
        with nogil:
            self.ptr.remove_duplicates()


collections.MutableMapping.register(PRawDict)
collections.MutableMapping.register(PDict)

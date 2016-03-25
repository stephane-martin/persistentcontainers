
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
    def __cinit__(self, dirname, dbname, LmdbOptions opts=None, mapping=None, Chain key_chain=None, Chain value_chain=None, **kwarg):
        self.rmrf_at_delete = 0
        cdef CBString dirn = tocbstring(make_utf8(dirname))
        cdef CBString dbn = tocbstring(make_utf8(dbname))
        if dirn.length() == 0:
            raise ValueError("empty dirname")
        if opts is None:
            opts = LmdbOptions()
        self.ptr = new cppPersistentDict(dirn, dbn, (<LmdbOptions> opts).opts)


    def __init__(self, dirname, dbname, LmdbOptions opts=None, mapping=None, Chain key_chain=None, Chain value_chain=None, **kwarg):
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
        d = cls(dirname=topy(temp_dir_ptr.get().get_path()), dbname=uuid.uuid4(), opts=opts, key_chain=key_chain, value_chain=value_chain)
        (<PRawDict>d).rmrf_at_delete = bool(destroy)
        return d

    def __reduce__(self):
        return dict, (self.noiteritems(), )

    def __richcmp__(self, other, op):
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

    # the following _get* functions allow us to release the GIL when retrieving data from LMDB
    # (we use memcpy to retrieve data from LMDB, which could block in case of page faults)
    # as we don't know the size of data before calling mdb_cursor_get, first we optimisticly try with a
    # preallocated 4096 bytes long buffer, so that in most cases we don't have to release the GIL twice

    # the _get*_buf functions avoid one memory copy to feed pickle

    # all this can be pretty useful when dealing with big values from LMDB

    cdef raw_get_key(self, cppConstIterator& it):
        cdef int keysize = self.ptr.get_maxkeysize()
        cdef MDB_val k
        cdef bytearray obj = bytearray(keysize)
        cdef PyBufferWrap view = move(PyBufferWrap(obj))
        with nogil:
            k = it.get_key_buffer()
            memcpy(view.buf(), k.mv_data, k.mv_size)
        view.close()
        PyByteArray_Resize(obj, k.mv_size)
        return obj

    cdef raw_get_key_buf(self, cppIterator& it):
        cdef MDB_val k
        cdef void* ptr
        with nogil:
            k = it.get_key_buffer()
            ptr = copy_mdb_val(k)
        return make_mbufferio(ptr, k.mv_size, 1)

    cdef raw_get_key_buf_const(self, cppConstIterator& it):
        cdef MDB_val k
        cdef void* ptr
        with nogil:
            k = it.get_key_buffer()
            ptr = copy_mdb_val(k)
        return make_mbufferio(ptr, k.mv_size, 1)

    cdef raw_get_value(self, cppConstIterator& it):
        cdef MDB_val v
        cdef cpp_bool done = 0
        cdef bytearray obj = bytearray(4096)
        cdef PyBufferWrap view = move(PyBufferWrap(obj))
        with nogil:
            v = it.get_value_buffer()
            if v.mv_size <= 4096:
                memcpy(view.buf(), v.mv_data, v.mv_size)
                done = 1
        view.close()
        if done:
            PyByteArray_Resize(obj, v.mv_size)
            return obj

        obj = bytearray(v.mv_size)
        view = move(PyBufferWrap(obj))
        with nogil:
            memcpy(view.buf(), v.mv_data, v.mv_size)
        view.close()
        return obj

    cdef raw_get_value_buf(self, cppIterator& it):
        cdef MDB_val v
        cdef void* ptr
        with nogil:
            v = it.get_value_buffer()
            ptr = copy_mdb_val(v)
        return make_mbufferio(ptr, v.mv_size, 1)

    cdef raw_get_value_buf_const(self, cppConstIterator& it):
        cdef MDB_val v
        cdef void* ptr
        with nogil:
            v = it.get_value_buffer()
            ptr = copy_mdb_val(v)
        return make_mbufferio(ptr, v.mv_size, 1)


    cdef raw_get_item(self, cppConstIterator& it):
        cdef int keysize = self.ptr.get_maxkeysize()
        cdef pair[MDB_val, MDB_val] kv
        cdef cpp_bool done = 0
        cdef bytearray key_obj = bytearray(keysize)
        cdef bytearray value_obj = bytearray(4096)

        cdef PyBufferWrap key_view = move(PyBufferWrap(key_obj))
        cdef PyBufferWrap value_view = move(PyBufferWrap(value_obj))

        with nogil:
            kv = it.get_item_buffer()
            memcpy(key_view.buf(), kv.first.mv_data, kv.first.mv_size)
            if kv.second.mv_size <= 4096:
                memcpy(value_view.buf(), kv.second.mv_data, kv.second.mv_size)
                done = 1
        key_view.close()
        value_view.close()
        PyByteArray_Resize(key_obj, kv.first.mv_size)
        if done:
            PyByteArray_Resize(value_obj, kv.second.mv_size)
            return key_obj, value_obj

        value_obj = bytearray(kv.second.mv_size)
        value_view = move(PyBufferWrap(value_obj))
        with nogil:
            memcpy(value_view.buf(), kv.second.mv_data, kv.second.mv_size)
        value_view.close()
        return key_obj, value_obj

    cdef raw_get_item_buf(self, cppIterator& it):
        cdef pair[MDB_val, MDB_val] kv
        cdef void* key_ptr
        cdef void* value_ptr
        with nogil:
            kv = it.get_item_buffer()
            key_ptr = copy_mdb_val(kv.first)
            value_ptr = copy_mdb_val(kv.second)
        return make_mbufferio(key_ptr, kv.first.mv_size, 1), make_mbufferio(value_ptr, kv.second.mv_size, 1)

    cdef raw_get_item_buf_const(self, cppConstIterator& it):
        cdef pair[MDB_val, MDB_val] kv
        cdef void* key_ptr
        cdef void* value_ptr
        with nogil:
            kv = it.get_item_buffer()
            key_ptr = copy_mdb_val(kv.first)
            value_ptr = copy_mdb_val(kv.second)
        return make_mbufferio(key_ptr, kv.first.mv_size, 1), make_mbufferio(value_ptr, kv.second.mv_size, 1)

    cdef raw_set_item_buf(self, cppIterator& it, k, v):
        cdef PyBufferWrap key_view = move(PyBufferWrap(k))
        cdef PyBufferWrap value_view = move(PyBufferWrap(v))

        with nogil:
            it.set_key_value(key_view.get_mdb_val(), value_view.get_mdb_val())

    def __getitem__(self, item):
        if not item:
            raise EmptyKey()
        cdef cppConstIterator it = move(cppConstIterator(self.ptr, PyBufferWrap(make_utf8(item)).get_mdb_val()))
        if it.has_reached_end():
            raise NotFound()
        return self.raw_get_value(it)

    cpdef get(self, item, default=''):
        try:
            return self[item]
        except NotFound:
            return default

    cpdef get_direct(self, item):
        return DirectAccess(self, item)

    cpdef setdefault(self, key, default=''):
        cdef PyBufferWrap key_view = move(PyBufferWrap(make_utf8(key)))
        cdef PyBufferWrap default_view = move(PyBufferWrap(make_utf8(default)))
        cdef CBString ret
        with nogil:
            ret = self.ptr.setdefault(key_view.get_mdb_val(), default_view.get_mdb_val())
        return topy(ret)

    def __setitem__(self, key, value):
        # the PyBufferWrap definitions can't be moved to the "nogil" section
        cdef PyBufferWrap key_view = move(PyBufferWrap(make_utf8(key)))
        cdef PyBufferWrap value_view = move(PyBufferWrap(make_utf8(value)))
        with nogil:
            self.ptr.insert(key_view.get_mdb_val(), value_view.get_mdb_val())

    def __delitem__(self, key):
        cdef PyBufferWrap key_view = move(PyBufferWrap(make_utf8(key)))
        with nogil:
            self.ptr.erase(key_view.get_mdb_val())

    cpdef erase(self, first, last):
        cdef CBString f = tocbstring(make_utf8(first))
        cdef CBString l = tocbstring(make_utf8(last))
        with nogil:
            self.ptr.erase_interval(f, l)

    cpdef noiterkeys(self):
        cdef vector[CBString] k_vec
        with nogil:
            k_vec = self.ptr.get_all_keys()
        return [topy(k) for k in k_vec]

    cpdef noitervalues(self):
        cdef vector[CBString] k_vec
        with nogil:
            k_vec = self.ptr.get_all_values()
        return [topy(k) for k in k_vec]

    cpdef noiteritems(self):
        cdef vector[pair[CBString, CBString]] kv_vec
        with nogil:
            kv_vec = self.ptr.get_all_items()
        return [(topy(p.first), topy(p.second)) for p in kv_vec]

    cpdef pop(self, key, default=None):
        key = make_utf8(key)
        cdef PyBufferWrap key_view = move(PyBufferWrap(key))
        cdef CBString val
        try:
            with nogil:
                val = self.ptr.pop(key_view.get_mdb_val())
        except NotFound:
            if default is None:
                raise
            return default
        return topy(val)

    cpdef popitem(self):
        cdef pair[CBString, CBString] p
        with nogil:
            p = self.ptr.popitem()
        return topy(p.first), topy(p.second)

    def __iter__(self):
        return self.keys()

    def keys(self, reverse=False):
        cdef int keysize = self.ptr.get_maxkeysize()
        cdef cppConstIterator it
        cdef MDB_val k
        cdef bytearray obj = bytearray(keysize)
        cdef PyBufferWrap view = move(PyBufferWrap(obj))
        if reverse:
            it = move(self.ptr.clast())
            while not it.has_reached_beginning():
                yield self.raw_get_key(it)
                it.decr()
        else:
            it = move(self.ptr.cbegin())
            while not it.has_reached_end():
                yield self.raw_get_key(it)
                it.incr()

    cpdef iterkeys(self, reverse=False):
        return self.keys(reverse)

    def values(self, reverse=False):
        cdef cppConstIterator it
        if reverse:
            it = move(self.ptr.clast())
            while not it.has_reached_beginning():
                yield self.raw_get_value(it)
                it.decr()
        else:
            it = move(self.ptr.cbegin())
            while not it.has_reached_end():
                yield self.raw_get_value(it)
                it.incr()

    cpdef itervalues(self, reverse=False):
        return self.values(reverse)

    def items(self, reverse=False):
        cdef cppConstIterator it
        if reverse:
            it = move(self.ptr.clast())
            while not it.has_reached_beginning():
                yield self.raw_get_item(it)
                it.decr()
        else:
            it = move(self.ptr.cbegin())
            while not it.has_reached_end():
                yield self.raw_get_item(it)
                it.incr()

    cpdef iteritems(self, reverse=False):
        return self.items(reverse)

    def __nonzero__(self):
        return self.ptr.is_initialized()

    def __len__(self):
        return self.ptr.size()

    cpdef clear(self):
        self.ptr.clear()

    def __contains__(self, item):
        cdef PyBufferWrap view = move(PyBufferWrap(make_utf8(item)))
        return self.ptr.contains(view.get_mdb_val())

    cpdef has_key(self, key):
        return key in self

    def update(self, e=None, **kwds):
        cdef PyBufferWrap keybuf
        cdef PyBufferWrap valuebuf
        cdef cppIterator it = move(cppIterator(self.ptr, 0, 0))
        if e is not None:
            if hasattr(e, 'keys'):
                for key in e.keys():
                    self.raw_set_item_buf(it, make_utf8(key), make_utf8(e[key]))
            else:
                for (key, value) in e:
                    keybuf = move(PyBufferWrap(make_utf8(key)))
                    valuebuf = move(PyBufferWrap(make_utf8(value)))
                    self.raw_set_item_buf(it, make_utf8(key), make_utf8(value))

        for key in kwds:
            keybuf = move(PyBufferWrap(make_utf8(key)))
            valuebuf = move(PyBufferWrap(make_utf8(kwds[key])))
            self.raw_set_item_buf(it, make_utf8(key), make_utf8(kwds[key]))


    # noinspection PyPep8Naming
    @classmethod
    def fromkeys(cls, dirname, dbname, opts=None, S=None, v=None):
        d = cls(dirname, dbname, opts=opts)
        v = b'' if v is None else make_utf8(v)
        if S is not None:
            d.update((make_utf8(key), v) for key in S)
        return d

    cpdef transform_values(self, binary_funct):
        with nogil:
            self.ptr.transform_values(make_binary_functor(binary_funct))

    cpdef remove_if(self, binary_pred):
        with nogil:
            self.ptr.remove_if(make_binary_predicate(binary_pred))

    cpdef copy_to(self, other, ssize_t chunk_size=-1):
        if not isinstance(other, PRawDict):
            raise TypeError()
        cdef CBString empt
        with nogil:
            self.ptr.copy_to(deref((<PRawDict> other).ptr), empt, empt, chunk_size)

    cpdef move_to(self, other, ssize_t chunk_size=-1):
        if not isinstance(other, PRawDict):
            raise TypeError()
        cdef CBString empt
        with nogil:
            self.ptr.move_to(deref((<PRawDict> other).ptr), empt, empt, chunk_size)


    cpdef remove_duplicates(self, first="", last=""):
        cdef CBString firstkey = tocbstring(make_utf8(first))
        cdef CBString lastkey = tocbstring(make_utf8(last))
        with nogil:
            self.ptr.remove_duplicates(firstkey, lastkey)


cdef class PDict(PRawDict):
    def __cinit__(self, dirname, dbname, LmdbOptions opts=None, mapping=None, Chain key_chain=None, Chain value_chain=None, **kwarg):
        self.key_chain = key_chain if key_chain else Chain(None, None, None)
        self.value_chain = value_chain if value_chain else Chain(PickleSerializer(), None, None)

    def __init__(self, dirname, dbname, LmdbOptions opts=None, mapping=None, Chain key_chain=None, Chain value_chain=None, **kwarg):
        super(PDict, self).__init__(dirname, dbname, opts, mapping, **kwarg)

    def __dealloc__(self):
        pass

    def __repr__(self):
        return u"PDict(dbname='{}', dirname='{}')".format(make_unicode(self.dbname), make_unicode(self.dirname))

    def __reduce__(self):
        return dict, (self.noiteritems(), )

    def __getitem__(self, key):
        cdef cppConstIterator it
        cdef PyBufferWrap key_view = move(PyBufferWrap(self.key_chain.dumps(key)))
        it = move(cppConstIterator(self.ptr, key_view.get_mdb_val()))
        if it.has_reached_end():
            raise NotFound()
        return self.value_chain.loads(self.raw_get_value_buf_const(it))

    cpdef get(self, key, default=None):
        try:
            return self[key]
        except NotFound:
            return default

    cpdef setdefault(self, key, default=None):
        cdef PyBufferWrap key_view = move(PyBufferWrap(self.key_chain.dumps(key)))
        cdef PyBufferWrap default_view = move(PyBufferWrap(self.value_chain.dumps(default)))
        cdef CBString ret
        with nogil:
            ret = self.ptr.setdefault(key_view.get_mdb_val(), default_view.get_mdb_val())
        return self.value_chain.loads(topy(ret))

    def __setitem__(self, key, value):
        cdef cppIterator it = move(cppIterator(self.ptr, 0, 0))
        self.raw_set_item_buf(it, self.key_chain.dumps(key), self.value_chain.dumps(value))

    def __delitem__(self, key):
        cdef PyBufferWrap key_view = move(PyBufferWrap(self.key_chain.dumps(key)))
        with nogil:
            self.ptr.erase(key_view.get_mdb_val())

    cpdef erase(self, first, last):
        raise NotImplementedError()

    cpdef noiterkeys(self):
        cdef vector[CBString] keylist
        with nogil:
            keylist = self.ptr.get_all_keys()
        return [self.key_chain.loads(topy(key)) for key in keylist]

    cpdef noitervalues(self):
        cdef vector[CBString] valuelist
        with nogil:
            valuelist = self.ptr.get_all_values()
        return [self.value_chain.loads(topy(val)) for val in valuelist]

    cpdef noiteritems(self):
        cdef vector[pair[CBString, CBString]] kvlist
        with nogil:
            kvlist = self.ptr.get_all_items()
        return [(self.key_chain.loads(topy(p.first)), self.value_chain.loads(topy(p.second))) for p in kvlist]


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
        return self.value_chain.loads(topy(v))

    cpdef popitem(self):
        cdef pair[CBString, CBString] p
        with nogil:
            p = self.ptr.popitem()
        return self.key_chain.loads(topy(p.first)), self.value_chain.loads(topy(p.second))

    def __iter__(self):
        return self.keys()

    # MBufferIO is used from here to avoid any more copy of bytestring arguments

    def keys(self, reverse=False):
        cdef cppConstIterator it
        if reverse:
            it = move(self.ptr.clast())
            while not it.has_reached_beginning():
                yield self.key_chain.loads(self.raw_get_key_buf_const(it))
                it.decr()
        else:
            it = move(self.ptr.cbegin())
            while not it.has_reached_end():
                yield self.key_chain.loads(self.raw_get_key_buf_const(it))
                it.incr()

    def values(self, reverse=False):
        cdef cppConstIterator it
        if reverse:
            it = move(self.ptr.clast())
            while not it.has_reached_beginning():
                yield self.value_chain.loads(self.raw_get_value_buf_const(it))
                it.decr()
        else:
            it = move(self.ptr.cbegin())
            while not it.has_reached_end():
                yield self.value_chain.loads(self.raw_get_value_buf_const(it))
                it.incr()

    def items(self, reverse=False):
        cdef cppConstIterator it
        if reverse:
            it = move(self.ptr.clast())
            while not it.has_reached_beginning():
                key, val = self.raw_get_item_buf_const(it)
                yield (self.key_chain.loads(key), self.value_chain.loads(val))
                it.decr()
        else:
            it = move(self.ptr.cbegin())
            while not it.has_reached_end():
                key, val = self.raw_get_item_buf_const(it)
                yield (self.key_chain.loads(key), self.value_chain.loads(val))
                it.incr()

    def __contains__(self, key):
        cdef PyBufferWrap key_view = move(PyBufferWrap(self.key_chain.dumps(key)))
        return self.ptr.contains(key_view.get_mdb_val())

    def update(self, e=None, **kwds):
        cdef cppIterator it = move(cppIterator(self.ptr, 0, 0))
        if e is not None:
            if hasattr(e, 'keys'):
                for key in e.keys():
                    self.raw_set_item_buf(it, self.key_chain.dumps(key), self.value_chain.dumps(e[key]))
            else:
                for (key, value) in e:
                    self.raw_set_item_buf(it, self.key_chain.dumps(key), self.value_chain.dumps(value))

        for key in kwds:
            self.raw_set_item_buf(it, self.key_chain.dumps(key), self.value_chain.dumps(kwds[key]))

    # noinspection PyPep8Naming
    @classmethod
    def fromkeys(cls, dirname, dbname, opts=None, S=None, v=None):
        d = cls(dirname, dbname, opts=opts)
        v = b'' if v is None else v
        if S is not None:
            d.update((key, v) for key in S)
        return d

    cpdef transform_values(self, binary_funct):
        binary_funct = _adapt_binary_functor(binary_funct, self.key_chain, self.value_chain)
        with nogil:
            self.ptr.transform_values(make_binary_functor(binary_funct))

    cpdef remove_if(self, binary_pred):
        binary_pred = _adapt_binary_predicate(binary_pred, self.key_chain, self.value_chain)
        with nogil:
            self.ptr.remove_if(make_binary_predicate(binary_pred))

    cpdef copy_to(self, other, ssize_t chunk_size=-1):
        if not isinstance(other, PDict):
            raise TypeError()
        cdef CBString empt
        with nogil:
            self.ptr.copy_to(deref((<PDict> other).ptr), empt, empt, chunk_size)

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

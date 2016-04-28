# -*- coding: utf-8 -*-

# noinspection PyPep8Naming
cdef class PRawDictAbstractIterator(object):
    def __init__(self, PRawDict d, int pos=0, key=None):
        self.dict = d
        self.pos = pos
        self.key = None
        if key is not None:
            self.key = d.key_chain.dumps(key)
            if len(self.key) == 0:
                raise EmptyKey()
            if len(self.key) > 511:
                raise BadValSize("key is too long")

    def start(self):
        raise NotImplementedError()

    def stop(self):
        with nogil:
            self.cpp_iterator_ptr.reset()

    def __enter__(self):
        return self.start()

    def __dealloc__(self):
        self.stop()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    cdef cpp_bool has_reached_end(self):
        return self.cpp_iterator_ptr.get().has_reached_end()

    cdef cpp_bool has_reached_beginning(self):
        return self.cpp_iterator_ptr.get().has_reached_beginning()

    cdef incr(self):    # calls lock_guard -> better to call 'incr' in nogil section
        self.cpp_iterator_ptr.get().abs_incr()

    cdef decr(self):
        self.cpp_iterator_ptr.get().abs_decr()

    cdef get_key_buf(self, cpp_bool incr=0, cpp_bool decr=0):
        cdef MDB_val k
        cdef void* ptr
        with nogil:
            k = self.cpp_iterator_ptr.get().get_key_buffer()
            ptr = copy_mdb_val(k)
            if incr:
                self.cpp_iterator_ptr.get().abs_incr()
            if decr:
                self.cpp_iterator_ptr.get().abs_decr()
        return self.dict.key_chain.loads(make_mbufferio(ptr, k.mv_size, 1))

    cdef get_value_buf(self, cpp_bool incr=0, cpp_bool decr=0):
        cdef MDB_val v
        cdef void* ptr
        with nogil:
            v = self.cpp_iterator_ptr.get().get_value_buffer()
            ptr = copy_mdb_val(v)
            if incr:
                self.cpp_iterator_ptr.get().abs_incr()
            if decr:
                self.cpp_iterator_ptr.get().abs_decr()
        return self.dict.value_chain.loads(make_mbufferio(ptr, v.mv_size, 1))

    cdef get_item_buf(self, cpp_bool incr=0, cpp_bool decr=0):
        cdef pair[MDB_val, MDB_val] kv
        cdef void* key_ptr
        cdef void* value_ptr
        with nogil:
            kv = self.cpp_iterator_ptr.get().get_item_buffer()
            key_ptr = copy_mdb_val(kv.first)
            value_ptr = copy_mdb_val(kv.second)
            if incr:
                self.cpp_iterator_ptr.get().abs_incr()
            if decr:
                self.cpp_iterator_ptr.get().abs_decr()
        return self.dict.key_chain.loads(make_mbufferio(key_ptr, kv.first.mv_size, 1)), self.dict.value_chain.loads(make_mbufferio(value_ptr, kv.second.mv_size, 1))

    def __getitem__(self, item):
        cdef PyBufferWrap key_view = move(PyBufferWrap(self.dict.key_chain.dumps(item)))
        if key_view.length() == 0:
            raise EmptyKey()
        if key_view.length() > 511:
            raise BadValSize("key is too long")
        self.cpp_iterator_ptr.get().set_position(key_view.get_mdb_val())
        return self.get_value_buf()

# noinspection PyPep8Naming
cdef class PRawDictConstIterator(PRawDictAbstractIterator):
    def start(self):
        if self.key is None:
            self.cpp_iterator_ptr.reset(new cppConstIterator(self.dict.ptr, self.pos))
        else:
            self.cpp_iterator_ptr.reset(new cppConstIterator(self.dict.ptr, PyBufferWrap(self.key).get_mdb_val()))
        return self


# noinspection PyPep8Naming
cdef class PRawDictIterator(PRawDictAbstractIterator):
    def start(self):
        # generates a LMDB write transaction -> it can block
        cdef PyBufferWrap key_view
        if self.key is None:
            with nogil:
                self.cpp_iterator_ptr.reset(new cppIterator(self.dict.ptr, self.pos, 0))
        else:
            key_view = move(PyBufferWrap(self.key))
            with nogil:
                self.cpp_iterator_ptr.reset(new cppIterator(self.dict.ptr, key_view.get_mdb_val(), 0))
        return self

    cdef set_rollback(self):
        self.cpp_iterator_ptr.get().set_rollback(1)

    cdef set_item_buf(self, k, v):
        cdef PyBufferWrap key_view = move(PyBufferWrap(self.dict.key_chain.dumps(k)))
        if key_view.length() == 0:
            raise EmptyKey()
        if key_view.length() > 511:
            raise BadValSize("key is too long")
        cdef PyBufferWrap value_view = move(PyBufferWrap(self.dict.value_chain.dumps(v)))
        cdef cppIterator* it_ptr = <cppIterator*> self.cpp_iterator_ptr.get()
        with nogil:
            it_ptr.set_key_value(key_view.get_mdb_val(), value_view.get_mdb_val())

    cdef dlte(self, key=None):
        cdef PyBufferWrap key_view
        cdef cppIterator* it_ptr = <cppIterator*> self.cpp_iterator_ptr.get()
        cdef cpp_bool result
        if key is None:
            result = it_ptr.dlte()
        else:
            key_view = move(PyBufferWrap(self.dict.key_chain.dumps(key)))
            with nogil:
                result = it_ptr.dlte(key_view.get_mdb_val())
        if not result:
            raise NotFound()

    def __setitem__(self, key, value):
        if not self.cpp_iterator_ptr.get():
            raise NotInitialized()
        self.set_item_buf(key, value)

    def __delitem__(self, key):
        if not self.cpp_iterator_ptr.get():
            raise NotInitialized()
        cdef PyBufferWrap key_view = move(PyBufferWrap(self.dict.key_chain.dumps(key)))
        if key_view.length() == 0:
            raise EmptyKey()
        if key_view.length() > 511:
            raise BadValSize("key is too long")
        self.dlte(key)

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
        if opts is None:
            opts = LmdbOptions()
        self.ptr = dict_factory(tocbstring(dirname), tocbstring(dbname), (<LmdbOptions> opts).opts)
        self.rmrf_at_delete = 0
        self.key_chain = NoneChain()
        self.value_chain = NoneChain()

    def __init__(self, bytes dirname, bytes dbname, LmdbOptions opts=None, mapping=None, Chain key_chain=None, Chain value_chain=None, **kwarg):
        if mapping is not None:
            self.update(e=mapping)
        if kwarg:
            self.update(e=kwarg)

    def __dealloc__(self):
        if self.ptr.get():
            if self.rmrf_at_delete:
                shutil.rmtree(self.dirname)
                self.rmrf_at_delete = 0

            with nogil:
                self.ptr.reset()

    def __repr__(self):
        return u"PRawDict(dbname='{}', dirname='{}')".format(
            make_unicode(self.dbname), make_unicode(self.dirname)
        )

    @property
    def options(self):
        return LmdbOptions.from_cpp(self.ptr.get().get_options())

    @classmethod
    def make_temp(cls, destroy=True, LmdbOptions opts=None, Chain key_chain=None, Chain value_chain=None):
        cdef shared_ptr[TempDirectory] temp_dir_ptr = make_temp_directory(True, False)
        d = cls(dirname=topy(temp_dir_ptr.get().get_path()), dbname=bytes(uuid.uuid1()), opts=opts, key_chain=key_chain, value_chain=value_chain)
        (<PRawDict>d).rmrf_at_delete = bool(destroy)
        return d

    def __reduce__(self):
        return dict, (self.noiteritems(), )

    def __richcmp__(self, other, op):
        if op == 2:
            if not isinstance(other, PRawDict):
                return False
            if deref((<PRawDict> self).ptr) != deref((<PRawDict> other).ptr):
                return False
            return (<PRawDict> self).key_chain == (<PRawDict> other).key_chain and (<PRawDict> self).value_chain == (<PRawDict> other).value_chain
        elif op == 3:
            if not isinstance(other, PRawDict):
                return True
            if deref((<PRawDict> self).ptr) != deref((<PRawDict> other).ptr):
                return True
            return (<PRawDict> self).key_chain != (<PRawDict> other).key_chain or (<PRawDict> self).value_chain != (<PRawDict> other).value_chain
        raise ValueError("unsupported comparison")

    property dirname:
        def __get__(self):
            return topy(self.ptr.get().get_dirname())

    property dbname:
        def __get__(self):
            return topy(self.ptr.get().get_dbname())

    def __getitem__(self, item):
        cdef PRawDictConstIterator it = PRawDictConstIterator(self, key=item)
        with it:
            if it.has_reached_end():
                raise NotFound()
            return it.get_value_buf()

    cpdef get(self, item, default=b''):
        try:
            return self[item]
        except NotFound:
            return default if default else b''

    cpdef get_direct(self, item):
        if not item:
            raise EmptyKey()
        return DirectAccess(self, item)

    cpdef setdefault(self, key, default=b''):
        cdef PyBufferWrap key_view = move(PyBufferWrap(self.key_chain.dumps(key)))
        if key_view.length() == 0:
            raise EmptyKey()
        if key_view.length() > 511:
            raise BadValSize("key is too long")
        cdef PyBufferWrap default_view = move(PyBufferWrap(self.value_chain.dumps(default)))
        cdef CBString ret
        with nogil:
            ret = self.ptr.get().setdefault(key_view.get_mdb_val(), default_view.get_mdb_val())
        return self.value_chain.loads(topy(ret))

    def __setitem__(self, key, value):
        cdef PRawDictIterator it = PRawDictIterator(self)
        with it:
            it.set_item_buf(key, value)

    def __delitem__(self, key):
        cdef PRawDictIterator it = PRawDictIterator(self, key=key)
        cdef cpp_bool result
        with it:
            it.dlte()

    cpdef erase(self, first, last):
        cdef CBString f = tocbstring(first)
        cdef CBString l = tocbstring(last)
        with nogil:
            self.ptr.get().erase_interval(f, l)

    cpdef noiterkeys(self):
        return list(self.keys())

    cpdef noitervalues(self):
        return list(self.values())

    cpdef noiteritems(self):
        return list(self.items())

    cpdef pop(self, key, default=None):
        cdef PyBufferWrap key_view = move(PyBufferWrap(self.key_chain.dumps(key)))
        if key_view.length() == 0:
            raise EmptyKey()
        if key_view.length() > 511:
            raise BadValSize("key is too long")
        cdef CBString v
        try:
            with nogil:
                v = self.ptr.get().pop(key_view.get_mdb_val())
        except NotFound:
            if default is None:
                raise
            return default
        return self.value_chain.loads(make_mbufferio_from_cbstring(v))

    cpdef popitem(self):
        cdef pair[CBString, CBString] p
        with nogil:
            p = self.ptr.get().popitem()
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
                    yield it.get_key_buf(0, 1)
                    #it.decr()
        else:
            it = PRawDictConstIterator(self)
            with it:
                while not it.has_reached_end():
                    yield it.get_key_buf(1)
                    #it.incr()

    cpdef iterkeys(self, reverse=False):
        return self.keys(reverse)

    def values(self, reverse=False):
        cdef PRawDictConstIterator it
        if reverse:
            it = PRawDictConstIterator(self, pos=1)
            with it:
                it.decr()
                while not it.has_reached_beginning():
                    yield it.get_value_buf(0, 1)
                    #it.decr()
        else:
            it = PRawDictConstIterator(self)
            with it:
                while not it.has_reached_end():
                    yield it.get_value_buf(1)
                    #it.incr()

    cpdef itervalues(self, reverse=False):
        return self.values(reverse)

    def items(self, reverse=False):
        cdef PRawDictConstIterator it
        if reverse:
            it = PRawDictConstIterator(self, pos=1)
            with it:
                it.decr()
                while not it.has_reached_beginning():
                    yield it.get_item_buf(0, 1)
                    #it.decr()
        else:
            it = PRawDictConstIterator(self)
            with it:
                while not it.has_reached_end():
                    yield it.get_item_buf(1)
                    #it.incr()

    cpdef iteritems(self, reverse=False):
        return self.items(reverse)

    def __nonzero__(self):
        return self.ptr.get().is_initialized()

    def __len__(self):
        return self.ptr.get().size()

    cpdef clear(self):
        self.ptr.get().clear()

    def __contains__(self, key):
        cdef PyBufferWrap key_view = move(PyBufferWrap(self.key_chain.dumps(key)))
        if key_view.length() == 0:
            raise EmptyKey()
        if key_view.length() > 511:
            raise BadValSize("key is too long")
        return self.ptr.get().contains(key_view.get_mdb_val())

    cpdef has_key(self, key):
        return key in self

    def write_batch(self):
        return PRawDictIterator(self)

    def read_transaction(self):
        return PRawDictConstIterator(self)

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
        v = b'' if not v else v
        if S:
            d.update((key, v) for key in S)
        return d

    cpdef transform_values(self, binary_funct):
        with nogil:
            self.ptr.get().transform_values(make_binary_scalar_functor(binary_funct))

    cpdef remove_if(self, binary_pred):
        with nogil:
            self.ptr.get().remove_if(make_binary_predicate(binary_pred))

    cpdef move_to(self, PRawDict other, ssize_t chunk_size=-1):
        cdef CBString empt
        with nogil:
            self.ptr.get().move_to(other.ptr, empt, empt, chunk_size)

    cpdef remove_duplicates(self, first="", last=""):
        cdef CBString firstkey = tocbstring(first)
        cdef CBString lastkey = tocbstring(last)
        with nogil:
            self.ptr.get().remove_duplicates(firstkey, lastkey)


cdef class PDict(PRawDict):
    def __cinit__(self, bytes dirname, bytes dbname, LmdbOptions opts=None, mapping=None, Chain key_chain=None, Chain value_chain=None, **kwarg):
        self.key_chain = key_chain if key_chain else NoneChain()
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
            self.ptr.get().transform_values(make_binary_scalar_functor(binary_funct))

    cpdef remove_if(self, binary_pred):
        binary_pred = _adapt_binary_predicate(binary_pred, self.key_chain, self.value_chain)
        with nogil:
            self.ptr.get().remove_if(make_binary_predicate(binary_pred))

    cpdef remove_duplicates(self, first="", last=""):
        with nogil:
            self.ptr.get().remove_duplicates()


collections.MutableMapping.register(PRawDict)
collections.MutableMapping.register(PDict)

# noinspection PyPep8Naming
cdef class NonBlockingPDictWrapper(object):
    def __cinit__(self, PRawDict pdict, int how_many_readers=1):
        pass

    def __init__(self, PRawDict pdict, int how_many_readers=1):
        if how_many_readers <= 0:
            raise ValueError()
        self.d = pdict
        self.n_readers = how_many_readers
        self.stopping = threading.Event()
        self.starting = threading.Event()
        self.readers_threads = []
        self.writer_thread = None

    cpdef start(self):
        if self.writer_thread is not None or self.starting.is_set() or self.stopping.is_set():
            return
        self.starting.set()

        for _ in range(self.n_readers):
            self.readers_threads.append(threading.Thread(target=self._reader_thread_fun))

        self.writer_thread = threading.Thread(target=self._writer_thread_fun)

        for t in self.readers_threads:
            t.daemon = True
            t.start()

        self.writer_thread.daemon = True
        self.writer_thread.start()
        self.starting.clear()

    cpdef stop(self):
        if self.writer_thread is None or self.starting.is_set() or self.stopping.is_set():
            return
        self.stopping.set()
        self.write_operations_queue.py_push(b"STOP")
        for t in self.readers_threads:
            self.read_operations_queue.py_push(b"STOP")
        self.writer_thread.join()
        for t in self.readers_threads:
            t.join()
        self.readers_threads = []
        self.writer_thread = None
        self.stopping.clear()

    def __enter__(self):
        self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def __dealloc__(self):
        self.stop()

    def _reader_thread_fun(self):
        cdef PyObject* operation
        op = None
        while not self.stopping.is_set():
            op = self.read_operations_queue.wait_and_pop(1)
            if op and op != b"STOP":
                self._do_operation(op)
        sleep(0.25)
        while True:
            op = self.read_operations_queue.try_pop()
            if not op:
                break
            if op != b"STOP":
              self._do_operation(op)

    def _writer_thread_fun(self):
        cdef PyObject* operation
        op = None
        while not self.stopping.is_set():
            op = self.write_operations_queue.wait_and_pop(1)
            if op and op != b"STOP":
                self._do_operation(op)
        while True:
            op = self.write_operations_queue.try_pop()
            if op is None:
                break
            if op != b"STOP":
                self._do_operation(op)

    cdef _do_operation(self, op):
        cdef PRawDictConstIterator iterator
        op_type = op[0]
        f = op[1]
        if not f.set_running_or_notify_cancel():
            return

        if op_type == b"getitem":
            iterator = op[2]
            _, _, _, item = op
            try:
                f.set_result(iterator[item])
            except Exception as ex:
                f.set_exception(ex)

        elif op_type == b"setitem":
            _, _, key, value = op
            try:
                self.d[key] = value
                f.set_result(True)
            except Exception as ex:
                f.set_exception(ex)

        elif op_type == b"get":
            iterator = op[2]
            _, _, _, item, default = op
            try:
                f.set_result(iterator[item])
            except NotFound:
                f.set_result(default)
            except Exception as ex:
                f.set_exception(ex)

        elif op_type == b"keys":
            iterator = op[2]
            l = []
            try:
                while not iterator.has_reached_end():
                    l.append(iterator.get_key_buf(1))
                f.set_result(l)
            except Exception as ex:
                f.set_exception(ex)

        elif op_type == b"values":
            iterator = op[2]
            l = []
            try:
                while not iterator.has_reached_end():
                    l.append(iterator.get_value_buf(1))
                f.set_result(l)
            except Exception as ex:
                f.set_exception(ex)

        elif op_type == b"items":
            iterator = op[2]
            l = []
            try:
                while not iterator.has_reached_end():
                    l.append(iterator.get_item_buf(1))
                f.set_result(l)
            except Exception as ex:
                f.set_exception(ex)

        elif op_type == b"update":
            _, _, e, kwds = op
            try:
                self.d.update(e, **kwds)
            except Exception as ex:
                f.set_exception(ex)

        elif op_type == b"pop":
            _, _, key, default = op
            try:
                f.set_result(self.d.pop(key, default))
            except Exception as ex:
                f.set_exception(ex)

        elif op_type == b"popitem":
            try:
                f.set_result(self.d.popitem())
            except Exception as ex:
                f.set_exception(ex)

        elif op_type == b"transform_values":
            _, _, binary_funct = op
            try:
                self.d.transform_values(binary_funct)
                f.set_result(True)
            except Exception as ex:
                f.set_exception(ex)

        elif op_type == b"remove_if":
            _, _, binary_pred = op
            try:
                self.d.remove_if(binary_pred)
                f.set_result(True)
            except Exception as ex:
                f.set_exception(ex)

        elif op_type == b"remove_duplicates":
            try:
                self.d.remove_duplicates()
                f.set_result(True)
            except Exception as ex:
                f.set_exception(ex)

        else:
            f.set_exception(ValueError("unknown operation"))

    cpdef setitem(self, key, value):
        if self.stopping.is_set():
            raise RuntimeError('cannot schedule new futures after shutdown')
        f = Future()
        self.write_operations_queue.push((b"setitem", f, key, value))
        return f

    cpdef getitem(self, item):
        if self.stopping.is_set():
            raise RuntimeError('cannot schedule new futures after shutdown')
        f = Future()
        self.read_operations_queue.push((b"getitem", f, self.d.read_transaction().start(), item))
        return f

    cpdef get(self, item, default=None):
        if self.stopping.is_set():
            raise RuntimeError('cannot schedule new futures after shutdown')
        f = Future()
        self.read_operations_queue.push((b"get", f, self.d.read_transaction().start(), item, default))
        return f

    cpdef keys(self):
        if self.stopping.is_set():
            raise RuntimeError('cannot schedule new futures after shutdown')
        f = Future()
        self.read_operations_queue.push((b"keys", f, self.d.read_transaction().start()))
        return f

    cpdef values(self):
        if self.stopping.is_set():
            raise RuntimeError('cannot schedule new futures after shutdown')
        f = Future()
        self.read_operations_queue.push((b"values", f, self.d.read_transaction().start()))
        return f

    cpdef items(self):
        if self.stopping.is_set():
            raise RuntimeError('cannot schedule new futures after shutdown')
        f = Future()
        self.read_operations_queue.push((b"items", f, self.d.read_transaction().start()))
        return f

    def update(self, e=None, **kwds):
        if self.stopping.is_set():
            raise RuntimeError('cannot schedule new futures after shutdown')
        f = Future()
        self.write_operations_queue.push((b"update", f, e, kwds))
        return f

    cpdef pop(self, key, default=None):
        if self.stopping.is_set():
            raise RuntimeError('cannot schedule new futures after shutdown')
        f = Future()
        self.write_operations_queue.push((b"pop", f, key, default))
        return f

    cpdef popitem(self):
        if self.stopping.is_set():
            raise RuntimeError('cannot schedule new futures after shutdown')
        f = Future()
        self.write_operations_queue.push((b"popitem", f))
        return f

    cpdef transform_values(self, binary_funct):
        if self.stopping.is_set():
            raise RuntimeError('cannot schedule new futures after shutdown')
        f = Future()
        self.write_operations_queue.push((b"transform_values", f))
        return f

    cpdef remove_if(self, binary_pred):
        if self.stopping.is_set():
            raise RuntimeError('cannot schedule new futures after shutdown')
        f = Future()
        self.write_operations_queue.push((b"remove_if", f, binary_pred))
        return f

    cpdef remove_duplicates(self):
        if self.stopping.is_set():
            raise RuntimeError('cannot schedule new futures after shutdown')
        f = Future()
        self.write_operations_queue.push((b"remove_duplicates", f))
        return f


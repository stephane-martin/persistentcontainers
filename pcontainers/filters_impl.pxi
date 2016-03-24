

cdef class Filter(object):
    cpdef dumps(self, obj):
        raise NotImplementedError()
    cpdef loads(self, obj):
        raise NotImplementedError()

# noinspection PyAbstractClass
cdef class Serializer(Filter):
    pass


cdef class NoneSerializer(Serializer):
    cpdef dumps(self, obj):
        if not isinstance(obj, MBufferIO):
            obj = MBufferIO(obj)
        obj.seek(0)
        return obj

    cpdef loads(self, obj):
        if isinstance(obj, MBufferIO):
            return obj.tobytes()
        return obj


cdef class PickleSerializer(Serializer):
    def __init__(self, protocol=2):
        if self.pickle_module is None:
            try:
                # noinspection PyPep8Naming
                import cPickle as pickle
            except ImportError:
                import pickle
            self.pickle_module = pickle
        self.protocol = protocol

    cpdef dumps(self, obj):
        buf = MBufferIO()
        self.pickle_module.dump(obj, buf, protocol=self.protocol)
        buf.seek(0)
        return buf

    cpdef loads(self, obj):
        if not isinstance(obj, MBufferIO):
            obj = MBufferIO(obj)
        obj.seek(0)
        return self.pickle_module.load(obj)


cdef class MessagePackSerializer(Serializer):
    def __init__(self, unicode_errors='strict', use_single_float=False, use_list=True, default=None, ext_hook=None):
        if self.messagepack_module is None:
            import msgpack
            self.messagepack_module = msgpack

        self.mpack_args = {
            'unicode_errors': unicode_errors, 'use_single_float': use_single_float,
            'encoding': 'utf-8', 'use_bin_type': True, 'default': default
        }
        self.use_list = bool(use_list)
        self.ext_hook = ext_hook if ext_hook else self.messagepack_module.ExtType

    cpdef dumps(self, obj):
        buf = MBufferIO()
        self.messagepack_module.pack(obj, buf, **self.mpack_args)
        buf.seek(0)
        return buf

    cpdef loads(self, obj):
        if not isinstance(obj, MBufferIO):
            obj = MBufferIO(obj)
        obj.seek(0)
        return self.messagepack_module.unpack(
            obj, use_list=self.use_list, encoding="utf-8", unicode_errors=self.mpack_args['unicode_errors'],
            ext_hook=self.ext_hook
        )

cdef class JsonSerializer(Serializer):

    def __init__(self, default=None):
        if self.json_module is None:
            try:
                # noinspection PyPackageRequirements
                import simplejson as json
            except ImportError:
                import json
            self.json_module = json
            self.default = default

    cpdef dumps(self, obj):
        buf = MBufferIO()
        self.json_module.dump(obj, buf, skipkeys=False, ensure_ascii=True, check_circular=True, allow_nan=True, indent=None,
                              separators=(',', ':'), encoding='utf-8', default=self.default)
        buf.seek(0)
        return buf

    cpdef loads(self, obj):
        if not isinstance(obj, MBufferIO):
            obj = MBufferIO(obj)
        obj.seek(0)
        return self.json_module.load(obj, encoding='utf-8')


# noinspection PyAbstractClass
cdef class Signer(Filter):
    def __init__(self, bytes secret=b''):
        self.secret = secret


cdef class NoneSigner(Signer):
    cpdef dumps(self, obj):
        if not isinstance(obj, MBufferIO):
            obj = MBufferIO(obj)
        obj.seek(0)
        return obj
    cpdef loads(self, obj):
        if not isinstance(obj, MBufferIO):
            obj = MBufferIO(obj)
        obj.seek(0)
        return obj


cdef class HMACSigner(Signer):
    def __init__(self, secret):
        super(HMACSigner, self).__init__(secret)
        if self.hmac_module is None:
            import hmac
            self.hmac_module = hmac
        if self.hashlib_module is None:
            import hashlib
            self.hashlib_module = hashlib

    cpdef dumps(self, obj):
        if not isinstance(obj, MBufferIO):
            obj = MBufferIO(obj)
        if self.secret:
            h = self.hmac_module.new(self.secret, obj, self.hashlib_module.sha256)
            obj.seek(0, 2)  # pos at end
            obj.write(h.digest())
        obj.seek(0)
        return obj

    cpdef loads(self, obj):
        if not isinstance(obj, MBufferIO):
            obj = MBufferIO(obj)
        if self.secret:
            digest = obj[-32:].tobytes()    # read last 32 bytes
            h = self.hmac_module.new(self.secret, obj[0:-32], self.hashlib_module.sha256).digest()  # forget the last 32 bytes and make hash
            if not self.hmac_module.compare_digest(digest, h):
                raise ValueError("HMAC check failed : data has been tampered ?")
            return obj[0:-32]
        obj.seek(0)
        return obj

# noinspection PyAbstractClass
cdef class Compresser(Filter):
    pass


cdef class NoneCompresser(Compresser):
    cpdef dumps(self, obj):
        return obj
    cpdef loads(self, obj):
        return obj


cdef class SnappyCompresser(Compresser):
    def __init__(self):
        if self.snappyx_module is None:
            # noinspection PyPackageRequirements
            import snappyx
            self.snappyx_module = snappyx
    cpdef dumps(self, obj):
        return MBufferIO(self.snappyx_module.compress(obj))
    cpdef loads(self, obj):
        return MBufferIO(self.snappyx_module.decompress(obj))


cdef class LZ4Compresser(Compresser):
    def __init__(self, level=None, block_size_id=None, block_mode_linked=True, checksum=True):
        if self.lz4_module is None:
            import lz4framed
            self.lz4_module = lz4framed
            self.blocksize_default = lz4framed.LZ4F_BLOCKSIZE_DEFAULT
            self.compression_min = lz4framed.LZ4F_COMPRESSION_MIN
        self.level = level if level is not None else self.compression_min
        self.block_size_id = block_size_id if block_size_id is not None else self.blocksize_default
        self.block_mode_linked = bool(block_mode_linked)
        self.checksum = bool(checksum)

    cpdef dumps(self, obj):
        if not isinstance(obj, MBufferIO):
            obj = MBufferIO(obj)
        compressed = MBufferIO()
        with self.lz4_module.Compressor(compressed, block_size_id=self.block_size_id, block_mode_linked=self.block_mode_linked,
                                        checksum=self.checksum, autoflush=True, level=self.level) as c:
            try:
                while True:
                    c.update(obj.read(65536))
            except self.lz4_module.Lz4FramedNoDataError:
                pass
        compressed.seek(0)
        return compressed

    cpdef loads(self, obj):
        if not isinstance(obj, MBufferIO):
            obj = MBufferIO(obj)
        decompressed = MBufferIO()
        for chunk in self.lz4_module.Decompressor(obj):
           decompressed.write(chunk)
        decompressed.seek(0)
        return decompressed


cdef class Chain(Filter):
    def __init__(self, Serializer serializer=None, Signer signer=None, Compresser compresser=None):
        self.serializer = serializer if serializer else NoneSerializer()
        self.signer = signer if signer else NoneSigner()
        self.compresser = compresser if compresser else NoneCompresser()

    cpdef dumps(self, obj):
        return self.signer.dumps(self.compresser.dumps(self.serializer.dumps(obj)))

    cpdef loads(self, obj):
        return self.serializer.loads(self.compresser.loads(self.signer.loads(obj)))


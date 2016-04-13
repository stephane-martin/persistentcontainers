

cdef class Filter(object):
    cdef dumps(self, obj):
        raise NotImplementedError()
    cdef loads(self, obj):
        raise NotImplementedError()
    cpdef pydumps(self, obj):
        return self.dumps(obj)
    cpdef pyloads(self, obj):
        return self.loads(obj)


# noinspection PyAbstractClass
cdef class Serializer(Filter):
    pass


cdef class NoneSerializer(Serializer):
    cdef dumps(self, obj):
        if isinstance(obj, Number):
            obj = bytes(obj)
        if PyUnicode_Check(obj):
            obj = PyUnicode_AsUTF8String(obj)
        if not isinstance(obj, MBufferIO):
            obj = MBufferIO(obj)
        obj.seek(0)
        return obj

    def __richcmp__(self, other, op):
        if op == 2:
            return isinstance(other, NoneSerializer)
        elif op == 3:
            return not isinstance(other, NoneSerializer)
        else:
            raise ValueError("unsupported operation")

cdef class PickleSerializer(Serializer):
    def __init__(self, int protocol=2):
        if self.pickle_module is None:
            try:
                # noinspection PyPep8Naming
                import cPickle as pickle
            except ImportError:
                import pickle
            self.pickle_module = pickle
        self.protocol = protocol

    cdef dumps(self, obj):
        buf = MBufferIO()
        self.pickle_module.dump(obj, buf, protocol=self.protocol)
        buf.seek(0)
        return buf

    cdef loads(self, obj):
        if not isinstance(obj, MBufferIO):
            obj = MBufferIO(obj)
        obj.seek(0)
        return self.pickle_module.load(obj)

    def __richcmp__(self, other, op):
        if op == 2:
            if not isinstance(other, PickleSerializer):
                return False
            return (<PickleSerializer> self).protocol == (<PickleSerializer> other).protocol
        elif op == 3:
            if isinstance(other, PickleSerializer):
                return (<PickleSerializer> self).protocol != (<PickleSerializer> other).protocol
            return True
        else:
            raise ValueError("unsupported operation")

cdef class MessagePackSerializer(Serializer):
    def __init__(self, unicode_errors='strict', use_single_float=False, use_list=True, default=None, ext_hook=None):
        if self.messagepack_module is None:
            import msgpack
            self.messagepack_module = msgpack

        self.mpack_args = {
            'unicode_errors': str(unicode_errors), 'use_single_float': bool(use_single_float),
            'encoding': 'utf-8', 'use_bin_type': True, 'default': default
        }
        self.use_list = bool(use_list)
        self.ext_hook = ext_hook if ext_hook else self.messagepack_module.ExtType

    cdef dumps(self, obj):
        buf = MBufferIO()
        self.messagepack_module.pack(obj, buf, **self.mpack_args)
        buf.seek(0)
        return buf

    cdef loads(self, obj):
        if not isinstance(obj, MBufferIO):
            obj = MBufferIO(obj)
        obj.seek(0)
        return self.messagepack_module.unpack(
            obj, use_list=self.use_list, encoding="utf-8", unicode_errors=self.mpack_args['unicode_errors'],
            ext_hook=self.ext_hook
        )

    def __richcmp__(self, other, op):
        if op == 2:
            if not isinstance(other, MessagePackSerializer):
                return False
            return (<MessagePackSerializer> self).mpack_args == (<MessagePackSerializer> other).mpack_args and \
                (<MessagePackSerializer> self).use_list == (<MessagePackSerializer> other).use_list and \
                (<MessagePackSerializer> self).ext_hook == (<MessagePackSerializer> other).ext_hook
        elif op == 3:
            if isinstance(other, MessagePackSerializer):
                return (<MessagePackSerializer> self).mpack_args != (<MessagePackSerializer> other).mpack_args or \
                    (<MessagePackSerializer> self).use_list != (<MessagePackSerializer> other).use_list or \
                    (<MessagePackSerializer> self).ext_hook != (<MessagePackSerializer> other).ext_hook
            return True
        else:
            raise ValueError("unsupported operation")

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

    cdef dumps(self, obj):
        buf = MBufferIO()
        self.json_module.dump(obj, buf, skipkeys=False, ensure_ascii=True, check_circular=True, allow_nan=True, indent=None,
                              separators=(',', ':'), encoding='utf-8', default=self.default)
        buf.seek(0)
        return buf

    cdef loads(self, obj):
        if not isinstance(obj, MBufferIO):
            obj = MBufferIO(obj)
        obj.seek(0)
        return self.json_module.load(obj, encoding='utf-8')

    def __richcmp__(self, other, op):
        if op == 2:
            if not isinstance(other, JsonSerializer):
                return False
            return (<JsonSerializer> self).default == (<JsonSerializer> other).default
        elif op == 3:
            if isinstance(other, JsonSerializer):
                return (<JsonSerializer> self).default != (<JsonSerializer> other).default
            return True
        else:
            raise ValueError("unsupported operation")


# noinspection PyAbstractClass
cdef class Signer(Filter):
    def __init__(self, bytes secret=b''):
        self.secret = secret

cdef class NoneSigner(Signer):
    cdef dumps(self, obj):
        if not isinstance(obj, MBufferIO):
            obj = MBufferIO(obj)
        obj.seek(0)
        return obj

    cdef loads(self, obj):
        if not isinstance(obj, MBufferIO):
            obj = MBufferIO(obj)
        obj.seek(0)
        return obj

    def __richcmp__(self, other, op):
        if op == 2:
            return isinstance(other, NoneSigner)
        elif op == 3:
            return not isinstance(other, NoneSigner)
        else:
            raise ValueError("unsupported operation")

cdef class HMACSigner(Signer):
    def __init__(self, secret):
        super(HMACSigner, self).__init__(secret)
        if self.hmac_module is None:
            import hmac
            self.hmac_module = hmac
        if self.hashlib_module is None:
            import hashlib
            self.hashlib_module = hashlib

    cdef dumps(self, obj):
        if not isinstance(obj, MBufferIO):
            obj = MBufferIO(obj)
        if self.secret:
            h = self.hmac_module.new(self.secret, obj, self.hashlib_module.sha256)
            obj.seek(0, 2)  # pos at end
            obj.write(h.digest())
        obj.seek(0)
        return obj

    cdef loads(self, obj):
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

    def __richcmp__(self, other, op):
        if op == 2:
            if not isinstance(other, HMACSigner):
                return False
            return (<HMACSigner> self).secret == (<HMACSigner> other).secret
        elif op == 3:
            if isinstance(other, HMACSigner):
                return (<HMACSigner> self).secret != (<HMACSigner> other).secret
            return True
        else:
            raise ValueError("unsupported operation")

# noinspection PyAbstractClass
cdef class Compresser(Filter):
    pass


cdef class NoneCompresser(Compresser):
    cdef dumps(self, obj):
        return obj
    cdef loads(self, obj):
        return obj

    def __richcmp__(self, other, op):
        if op == 2:
            return isinstance(other, NoneCompresser)
        elif op == 3:
            return not isinstance(other, NoneCompresser)
        else:
            raise ValueError("unsupported operation")

cdef class SnappyCompresser(Compresser):
    def __init__(self):
        if self.snappyx_module is None:
            # noinspection PyPackageRequirements
            import snappyx
            self.snappyx_module = snappyx
    cdef dumps(self, obj):
        return MBufferIO(self.snappyx_module.compress(obj))
    cdef loads(self, obj):
        return MBufferIO(self.snappyx_module.decompress(obj))

    def __richcmp__(self, other, op):
        if op == 2:
            return isinstance(other, SnappyCompresser)
        elif op == 3:
            return not isinstance(other, SnappyCompresser)
        else:
            raise ValueError("unsupported operation")

cdef class LZ4Compresser(Compresser):
    def __init__(self, level=None, block_size_id=None, block_mode_linked=True, checksum=True):
        if self.lz4_module is None:
            import lz4framed
            self.lz4_module = lz4framed
            self.blocksize_default = lz4framed.LZ4F_BLOCKSIZE_DEFAULT
            self.compression_min = lz4framed.LZ4F_COMPRESSION_MIN
        self.level = int(level) if level is not None else self.compression_min
        self.block_size_id = int(block_size_id) if block_size_id is not None else self.blocksize_default
        self.block_mode_linked = bool(block_mode_linked)
        self.checksum = bool(checksum)

    cdef dumps(self, obj):
        if not isinstance(obj, MBufferIO):
            obj = MBufferIO(obj)
        compressed = MBufferIO()
        if len(obj) == 0:
            return compressed
        with self.lz4_module.Compressor(compressed, block_size_id=self.block_size_id, block_mode_linked=self.block_mode_linked,
                                        checksum=self.checksum, autoflush=True, level=self.level) as c:
            try:
                while True:
                    c.update(obj.read(65536))
            except self.lz4_module.Lz4FramedNoDataError:
                pass
        compressed.seek(0)
        return compressed

    cdef loads(self, obj):
        if not isinstance(obj, MBufferIO):
            obj = MBufferIO(obj)
        decompressed = MBufferIO()
        if len(obj) == 0:
            return decompressed
        for chunk in self.lz4_module.Decompressor(obj):
           decompressed.write(chunk)
        decompressed.seek(0)
        return decompressed

    def __richcmp__(self, other, op):
        if op == 2:
            if not isinstance(other, LZ4Compresser):
                return False
            return (<LZ4Compresser> self).level == (<LZ4Compresser> other).level and \
                (<LZ4Compresser> self).block_size_id == (<LZ4Compresser> other).block_size_id and \
                (<LZ4Compresser> self).block_mode_linked == (<LZ4Compresser> other).block_mode_linked and \
                (<LZ4Compresser> self).checksum == (<LZ4Compresser> other).checksum
        elif op == 3:
            if isinstance(other, LZ4Compresser):
                return (<LZ4Compresser> self).level != (<LZ4Compresser> other).level or \
                    (<LZ4Compresser> self).block_size_id != (<LZ4Compresser> other).block_size_id or \
                    (<LZ4Compresser> self).block_mode_linked != (<LZ4Compresser> other).block_mode_linked or \
                    (<LZ4Compresser> self).checksum != (<LZ4Compresser> other).checksum
            return True
        else:
            raise ValueError("unsupported operation")

cdef class Chain(Filter):
    def __init__(self, Serializer serializer=None, Signer signer=None, Compresser compresser=None):
        self.serializer = serializer if serializer else NoneSerializer()
        self.signer = signer if signer else NoneSigner()
        self.compresser = compresser if compresser else NoneCompresser()

    cdef dumps(self, obj):
        return self.signer.dumps(self.compresser.dumps(self.serializer.dumps(obj)))

    cdef loads(self, obj):
        return self.serializer.loads(self.compresser.loads(self.signer.loads(obj)))

    def __richcmp__(self, other, op):
        if op == 2:
            if not isinstance(other, Chain):
                return False
            return (<Chain> self).serializer == (<Chain> other).serializer and \
                (<Chain> self).signer == (<Chain> other).signer and \
                (<Chain> self).compresser == (<Chain> other).compresser
        elif op == 3:
            return (<Chain> self).serializer != (<Chain> other).serializer or \
                (<Chain> self).signer != (<Chain> other).signer or \
                (<Chain> self).compresser != (<Chain> other).compresser
        else:
            raise ValueError("unsupported operation")


cdef class NoneChain(Chain):
    def __init__(self):
        super(NoneChain, self).__init__()


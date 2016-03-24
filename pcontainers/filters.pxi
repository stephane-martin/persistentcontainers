
cdef class Filter(object):
    cpdef dumps(self, obj)
    cpdef loads(self, obj)

cdef class Serializer(Filter):
    pass

cdef class NoneSerializer(Serializer):
    pass

cdef class PickleSerializer(Serializer):
    cdef object pickle_module
    cdef int protocol

cdef class MessagePackSerializer(Serializer):
    cdef object messagepack_module
    cdef dict mpack_args
    cdef object use_list
    cdef object ext_hook

cdef class JsonSerializer(Serializer):
    cdef object json_module
    cdef object default

cdef class Signer(Filter):
    cdef bytes secret

cdef class NoneSigner(Signer):
    pass

cdef class HMACSigner(Signer):
    cdef object hmac_module
    cdef object hashlib_module

cdef class Compresser(Filter):
    pass

cdef class NoneCompresser(Compresser):
    pass

cdef class SnappyCompresser(Compresser):
    cdef object snappyx_module

cdef class LZ4Compresser(Compresser):
    cdef object lz4_module
    cdef int blocksize_default
    cdef int compression_min
    cdef object block_mode_linked
    cdef object checksum
    cdef int level
    cdef int block_size_id

cdef class Chain(Filter):
    cdef Serializer serializer
    cdef Signer signer
    cdef Compresser compresser



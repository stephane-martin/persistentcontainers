# -*- coding: utf-8 -*-

cdef inline unicode make_unicode(s):
    if s is None:
        return u''
    if PyUnicode_Check(s):
        return s
    if PyBytes_Check(s):
        return s.decode('utf-8')
    if hasattr(s, '__unicode__'):
        return s.__unicode__()
    if hasattr(s, '__bytes__'):
        return s.__bytes__().decode('utf-8')
    s = str(s)
    if PyUnicode_Check(s):
        return s
    return s.decode('utf-8')


cdef inline bytes make_utf8(s):
    if s is None:
        return b''
    if PyBytes_Check(s):
        return s
    if PyUnicode_Check(s):
        return PyUnicode_AsUTF8String(s)
    if hasattr(s, '__bytes__'):
        return s.__bytes__()
    if hasattr(s, '__unicode__'):
        return PyUnicode_AsUTF8String(s.__unicode__())
    s = str(s)
    if PyUnicode_Check(s):
        return PyUnicode_AsUTF8String(s)
    return s


cdef inline void* copy_mdb_val(MDB_val v) nogil:
    ptr = malloc(v.mv_size)
    memcpy(ptr, v.mv_data, v.mv_size)
    return ptr


cdef inline make_mbufferio_from_cbstring(CBString& s):
    cdef Py_buffer* view = <Py_buffer*> PyMem_Malloc(sizeof(Py_buffer))
    PyBuffer_FillInfo(view, None, <void*> s.data, s.slen, 1, PyBUF_SIMPLE)
    return MBufferIO.from_mview(PyMemoryView_FromBuffer(view), 0)   # the MBufferIO doesnt take ownership of 's' memory


cdef inline make_mbufferio(void* ptr, size_t size, bint take_ownership):
    cdef Py_buffer* view = <Py_buffer*> PyMem_Malloc(sizeof(Py_buffer))
    PyBuffer_FillInfo(view, None, ptr, size, 1, PyBUF_SIMPLE)
    # the mview object created by PyMemoryView_FromBuffer now owns the 'view' Py_buffer, no need to free explicitely
    # the free of ptr will be taken care by MBufferIO
    return MBufferIO.from_mview(PyMemoryView_FromBuffer(view), bool(take_ownership))


cdef inline topy(const CBString& s):
    return PyBytes_FromStringAndSize(<char*> s.data, s.slen)


cdef inline CBString tocbstring(s):
    # todo: accept buffer protocol
    s = make_utf8(s)
    return CBString(<char*> s, len(s))

cdef extern from "utils/pyutils.h" namespace "utils":
    # noinspection PyPep8Naming
    cppclass PyBufferWrap:
        PyBufferWrap() nogil
        PyBufferWrap(object o) except +custom_handler
        Py_buffer* get() nogil
        MDB_val get_mdb_val() nogil
        Py_ssize_t length() nogil
        void* buf() nogil
        void close()

    cdef PyBufferWrap move "boost::move"(PyBufferWrap other)


cdef extern from "utils/pyutils.h" namespace "utils":
    cppclass py_threadsafe_queue:
        py_threadsafe_queue()
        PyObject* py_wait_and_pop()
        PyObject* py_wait_and_pop(long long ms)
        PyObject* py_try_pop()
        void py_push(object new_value)
        cpp_bool empty() const

    py_threadsafe_queue move "boost::move"(py_threadsafe_queue& other)

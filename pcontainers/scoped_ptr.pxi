
cdef extern from "boost/scoped_ptr.hpp" namespace "boost" nogil:

    cppclass scoped_ptr[T]:
        scoped_ptr()
        scoped_ptr(T*)

        # Modifiers
        void reset()
        void reset(T*)
        void swap(scoped_ptr&)

        # Observers
        T* get()
        T& operator*()
        cpp_bool operator bool()
        cpp_bool operator!()

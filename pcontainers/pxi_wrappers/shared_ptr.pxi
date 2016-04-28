
cdef extern from "boost/shared_ptr.hpp" namespace "boost" nogil:

    cppclass shared_ptr[T]:
        shared_ptr()
        shared_ptr(T*)
        shared_ptr(shared_ptr[T]&)
        shared_ptr(shared_ptr[T]&, T*)

        # Modifiers
        void reset()
        void reset(T*)
        void swap(shared_ptr&)

        # Observers
        T* get()
        T& operator*()
        #cpp_bool operator bool()
        #cpp_bool operator!()

        cpp_bool operator==(const shared_ptr&)
        cpp_bool operator!=(const shared_ptr&)


cdef extern from "boost/atomic.hpp" namespace "boost" nogil:
    cppclass atomic_int:
        atomic_int()
        atomic_int(int)
        int load()
        void store(int)
        int exchange(int)


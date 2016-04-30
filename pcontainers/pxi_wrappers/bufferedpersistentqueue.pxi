cdef extern from "cpp_persistent_dict_queue/bufferedpersistentqueue.h" namespace "quiet" nogil:
    cppclass cppBufferedPersistentQueue "quiet::BufferedPersistentQueue":
        cppBufferedPersistentQueue(shared_ptr[cppPersistentQueue] d, uint64_t flush_interval) except +custom_handler
        shared_future[cpp_bool] push_back(const CBString& value) except +custom_handler
        shared_future[cpp_bool] push_back(MDB_val value) except +custom_handler


cdef extern from "cpp_persistent_dict_queue/bufferedpersistentdict.h" namespace "quiet" nogil:
    cppclass cppBufferedPersistentDict "quiet::BufferedPersistentDict":
        cppBufferedPersistentDict(shared_ptr[cppPersistentDict] d, uint64_t flush_interval) except +custom_handler
        void flush() except +custom_handler
        void start() except +custom_handler
        void stop() except +custom_handler
        CBString at(const CBString& key) except +custom_handler
        CBString at(MDB_val key) except +custom_handler
        shared_future[CBString] async_at(const CBString& key) except +custom_handler
        shared_future[CBString] async_at(MDB_val key) except +custom_handler
        shared_future[cpp_bool] erase(const CBString& key) except +custom_handler
        shared_future[cpp_bool] erase(MDB_val key) except +custom_handler
        shared_future[cpp_bool] insert_key_value "quiet::BufferedPersistentDict::insert" (const CBString& key, const CBString& value) except +custom_handler
        shared_future[cpp_bool] insert_key_value "quiet::BufferedPersistentDict::insert" (MDB_val key, MDB_val value) except +custom_handler

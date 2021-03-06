
cdef extern from "cpp_persistent_dict_queue/persistentqueue.h" namespace "quiet" nogil:
    # noinspection PyPep8Naming
    cdef cppclass cppPersistentQueue "quiet::PersistentQueue":
        CBString get_dirname()
        CBString get_dbname()

        cpp_bool push_front(const CBString& val) except +custom_handler
        cpp_bool push_front(MDB_val val) except +custom_handler
        cpp_bool push_front(size_t n, const CBString& val) except +custom_handler
        cpp_bool push_front[InputIterator](InputIterator first, InputIterator last) except +custom_handler

        cpp_bool push_back(const CBString& val) except +custom_handler
        cpp_bool push_back(MDB_val val) except +custom_handler
        cpp_bool push_back(size_t n, const CBString& val) except +custom_handler
        cpp_bool push_back[InputIterator](InputIterator first, InputIterator last) except +custom_handler

        shared_future[cpp_bool] async_push_back(const CBString& val) except +custom_handler
        shared_future[cpp_bool] async_push_back(MDB_val val) except +custom_handler
        shared_future[cpp_bool] async_push_back_many "quiet::PersistentQueue::async_push_back<quiet::PyStringInputIterator>" (PyStringInputIterator first, PyStringInputIterator last) except +custom_handler

        CBString pop_back() except +custom_handler
        CBString pop_front() except +custom_handler
        CBString wait_and_pop_front() except +custom_handler
        CBString wait_and_pop_front(milliseconds) except +custom_handler
        shared_future[CBString] async_wait_and_pop_front() except +custom_handler
        shared_future[CBString] async_wait_and_pop_front(milliseconds) except +custom_handler

        void pop_all[OutputIterator](OutputIterator oit) except +custom_handler

        size_t size() except +custom_handler
        cpp_bool empty() except +custom_handler
        void clear() except +custom_handler

        void move_to(shared_ptr[cppPersistentQueue] other) except +custom_handler
        void move_to(shared_ptr[cppPersistentQueue] other, ssize_t chunk_size) except +custom_handler

        void remove_if(unary_predicate unary_pred) except +custom_handler
        void transform_values(unary_functor unary_funct) except +custom_handler
        void remove_duplicates() except +custom_handler

    shared_ptr[cppPersistentQueue] queue_factory "quiet::PersistentQueue::factory"(const CBString& directory_name) except +custom_handler
    shared_ptr[cppPersistentQueue] queue_factory "quiet::PersistentQueue::factory"(const CBString& directory_name, const CBString& database_name) except +custom_handler
    shared_ptr[cppPersistentQueue] queue_factory "quiet::PersistentQueue::factory"(const CBString& directory_name, const CBString& database_name, const lmdb_options& options) except +custom_handler

    # noinspection PyPep8Naming
    cdef cppclass QueueIterator "quiet::PersistentQueue::iiterator":
        QueueIterator()
        QueueIterator(shared_ptr[cppPersistentQueue] q) except +custom_handler
        QueueIterator(shared_ptr[cppPersistentQueue], int pos) except +custom_handler
        void set_rollback()
        void set_rollback(cpp_bool val)
        size_t size() except +custom_handler
        MDB_val get_value_buffer() except +custom_handler
        (QueueIterator&) incr "quiet::PersistentQueue::iiterator::operator++"() except +custom_handler
        (QueueIterator&) decr "quiet::PersistentQueue::iiterator::operator--"() except +custom_handler
        cpp_bool empty() except +custom_handler

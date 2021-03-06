cdef extern from "cpp_persistent_dict_queue/persistentdict.h" namespace "quiet" nogil:

    # noinspection PyPep8Naming
    cppclass cppPersistentDict "quiet::PersistentDict":
        cpp_bool is_initialized()
        int get_maxkeysize() except +custom_handler
        lmdb_options get_options()
        CBString get_dirname()
        CBString get_dbname()

        CBString setdefault(MDB_val k, MDB_val dflt) except +custom_handler
        CBString at(const CBString& key) except +custom_handler
        CBString at(MDB_val k) except +custom_handler
        CBString get "quiet::PersistentDict::operator[]" (const CBString& key) except +custom_handler
        CBString pop(const CBString& key) except +custom_handler
        CBString pop(MDB_val k) except +custom_handler
        pair[CBString, CBString] popitem() except +custom_handler

        void map_keys[OutputIterator](OutputIterator oit) except +custom_handler
        void map_keys[OutputIterator](OutputIterator oit, const CBString& first) except +custom_handler
        void map_keys[OutputIterator](OutputIterator oit, const CBString& first, const CBString& last) except +custom_handler
        void map_keys[OutputIterator](OutputIterator oit, const CBString& first, const CBString& last, unary_functor f) except +custom_handler
        void map_keys[OutputIterator](OutputIterator oit, const CBString& first, const CBString& last, unary_functor f, unary_predicate unary_pred) except +custom_handler

        void map_values[OutputIterator](OutputIterator oit) except +custom_handler
        void map_values[OutputIterator](OutputIterator oit, const CBString& first) except +custom_handler
        void map_values[OutputIterator](OutputIterator oit, const CBString& first, const CBString& last) except +custom_handler
        void map_values[OutputIterator](OutputIterator oit, const CBString& first, const CBString& last, unary_functor f) except +custom_handler
        void map_values[OutputIterator](OutputIterator oit, const CBString& first, const CBString& last, unary_functor f, unary_predicate unary_pred) except +custom_handler

        void map_keys_values[OutputIterator](OutputIterator oit) except +custom_handler
        void map_keys_values[OutputIterator](OutputIterator oit, const CBString& first) except +custom_handler
        void map_keys_values[OutputIterator](OutputIterator oit, const CBString& first, const CBString& last) except +custom_handler
        void map_keys_values[OutputIterator](OutputIterator oit, const CBString& first, const CBString& last, binary_functor f) except +custom_handler
        void map_keys_values[OutputIterator](OutputIterator oit, const CBString& first, const CBString& last, binary_functor f, binary_predicate unary_pred) except +custom_handler

        void insert(const CBString& key, const CBString& value) except +custom_handler
        void insert(MDB_val k, MDB_val v) except +custom_handler

        void clear() except +custom_handler
        cpp_bool erase(const CBString& key) except +custom_handler
        cpp_bool erase(MDB_val key) except +custom_handler
        vector[CBString] erase_interval() except +custom_handler
        vector[CBString] erase_interval(const CBString& first_key) except +custom_handler
        vector[CBString] erase_interval(const CBString& first_key, const CBString& last_key) except +custom_handler
        vector[CBString] erase_interval(const CBString& first_key, const CBString& last_key, ssize_t chunk_size) except +custom_handler

        cpp_bool empty() except +custom_handler
        cpp_bool empty_interval() except +custom_handler
        cpp_bool empty_interval(const CBString& first_key) except +custom_handler
        cpp_bool empty_interval(const CBString& first_key, const CBString& last_key) except +custom_handler

        size_t size() except +custom_handler
        size_t count_interval() except +custom_handler
        size_t count_interval(const CBString& first_key) except +custom_handler
        size_t count_interval(const CBString& first_key, const CBString& last_key) except +custom_handler

        cpp_bool contains(const CBString& key) except +custom_handler
        cpp_bool contains(MDB_val key) except +custom_handler

        void transform_values(binary_scalar_functor binary_funct) except +custom_handler
        void transform_values(binary_scalar_functor binary_funct, const CBString& first_key) except +custom_handler
        void transform_values(binary_scalar_functor binary_funct, const CBString& first_key, const CBString& last_key) except +custom_handler
        void transform_values(binary_scalar_functor binary_funct, const CBString& first_key, const CBString& last_key, ssize_t chunk_size) except +custom_handler

        void remove_if(binary_predicate binary_pred) except +custom_handler
        void remove_if(binary_predicate binary_pred, const CBString& first_key) except +custom_handler
        void remove_if(binary_predicate binary_pred, const CBString& first_key, const CBString& last_key) except +custom_handler
        void remove_if(binary_predicate binary_pred, const CBString& first_key, const CBString& last_key, ssize_t chunk_size) except +custom_handler

        void move_to(shared_ptr[cppPersistentDict] other) except +custom_handler
        void move_to(shared_ptr[cppPersistentDict] other, const CBString& first_key) except +custom_handler
        void move_to(shared_ptr[cppPersistentDict] other, const CBString& first_key, const CBString& last_key) except +custom_handler
        void move_to(shared_ptr[cppPersistentDict], const CBString& first_key, const CBString& last_key, ssize_t chunk_size) except +custom_handler


        void remove_duplicates() except +custom_handler
        void remove_duplicates(const CBString& first_key) except +custom_handler
        void remove_duplicates(const CBString& first_key, const CBString& last_key) except +custom_handler

        cppIterator before() except +custom_handler
        cppIterator before(cpp_bool readonly) except +custom_handler
        cppConstIterator cbefore() except +custom_handler
        cppIterator begin() except +custom_handler
        cppIterator begin(cpp_bool readonly) except +custom_handler
        cppConstIterator cbegin() except +custom_handler
        cppIterator end() except +custom_handler
        cppIterator end(cpp_bool readonly) except +custom_handler
        cppConstIterator cend() except +custom_handler
        cppIterator last() except +custom_handler
        cppIterator last(cpp_bool readonly) except +custom_handler
        cppConstIterator clast() except +custom_handler

    cpp_bool operator==(const cppPersistentDict& one, const cppPersistentDict& other)
    cpp_bool operator!=(const cppPersistentDict& one, const cppPersistentDict& other)

    shared_ptr[cppPersistentDict] dict_factory "quiet::PersistentDict::factory"(const CBString& directory_name) except +custom_handler
    shared_ptr[cppPersistentDict] dict_factory "quiet::PersistentDict::factory"(const CBString& directory_name, const CBString& database_name) except +custom_handler
    shared_ptr[cppPersistentDict] dict_factory "quiet::PersistentDict::factory"(const CBString& directory_name, const CBString& database_name, const lmdb_options& options) except +custom_handler


    cdef cppclass abstract_iterator "quiet::PersistentDict::abstract_iterator":
        CBString get_key() except +custom_handler
        MDB_val get_key_buffer() except +custom_handler
        CBString get_value() except +custom_handler
        MDB_val get_value_buffer() except +custom_handler
        pair[CBString, CBString] get_item() except +custom_handler
        pair[MDB_val, MDB_val] get_item_buffer() except +custom_handler

        cpp_bool has_reached_end() except +custom_handler
        cpp_bool has_reached_beginning() except +custom_handler
        void set_position(MDB_val key) except +custom_handler
        void set_range(MDB_val key) except +custom_handler

        void set_rollback(cpp_bool val)

        (abstract_iterator&) abs_incr "quiet::PersistentDict::abstract_iterator::operator++"() except +custom_handler
        (abstract_iterator&) abs_decr "quiet::PersistentDict::abstract_iterator::operator--"() except +custom_handler



    # noinspection PyPep8Naming
    cdef cppclass cppIterator "quiet::PersistentDict::iterator" (abstract_iterator):
        cppIterator()
        cppIterator(shared_ptr[cppPersistentDict] d) except +custom_handler
        cppIterator(shared_ptr[cppPersistentDict] d, int pos) except +custom_handler
        cppIterator(shared_ptr[cppPersistentDict] d, int pos, cpp_bool ro) except +custom_handler
        cppIterator(shared_ptr[cppPersistentDict] d, const CBString& key) except +custom_handler
        cppIterator(shared_ptr[cppPersistentDict] d, const CBString& key, cpp_bool ro) except +custom_handler
        cppIterator(shared_ptr[cppPersistentDict] d, MDB_val key) except +custom_handler
        cppIterator(shared_ptr[cppPersistentDict] d, MDB_val key, cpp_bool ro) except +custom_handler

        cppIterator(const cppIterator& other) except +custom_handler

        (cppIterator&) incr "quiet::PersistentDict::iterator::operator++"() except +custom_handler
        (cppIterator&) decr "quiet::PersistentDict::iterator::operator--"() except +custom_handler

        cpp_bool operator==(const cppIterator& other) except +custom_handler
        cpp_bool operator!=(const cppIterator& other) except +custom_handler

        void set_value(const CBString& value) except +custom_handler
        void set_value(MDB_val value) except +custom_handler
        void set_key_value(const CBString& key, const CBString& value) except +custom_handler
        void set_key_value(MDB_val key, MDB_val value) except +custom_handler

        cpp_bool dlte "quiet::PersistentDict::iterator::del"() except +custom_handler
        cpp_bool dlte "quiet::PersistentDict::iterator::del"(MDB_val key) except +custom_handler

    # noinspection PyPep8Naming
    cdef cppclass cppConstIterator "quiet::PersistentDict::const_iterator" (abstract_iterator):
        cppConstIterator()
        cppConstIterator(shared_ptr[cppPersistentDict] d) except +custom_handler
        cppConstIterator(shared_ptr[cppPersistentDict] d, int pos) except +custom_handler
        cppConstIterator(shared_ptr[cppPersistentDict] d, const CBString& key) except +custom_handler
        cppConstIterator(shared_ptr[cppPersistentDict] d, MDB_val key) except +custom_handler

        cppConstIterator(const cppConstIterator& other) except +custom_handler

        (cppConstIterator&) incr "quiet::PersistentDict::const_iterator::operator++"() except +custom_handler
        (cppConstIterator&) decr "quiet::PersistentDict::const_iterator::operator--"() except +custom_handler

        cpp_bool operator==(const cppConstIterator& other) except +custom_handler
        cpp_bool operator!=(const cppConstIterator& other) except +custom_handler

    cdef cppIterator move "boost::move"(cppIterator other)
    cdef cppConstIterator move "boost::move"(cppConstIterator other)


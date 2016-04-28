cdef extern from "boost/thread/future.hpp" namespace "boost" nogil:
    cdef cppclass future_status:
        pass

    cdef future_status f_ready "boost::future_status::ready"
    cdef future_status f_timeout "boost::future_status::timeout"
    cdef future_status f_deferred "boost::future_status::deferred"

    cpp_bool operator==(const future_status& one, const future_status& other)

cdef extern from "cpp_persistent_dict_queue/bufferedpersistentdict.h" namespace "quiet" nogil:

    cppclass future[T]:
        cpp_bool valid()
        cpp_bool is_ready()
        cpp_bool has_exception()
        cpp_bool has_value()
        void wait() except +custom_handler
        const (T&) get() except +custom_handler

    cppclass shared_future[T]:
        shared_future()
        cpp_bool valid()
        cpp_bool is_ready()
        cpp_bool has_exception()
        cpp_bool has_value()
        void wait() except +custom_handler
        future_status wait_for(milliseconds rel_time) except +custom_handler
        const (T&) get() except +custom_handler
        #future[T] then(then_callback[T] func)

cdef extern from "utils/pyfunctor.h" namespace "quiet" nogil:
    (shared_future[T]) then_[T](shared_future[T] fut, object obj) except +custom_handler
    shared_future[CBString] move "boost::move" (shared_future[CBString] other)
    shared_future[cpp_bool] move "boost::move" (shared_future[cpp_bool] other)
    future[CBString] move "boost::move" (future[CBString] other)
    future[cpp_bool] move "boost::move" (future[cpp_bool] other)


cdef extern from "utils/utils.h" namespace "utils" nogil:

    # noinspection PyPep8Naming
    cppclass TempDirectory:
        TempDirectory() except +custom_handler
        TempDirectory(cpp_bool create) except +custom_handler
        TempDirectory(cpp_bool create, cpp_bool destroy) except +custom_handler
        TempDirectory(cpp_bool create, cpp_bool destroy, const CBString& tmpl) except +custom_handler
        CBString get_path() except +custom_handler

    shared_ptr[TempDirectory] make_temp_directory "utils::TempDirectory::make"() except +custom_handler
    shared_ptr[TempDirectory] make_temp_directory "utils::TempDirectory::make"(cpp_bool create) except +custom_handler
    shared_ptr[TempDirectory] make_temp_directory "utils::TempDirectory::make"(cpp_bool create, cpp_bool destroy) except +custom_handler


cdef extern from "utils/threadsafe_queue.h" namespace "utils" nogil:
    cppclass threadsafe_queue[T]:
        threadsafe_queue()
        shared_ptr[T] wait_and_pop()
        shared_ptr[T] try_pop()
        push(T new_value)
        cpp_bool empty() const

cdef extern from "boost/chrono/chrono.hpp" namespace "boost::chrono" nogil:
    cppclass milliseconds:
        milliseconds()
        milliseconds(uint64_t)
        uint64_t count()
    cppclass nanoseconds:
        nanoseconds()
        nanoseconds(uint64_t)
        uint64_t count()
    cppclass steady_timepoint "boost::chrono::time_point<steady_clock>":
        steady_timepoint()
        nanoseconds time_since_epoch()
    steady_timepoint steady_now "boost::chrono::steady_clock::now"()

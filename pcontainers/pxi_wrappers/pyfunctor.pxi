cdef extern from "utils/pyfunctor.h" namespace "quiet" nogil:
    # noinspection PyPep8Naming
    cppclass unary_predicate:
        pass
    # noinspection PyPep8Naming
    cppclass binary_predicate:
        pass
    # noinspection PyPep8Naming
    cppclass unary_functor:
        pass
    # noinspection PyPep8Naming
    cppclass binary_scalar_functor:
        pass
    # noinspection PyPep8Naming
    cppclass binary_functor:
        pass


    cdef unary_predicate make_unary_predicate "quiet::PyPredicate::make_unary_predicate"(object obj) except +custom_handler
    cdef binary_predicate make_binary_predicate "quiet::PyPredicate::make_binary_predicate"(object obj) except +custom_handler
    cdef unary_functor make_unary_functor "quiet::PyFunctor::make_unary_functor"(object obj) except +custom_handler
    cdef binary_scalar_functor make_binary_scalar_functor "quiet::PyFunctor::make_binary_scalar_functor"(object obj) except +custom_handler

    # noinspection PyPep8Naming
    cdef cppclass PyStringInputIterator:
        PyStringInputIterator()
        PyStringInputIterator(object obj) except +custom_handler

    cdef PyStringInputIterator move "boost::move"(PyStringInputIterator other)

    # noinspection PyPep8Naming
    cdef cppclass PyFunctionOutputIterator:
        PyFunctionOutputIterator()
        PyFunctionOutputIterator(object obj) except +custom_handler

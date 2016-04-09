# -*- coding: utf-8 -*-

from setup import extensions, runsetup, Extension, setup_requires, IS_LINUX, ROOT, join

extensions[:] = [
    Extension(
        name="pcontainers._py_exceptions",
        sources=[
            'pcontainers/_py_exceptions.pyx',
            'pcontainers/custom_exc.cpp'
        ],
        include_dirs=[join(ROOT, "pcontainers", "includes")],
        language="c++"
    ),
    Extension(
        name="pcontainers._pdict",
        sources=[
            'pcontainers/_pdict.pyx',
            'pcontainers/boost/chrono.cpp',
            'pcontainers/boost/error_code.cpp',
            'pcontainers/boost/future.cpp',
            'pcontainers/boost/lockpool.cpp',
            'pcontainers/boost/once.cpp',
            'pcontainers/boost/process_cpu_clocks.cpp',
            'pcontainers/boost/thread.cpp',
            'pcontainers/boost/thread_clock.cpp',
            'pcontainers/boost/tss_null.cpp',
            'pcontainers/lmdb/mdb.c',
            'pcontainers/lmdb/midl.c',
            'pcontainers/persistentdict.cpp',
            'pcontainers/persistentqueue.cpp',
            'pcontainers/lmdb_environment.cpp',
            'pcontainers/logging.cpp',
            'pcontainers/pylogging.cpp',
            'pcontainers/utils.cpp',
            'pcontainers/lmdb_exceptions.cpp',
            'pcontainers/includes/bstrlib/bstrlib.c',
            'pcontainers/includes/bstrlib/bstrwrap.cpp',
            'pcontainers/includes/bstrlib/utf8util.c',
            'pcontainers/includes/bstrlib/buniutil.c'

        ],
        include_dirs=[join(ROOT, "pcontainers", "includes")],
        language="c++",
        define_macros=[
            ('BSTRLIB_CAN_USE_IOSTREAM', None),
            ('BSTRLIB_CAN_USE_STL', None),
            ('BSTRLIB_THROWS_EXCEPTIONS', None),
            ('BOOST_THREAD_USES_ATOMIC', None)
        ]
    )
]

setup_requires.append('cython')

if __name__ == "__main__":
    runsetup()


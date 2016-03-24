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
            'pcontainers/mdb.c',
            'pcontainers/midl.c',
            'pcontainers/persistentdict.cpp',
            'pcontainers/persistentqueue.cpp',
            'pcontainers/lmdb_environment.cpp',
            'pcontainers/logging.cpp',
            'pcontainers/pylogging.cpp',
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
            ('BSTRLIB_THROWS_EXCEPTIONS', None)
        ]
    )
]

setup_requires.append('cython')

if __name__ == "__main__":
    runsetup()


# -*- coding: utf-8 -*-

from setup import extensions, runsetup, Extension, setup_requires, IS_LINUX, ROOT, join

extensions[:] = [
    Extension(
        name="pcontainers._py_exceptions",
        sources=[
            'pcontainers/_py_exceptions.pyx',
            'pcontainers/custom_exc.cpp'
        ],
        language="c++"
    ),
    Extension(
        name="pcontainers._pdict",
        sources=[
            'pcontainers/_pdict.pyx',
            'pcontainers/mdb.c',
            'pcontainers/midl.c',
            'pcontainers/persistentdict.cpp',
            'pcontainers/lmdb_environment.cpp'
        ],
        include_dirs=[join(ROOT, "pcontainers", "includes")],
        language="c++"
    )
]

setup_requires.append('cython')

if __name__ == "__main__":
    runsetup()


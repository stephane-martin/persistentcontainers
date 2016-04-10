# -*- coding: utf-8 -*-
from setup import extensions, runsetup, Extension, setup_requires, IS_LINUX, ROOT, join, pdict_sources

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
        sources=pdict_sources,
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


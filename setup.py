# -*- coding: utf-8 -*-


from setuptools import setup, find_packages, Extension
from os.path import dirname, abspath, exists, join
import os
import platform
import distutils.sysconfig
import sysconfig
import sys
from io import open
import subprocess

ROOT = abspath(dirname(__file__))
ON_READTHEDOCS = os.environ.get('READTHEDOCS', None) == 'True'
PLATFORM = platform.system().lower().strip()
IS_MACOSX = "darwin" in PLATFORM
IS_LINUX = "linux" in PLATFORM


def check_libuuid():
    sp = subprocess.Popen(['cpp'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    sp.communicate('#include <uuid/uuid.h>\n')
    return sp.returncode == 0


requirements = ['future', 'mbufferio', 'futures']

setup_requires = [
    'setuptools_git', 'setuptools', 'twine', 'wheel', 'pip', 'pytest'
]

pdict_sources = [
    'pcontainers/_pdict.pyx',
    'pcontainers/boost/chrono.cpp',
    'pcontainers/boost/clone_current_exception_non_intrusive.cpp',
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
    'pcontainers/cpp_persistent_dict_queue/persistentdict.cpp',
    'pcontainers/cpp_persistent_dict_queue/persistentqueue.cpp',
    'pcontainers/cpp_persistent_dict_queue/bufferedpersistentdict.cpp',
    'pcontainers/lmdb_environment/lmdb_environment.cpp',
    'pcontainers/logging/logging.cpp',
    'pcontainers/logging/pylogging.cpp',
    'pcontainers/utils/pyfunctor.cpp',
    'pcontainers/utils/utils.cpp',
    'pcontainers/lmdb_exceptions/lmdb_exceptions.cpp',
    'pcontainers/includes/bstrlib/bstrlib.c',
    'pcontainers/includes/bstrlib/bstrwrap.cpp',
    'pcontainers/includes/bstrlib/utf8util.c',
    'pcontainers/includes/bstrlib/buniutil.c'
]

macros = [
    ('BSTRLIB_CAN_USE_IOSTREAM', None),
    ('BSTRLIB_CAN_USE_STL', None),
    ('BSTRLIB_THROWS_EXCEPTIONS', None),
    ('BOOST_THREAD_USES_ATOMIC', None),
    ('BOOST_THREAD_VERSION', '4'),
    ('BOOST_THREAD_CONTINUATION_SYNC', None),
    ('BOOST_DATE_TIME_NO_LIB', None)
]

extensions = [
    Extension(
        name="pcontainers._py_exceptions",
        sources=[
            'pcontainers/_py_exceptions.cpp',
            'pcontainers/custom_exc.cpp'
        ],
        include_dirs=[join(ROOT, "pcontainers", "includes")],
        language="c++"
    ),
    Extension(
        name="pcontainers._pdict",
        sources=[srcfile.replace(".pyx", ".cpp") for srcfile in pdict_sources],
        include_dirs=[join(ROOT, "pcontainers", "includes")],
        language="c++",
        define_macros=macros
    )
]

name = 'persistentcontainers'
version = '0.5'
description = 'persistentcontainers provides some basic python structures, such as dict or queue, but persisted in LMDB'
author = 'Stephane Martin'
author_email = 'stephane.martin_github@vesperal.eu'
url = 'https://github.com/stephane-martin/persistentcontainers'
licens = "OpenLDAP Public License"
keywords = 'lmdb python'
data_files = []

classifiers = [
    'Development Status :: 4 - Beta',
    'Programming Language :: C',
    'Programming Language :: C++',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3',
    'Programming Language :: Cython',
    'Operating System :: POSIX',
    'Topic :: Software Development :: Libraries'
]

entry_points = dict()

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

long_description = readme + '\n\n' + history


def info(s):
    sys.stderr.write(s + "\n")


def runsetup():

    if IS_MACOSX:
        disutils_sysconfig = distutils.sysconfig.get_config_vars()
        # don't build useless i386 architecture
        disutils_sysconfig['LDSHARED'] = disutils_sysconfig['LDSHARED'].replace('-arch i386', '')
        disutils_sysconfig['CFLAGS'] = disutils_sysconfig['CFLAGS'].replace('-arch i386', '')
        # suppress painful warnings
        disutils_sysconfig['CFLAGS'] = disutils_sysconfig['CFLAGS'].replace('-Wstrict-prototypes', '')
        python_config_vars = sysconfig.get_config_vars()
        # use the same SDK as python executable
        if not exists(python_config_vars['UNIVERSALSDK']):
            info("'{}' SDK does not exist. Aborting.\n".format(python_config_vars['UNIVERSALSDK']))
            sys.exit(-1)
        info("Building for MacOSX SDK: {}".format(python_config_vars["MACOSX_DEPLOYMENT_TARGET"]))
        os.environ["MACOSX_DEPLOYMENT_TARGET"] = python_config_vars["MACOSX_DEPLOYMENT_TARGET"]
        os.environ["SDKROOT"] = python_config_vars["UNIVERSALSDK"]

    elif IS_LINUX:
        pass
    else:
        info("Works on MacOSX or Linux")
        sys.exit(1)

    setup(
        name=name,
        version=version,
        description=description,
        long_description=long_description,
        author=author,
        author_email=author_email,
        url=url,
        packages=find_packages(),
        setup_requires=setup_requires,
        include_package_data=True,
        install_requires=requirements,
        license=licens,
        zip_safe=False,
        keywords=keywords,
        classifiers=classifiers,
        entry_points=entry_points,
        data_files=data_files,
        ext_modules=extensions,

    )


if __name__ == "__main__":
    runsetup()

# -*- coding: utf-8 -*-

# noinspection PyUnresolvedReferences
from cpython.ref cimport PyObject, Py_DECREF
# noinspection PyUnresolvedReferences
from cython.operator cimport dereference as deref
from libc.string cimport memcpy
from libc.time cimport time as c_time

import logging
import uuid
import shutil
import collections
import threading
from time import sleep
from queue import Empty, Full
from os.path import join
from numbers import Number
from concurrent.futures import Future, ThreadPoolExecutor, CancelledError, TimeoutError



# noinspection PyPackageRequirements
from mbufferio import MBufferIO

from ._py_exceptions import EmptyDatabase, NotFound, EmptyKey, BadValSize, NotInitialized, LmdbError

include "lmdb_options_impl.pxi"
include "pdict_impl.pxi"
include "pqueue_impl.pxi"
include "cpp_future_wrapper_impl.pxi"
include "buffered_pdict_impl.pxi"
include "buffered_pqueue_impl.pxi"
include "expiry_dict_impl.pxi"
include "threadsafe_queue_impl.pxi"
include "monotonic_impl.pxi"
include "logging_impl.pxi"
include "filters_impl.pxi"


def _adapt_list_append(l):
    def append(k, v):
        return l.append((k, v))
    return append

def _adapt_unary_functor(unary_funct, Chain value_chain):
    def functor(x):
        return value_chain.dumps(unary_funct(value_chain.loads(x))).tobytes()
    return functor


def _adapt_binary_scalar_functor(binary_funct, Chain key_chain, Chain value_chain):
    def functor(x, y):
        return value_chain.dumps(
            binary_funct(
                key_chain.loads(x),
                value_chain.loads(y)
            )
        ).tobytes()
    return functor


def _adapt_unary_predicate(unary_pred, Chain value_chain):
    def predicate(x):
        return bool(unary_pred(value_chain.loads(x)))
    return predicate


def _adapt_binary_predicate(binary_pred, Chain key_chain, Chain value_chain):
    def predicate(x, y):
        return bool(
            binary_pred(
                key_chain.loads(x),
                value_chain.loads(y)
            )
        )
    return predicate


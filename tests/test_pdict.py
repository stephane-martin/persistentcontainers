# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import os
import shutil

import pytest

from pcontainers import PRawDict, NotFound, EmptyKey


@pytest.fixture
def temp_raw_dict():
    return PRawDict.make_temp()

class TestPRawDict(object):
    def test_empty_constructor(self):
        with pytest.raises(ValueError):
            PRawDict("", "")
        with pytest.raises(ValueError):
            PRawDict("", "foo")

    def test_constructor(self):
        try:
            d = PRawDict('/tmp/foo', '')
            assert(d.dirname in ['/tmp/foo', '/private/tmp/foo'])
        finally:
            try:
                shutil.rmtree('/tmp/foo')
            except:
                pass


    def test_constructor_dbname(self):
        try:
            d = PRawDict('/tmp/foo', b'mydb')
            assert(d.dirname in ['/tmp/foo', '/private/tmp/foo'])
            assert(d.dbname == b'mydb')
        finally:
            try:
                shutil.rmtree('/tmp/foo')
            except:
                pass

    def test_constructor_temp(self):
        d = PRawDict.make_temp(destroy=True)
        dirname = d.dirname
        assert(os.path.isdir(dirname))
        del d
        assert(not os.path.exists(dirname))

    def test_equality(self, temp_raw_dict):
        copy = PRawDict(temp_raw_dict.dirname, temp_raw_dict.dbname)
        assert(temp_raw_dict == copy)
        another = PRawDict.make_temp()
        assert(temp_raw_dict != another)

    def test_getitem_with_empty_dict(self, temp_raw_dict):
        with pytest.raises(NotFound):
            val = temp_raw_dict[b'foo']

    def test_getitem_empty_key(self, temp_raw_dict):
        with pytest.raises(EmptyKey):
            val = temp_raw_dict['']

    def test_get_with_empty_dict(self, temp_raw_dict):
        assert(temp_raw_dict.get(b'foo') == b'')
        assert(temp_raw_dict.get(b'foo', b'bar') == b'bar')

    def test_get_empty_key(self, temp_raw_dict):
        with pytest.raises(EmptyKey):
            val = temp_raw_dict.get('')
        with pytest.raises(EmptyKey):
            val = temp_raw_dict.get('', b'foo')

    def test_get_set_only_one(self, temp_raw_dict):
        assert(temp_raw_dict.get(b'foo') == b'')
        temp_raw_dict[b'foo'] = b'bar'
        assert(temp_raw_dict.get(b'foo') == b'bar')

    def test_setdefault(self, temp_raw_dict):
        with pytest.raises(NotFound):
            val = temp_raw_dict[b'foo']
        temp_raw_dict[b'foo'] = b'bar'
        assert(temp_raw_dict.setdefault(b'foo', b'zogzog') == b'bar')
        assert(temp_raw_dict.setdefault(b'foo2', b'zogzog') == b'zogzog')

    # noinspection PyCompatibility
    def test_setitem(self, temp_raw_dict):
        all_keys = set()
        all_values = set()
        for i in range(1000):
            all_keys.add(bytes(i))
            all_values.add(b"foo" + bytes(i))
            temp_raw_dict[bytes(i)] = b"foo" + bytes(i)
        assert(set(temp_raw_dict.noiterkeys()) == all_keys)
        assert(set(temp_raw_dict.noitervalues()) == all_values)

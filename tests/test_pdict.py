# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import os
import shutil
import pytest

from pcontainers import PRawDict, NotFound, EmptyKey, set_logger, BadValSize, EmptyDatabase, LmdbError

set_logger()

@pytest.fixture
def temp_raw_dict():
    return PRawDict.make_temp()


@pytest.fixture
def init_temp_raw_dict(temp_raw_dict):
    temp_raw_dict['1'] = b'bar1'
    temp_raw_dict['2'] = b'bar2'
    temp_raw_dict['4'] = b'bar4'
    temp_raw_dict['7'] = b'bar7'
    temp_raw_dict['8'] = b'bar8'
    temp_raw_dict['9'] = b'bar9'
    return temp_raw_dict


# noinspection PyCompatibility
class TestPRawDict(object):
    def test_empty_constructor(self):
        with pytest.raises(LmdbError):
            PRawDict(b"", b"")
        with pytest.raises(LmdbError):
            PRawDict(b"", b"foo")

    def test_constructor(self):
        try:
            d = PRawDict(b'/tmp/foo', b'')
            assert(d.dirname in ['/tmp/foo', '/private/tmp/foo'])
        finally:
            try:
                shutil.rmtree('/tmp/foo')
            except:
                pass

    def test_constructor_dbname(self):
        try:
            d = PRawDict(b'/tmp/foo', b'mydb')
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
        temp_raw_dict[b'foo2'] = b'bar2'
        assert(temp_raw_dict[b'foo2'] == b'bar2')

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
        temp_raw_dict.noiterkeys()
        assert(set(bytes(k) for k in temp_raw_dict.noiterkeys()) == all_keys)
        assert(set(bytes(v) for v in temp_raw_dict.noitervalues()) == all_values)

    def test_setitem_empty_key_or_value(self, temp_raw_dict):
        with pytest.raises(EmptyKey):
            temp_raw_dict[''] = 'bla'
        temp_raw_dict['bla'] = ''

    def test_delitem_empty(self, temp_raw_dict):
        with pytest.raises(EmptyKey):
            del temp_raw_dict['']
        with pytest.raises(NotFound):
            del temp_raw_dict['foo']

    def test_delitem(self, temp_raw_dict):
        temp_raw_dict['foo'] = b'bar'
        assert(temp_raw_dict['foo'] == b'bar')
        del temp_raw_dict['foo']
        with pytest.raises(NotFound):
            del temp_raw_dict['foo']

    def test_too_long_key(self, temp_raw_dict):
        with pytest.raises(BadValSize):
            temp_raw_dict[600 * b'a'] = "zog"

    def test_erase(self, init_temp_raw_dict):
        init_temp_raw_dict.erase('2', '8')

        assert (init_temp_raw_dict['1'] == b'bar1')
        assert (init_temp_raw_dict['8'] == b'bar8')
        assert (init_temp_raw_dict['9'] == b'bar9')
        for i in (2, 4, 7):
            with pytest.raises(NotFound):
                # noinspection PyStatementEffect
                init_temp_raw_dict[i]

        init_temp_raw_dict.erase('a', 'b')

        assert (init_temp_raw_dict['1'] == b'bar1')
        assert (init_temp_raw_dict['8'] == b'bar8')
        assert (init_temp_raw_dict['9'] == b'bar9')
        for i in (2, 4, 7):
            with pytest.raises(NotFound):
                # noinspection PyStatementEffect
                init_temp_raw_dict[i]

    def test_noiterkeys_empty(self, temp_raw_dict):
        assert(temp_raw_dict.noiterkeys() == [])

    def test_noitervalues_empty(self, temp_raw_dict):
        assert (temp_raw_dict.noitervalues() == [])

    def test_noiteritems_empty(self, temp_raw_dict):
        assert (temp_raw_dict.noiteritems() == [])

    def test_noiterkeys(self, init_temp_raw_dict):
        assert(init_temp_raw_dict.noiterkeys() == [b"1", b"2", b"4", b"7", b"8", b"9"])
        del init_temp_raw_dict[b'4']
        assert (init_temp_raw_dict.noiterkeys() == [b"1", b"2", b"7", b"8", b"9"])

    def test_noitervalues(self, init_temp_raw_dict):
        assert(init_temp_raw_dict.noitervalues() == [b"bar1", b"bar2", b"bar4", b"bar7", b"bar8", b"bar9"])
        del init_temp_raw_dict[b'7']
        assert (init_temp_raw_dict.noitervalues() == [b"bar1", b"bar2", b"bar4", b"bar8", b"bar9"])

    def test_noiteritems(self, init_temp_raw_dict):
        assert(init_temp_raw_dict.noiteritems() == [
            (b'1', b'bar1'),
            (b'2', b'bar2'),
            (b'4', b'bar4'),
            (b'7', b'bar7'),
            (b'8', b'bar8'),
            (b'9', b'bar9'),
        ])
        del init_temp_raw_dict[b'8']
        assert (init_temp_raw_dict.noiteritems() == [
            (b'1', b'bar1'),
            (b'2', b'bar2'),
            (b'4', b'bar4'),
            (b'7', b'bar7'),
            (b'9', b'bar9'),
        ])

    def test_empty_pop(self, temp_raw_dict):
        with pytest.raises(NotFound):
            temp_raw_dict.pop('foo')

    def test_pop(self, init_temp_raw_dict):
        with pytest.raises(NotFound):
            init_temp_raw_dict.pop('foo')
        assert(init_temp_raw_dict.pop(b'4') == b'bar4')
        with pytest.raises(NotFound):
            init_temp_raw_dict.pop(b'4')
        assert (init_temp_raw_dict.pop(b'7') == b'bar7')

    def test_empty_popitem(self, temp_raw_dict):
        with pytest.raises(EmptyDatabase):
            temp_raw_dict.popitem()

    def test_popitem(self, init_temp_raw_dict):
        assert(init_temp_raw_dict.popitem() == (b'1', b'bar1'))
        assert(init_temp_raw_dict.popitem() == (b'2', b'bar2'))
        assert(init_temp_raw_dict.popitem() == (b'4', b'bar4'))
        assert(init_temp_raw_dict.popitem() == (b'7', b'bar7'))
        assert(init_temp_raw_dict.popitem() == (b'8', b'bar8'))
        assert(init_temp_raw_dict.popitem() == (b'9', b'bar9'))
        with pytest.raises(EmptyDatabase):
            init_temp_raw_dict.popitem()

    def test_empty_iter(self, temp_raw_dict):
        assert(list(iter(temp_raw_dict)) == [])
        assert(list(temp_raw_dict.keys(True)) == [])

    def test_iter(self, init_temp_raw_dict):
        assert(list(iter(init_temp_raw_dict)) == [b'1', b'2', b'4', b'7', b'8', b'9'])
        assert(list(init_temp_raw_dict.keys(True)) == [b'9', b'8', b'7', b'4', b'2', b'1'])

    def test_empty_values(self, temp_raw_dict):
        assert(list(temp_raw_dict.values()) == [])
        assert(list(temp_raw_dict.values(True)) == [])

    def test_values(self, init_temp_raw_dict):
        assert(list(init_temp_raw_dict.values()) == [b'bar1', b'bar2', b'bar4', b'bar7', b'bar8', b'bar9'])
        assert(list(init_temp_raw_dict.values(True)) == [b'bar9', b'bar8', b'bar7', b'bar4', b'bar2', b'bar1'])

    def test_empty_items(self, temp_raw_dict):
        assert(list(temp_raw_dict.items()) == [])
        assert(list(temp_raw_dict.items(True)) == [])

    def test_items(self, init_temp_raw_dict):
        assert(list(init_temp_raw_dict.items()) == [(b'1', b'bar1'), (b'2', b'bar2'), (b'4', b'bar4'), (b'7', b'bar7'), (b'8', b'bar8'), (b'9', b'bar9')])
        assert (list(init_temp_raw_dict.items(True)) == [(b'9', b'bar9'), (b'8', b'bar8'), (b'7', b'bar7'), (b'4', b'bar4'), (b'2', b'bar2'), (b'1', b'bar1')])

    def test_empty_bool(self, temp_raw_dict):
        assert(bool(temp_raw_dict))

    def test_bool(self, init_temp_raw_dict):
        assert(bool(init_temp_raw_dict))

    def test_empty_len(self, temp_raw_dict):
        assert(len(temp_raw_dict) == 0)

    def test_len(self, init_temp_raw_dict):
        assert(len(init_temp_raw_dict) == 6)

    def test_empty_clear(self, temp_raw_dict):
        temp_raw_dict.clear()
        assert(len(temp_raw_dict) == 0)

    def test_clear(self, init_temp_raw_dict):
        init_temp_raw_dict.clear()
        assert(len(init_temp_raw_dict) == 0)

    def test_empty_contains(self, temp_raw_dict):
        assert(b"foo" not in temp_raw_dict)
        assert(b"1" not in temp_raw_dict)

    def test_contains(self, init_temp_raw_dict):
        assert(b"foo" not in init_temp_raw_dict)
        assert(b"1" in init_temp_raw_dict)
        assert(u"1" in init_temp_raw_dict)

    def test_update(self, temp_raw_dict):
        g = ((bytes(i), b"foo" + bytes(i)) for i in xrange(1000))
        temp_raw_dict.update(g)
        assert(temp_raw_dict.noiterkeys() == sorted([bytes(i) for i in xrange(1000)]))
        assert(temp_raw_dict.noitervalues() == sorted([b'foo' + bytes(i) for i in xrange(1000)]))

    def test_remove_if(self, init_temp_raw_dict):
        init_temp_raw_dict[b'10'] = b'zog'
        init_temp_raw_dict[b'john'] = b'doh'
        init_temp_raw_dict.remove_if(lambda key, val: key.isdigit())
        for k in init_temp_raw_dict:
            assert(not k.isdigit())
        assert(b'10' not in init_temp_raw_dict)
        assert(init_temp_raw_dict[b'john'] == b'doh')
        assert(len(init_temp_raw_dict) == 1)

    def test_empty_transform_values(self, temp_raw_dict):
        temp_raw_dict.transform_values(lambda key, val: key + val)
        assert(len(temp_raw_dict) == 0)

    def test_transform_values(self, init_temp_raw_dict):
        f = lambda key, val: key + val
        initial_content = dict(init_temp_raw_dict)
        transformed_content = {key: f(key, val) for key, val in initial_content.items()}
        init_temp_raw_dict.transform_values(f)
        assert(dict(init_temp_raw_dict) == transformed_content)


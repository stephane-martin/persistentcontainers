# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import os
import shutil
import pytest

from pcontainers import PRawDict, NotFound, EmptyKey, set_logger, BadValSize, EmptyDatabase, LmdbError, PDict
from pcontainers import LmdbOptions
from pcontainers import Chain
from pcontainers import PickleSerializer, JsonSerializer, MessagePackSerializer, NoneSerializer
from pcontainers import HMACSigner, NoneSigner
from pcontainers import SnappyCompresser, LZ4Compresser, NoneCompresser


set_logger()


@pytest.fixture(params=['key_none', 'key_json', 'key_pickle', 'key_messagepack'])
def key_serializer(request):
    if request.param == "key_none":
        return NoneSerializer()
    if request.param == "key_json":
        return JsonSerializer()
    if request.param == "key_pickle":
        return PickleSerializer()
    if request.param == 'key_messagepack':
        return MessagePackSerializer()


@pytest.fixture(params=['value_none', 'value_json', 'value_pickle', 'value_messagepack'])
def value_serializer(request):
    if request.param == "value_none":
        return NoneSerializer()
    if request.param == "value_json":
        return JsonSerializer()
    if request.param == "value_pickle":
        return PickleSerializer()
    if request.param == 'value_messagepack':
        return MessagePackSerializer()


@pytest.fixture(params=['key_none_signer', 'key_hmac_signer'])
def key_signer(request):
    if request.param == 'key_none_signer':
        return NoneSigner()
    if request.param == "key_hmac_signer":
        return HMACSigner(b'my secret signing key')


@pytest.fixture(params=['value_none_signer', 'value_hmac_signer'])
def value_signer(request):
    if request.param == 'value_none_signer':
        return NoneSigner()
    if request.param == "value_hmac_signer":
        return HMACSigner(b'my secret signing key')


@pytest.fixture(params=['key_none_compresser', 'key_snappy_compresser', 'key_lz4_compresser'])
def key_compresser(request):
    if request.param == 'key_none_compresser':
        return NoneCompresser()
    if request.param == 'key_snappy_compresser':
        return SnappyCompresser()
    if request.param == 'key_lz4_compresser':
        return LZ4Compresser()


@pytest.fixture(params=['value_none_compresser', 'value_snappy_compresser', 'value_lz4_compresser'])
def value_compresser(request):
    if request.param == 'value_none_compresser':
        return NoneCompresser()
    if request.param == 'value_snappy_compresser':
        return SnappyCompresser()
    if request.param == 'value_lz4_compresser':
        return LZ4Compresser()


@pytest.fixture
def key_chain(key_serializer, key_signer, key_compresser):
    return Chain(serializer=key_serializer, signer=key_signer, compresser=key_compresser)


@pytest.fixture
def value_chain(value_serializer, value_signer, value_compresser):
    return Chain(serializer=value_serializer, signer=value_signer, compresser=value_compresser)


@pytest.fixture(params=["default_lmdb_opts", "map_async_write_map", "no_meta_sync", "no_sync"])
def lmdb_options(request):
    if request.param == "map_async_write_map":
        return LmdbOptions(map_async=True, write_map=True)
    if request.param == "no_meta_sync":
        return LmdbOptions(no_meta_sync=True)
    if request.param == "no_sync":
        return LmdbOptions(no_sync=True)
    return LmdbOptions()


@pytest.fixture
def temp_raw_dict(lmdb_options):
    return PRawDict.make_temp(opts=lmdb_options)


@pytest.fixture
def temp_dict(lmdb_options, value_chain):
    return PDict.make_temp(opts=lmdb_options, value_chain=value_chain)


@pytest.fixture
def temp_all_dict(lmdb_options, key_chain, value_chain):
    return PDict.make_temp(opts=lmdb_options, key_chain=key_chain, value_chain=value_chain)


@pytest.fixture
def init_temp_raw_dict(temp_raw_dict):
    temp_raw_dict['1'] = b'bar1'
    temp_raw_dict['2'] = b'bar2'
    temp_raw_dict['4'] = b'bar4'
    temp_raw_dict['7'] = b'bar7'
    temp_raw_dict['8'] = b'bar8'
    temp_raw_dict['9'] = b'bar9'
    return temp_raw_dict


@pytest.fixture
def init_temp_dict(temp_dict):
    temp_dict['1'] = u'bar1'
    temp_dict['2'] = u'bar2'
    temp_dict['4'] = u'bar4'
    temp_dict['7'] = u'bar7'
    temp_dict['8'] = u'bar8'
    temp_dict['9'] = u'bar9'
    return temp_dict


@pytest.fixture
def init_temp_all_dict(temp_all_dict):
    temp_all_dict[u'1'] = u'bar1'
    temp_all_dict[u'2'] = u'bar2'
    temp_all_dict[u'4'] = u'bar4'
    temp_all_dict[u'7'] = u'bar7'
    temp_all_dict[u'8'] = u'bar8'
    temp_all_dict[u'9'] = u'bar9'
    return temp_all_dict


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

    def test_empty_remove_duplicates(self, temp_raw_dict):
        temp_raw_dict.remove_duplicates()
        assert(len(temp_raw_dict) == 0)

    def test_remove_duplicates(self, init_temp_raw_dict):
        l = len(init_temp_raw_dict)
        init_vals = set(init_temp_raw_dict.noitervalues())
        k, v = init_temp_raw_dict.popitem()
        init_temp_raw_dict[b'foo'] = v
        init_temp_raw_dict[b'zog'] = v
        init_temp_raw_dict[b'bla'] = v
        init_temp_raw_dict.remove_duplicates()
        assert(len(init_temp_raw_dict) == l)
        assert(set(init_temp_raw_dict.noitervalues()) == init_vals)

    def test_write_batch(self, init_temp_raw_dict):
        l = len(init_temp_raw_dict)
        with init_temp_raw_dict.write_batch():
            init_temp_raw_dict[b'foo'] = b'bar'
            with pytest.raises(NotFound):
                val = init_temp_raw_dict[b'foo']
            init_temp_raw_dict[b'foo2'] = b'bar2'
        assert(len(init_temp_raw_dict) == (l + 2))
        assert(init_temp_raw_dict[b'foo'] == b'bar')
        assert (init_temp_raw_dict[b'foo2'] == b'bar2')

    def test_move_to(self, init_temp_raw_dict):
        other = PRawDict.make_temp()
        other['foo'] = 'bar'
        l = len(init_temp_raw_dict)
        init_temp_raw_dict.move_to(other)
        assert(len(init_temp_raw_dict) == 0)
        assert(len(other) == (l + 1))
        assert(other['foo'] == b'bar')


# noinspection PyCompatibility
class TestSimplePDict(object):
    def test_empty_constructor(self):
        with pytest.raises(LmdbError):
            PDict(b"", b"")
        with pytest.raises(LmdbError):
            PDict(b"", b"foo")

    def test_constructor(self):
        try:
            d = PDict(b'/tmp/foo', b'')
            assert (d.dirname in ['/tmp/foo', '/private/tmp/foo'])
        finally:
            try:
                shutil.rmtree('/tmp/foo')
            except:
                pass

    def test_constructor_dbname(self):
        try:
            d = PDict(b'/tmp/foo', b'mydb')
            assert (d.dirname in ['/tmp/foo', '/private/tmp/foo'])
            assert (d.dbname == b'mydb')
        finally:
            try:
                shutil.rmtree('/tmp/foo')
            except:
                pass

    def test_constructor_temp(self):
        d = PDict.make_temp(destroy=True)
        dirname = d.dirname
        assert (os.path.isdir(dirname))
        del d
        assert (not os.path.exists(dirname))

    def test_equality(self, temp_dict):
        copy = PDict(dirname=temp_dict.dirname, dbname=temp_dict.dbname, opts=temp_dict.options,
                     value_chain=temp_dict.value_chain)
        assert (temp_dict == copy)
        another = PDict.make_temp()
        assert (temp_dict != another)

    def test_getitem_with_empty_dict(self, temp_dict):
        with pytest.raises(NotFound):
            val = temp_dict[b'foo']

    def test_getitem_empty_key(self, temp_dict):
        with pytest.raises(EmptyKey):
            val = temp_dict['']

    def test_get_with_empty_dict(self, temp_dict):
        assert (temp_dict.get(b'foo') is None)
        assert (temp_dict.get(b'foo', u'bar') == u'bar')

    def test_get_empty_key(self, temp_dict):
        with pytest.raises(EmptyKey):
            val = temp_dict.get('')
        with pytest.raises(EmptyKey):
            val = temp_dict.get('', u'foo')

    def test_get_set_only_one(self, temp_dict):
        assert (temp_dict.get(b'foo') is None)
        temp_dict[b'foo'] = u'bar'
        assert (temp_dict.get(b'foo') == u'bar')
        temp_dict[b'foo2'] = u'bar2'
        assert (temp_dict[b'foo2'] == u'bar2')

    def test_setdefault(self, temp_dict):
        with pytest.raises(NotFound):
            val = temp_dict[b'foo']
        temp_dict[b'foo'] = u'bar'
        assert (temp_dict.setdefault(b'foo', u'zogzog') == u'bar')
        assert (temp_dict.setdefault(b'foo2', u'zogzog') == u'zogzog')
        assert (temp_dict[b'foo'] == u'bar')
        assert (temp_dict[b'foo2'] == u'zogzog')

    # noinspection PyCompatibility
    def test_setitem(self, temp_dict):
        all_keys = set()
        all_values = set()
        for i in range(1000):
            all_keys.add(bytes(i))
            all_values.add(u"foo" + unicode(i))
            temp_dict[bytes(i)] = u"foo" + unicode(i)
            temp_dict.noiterkeys()
        assert (set(bytes(k) for k in temp_dict.noiterkeys()) == all_keys)
        assert (set(bytes(v) for v in temp_dict.noitervalues()) == all_values)

    def test_setitem_empty_key_or_value(self, temp_dict):
        with pytest.raises(EmptyKey):
            temp_dict[''] = u'bla'

    def test_delitem_empty(self, temp_dict):
        with pytest.raises(EmptyKey):
            del temp_dict['']
        with pytest.raises(NotFound):
            del temp_dict['foo']

    def test_delitem(self, temp_dict):
        temp_dict['foo'] = u'bar'
        assert (temp_dict['foo'] == u'bar')
        del temp_dict['foo']
        with pytest.raises(NotFound):
            del temp_dict['foo']

    def test_too_long_key(self, temp_dict):
        with pytest.raises(BadValSize):
            temp_dict[600 * b'a'] = "zog"

    def test_erase(self, init_temp_dict):
        with pytest.raises(NotImplementedError):
            init_temp_dict.erase('2', '8')

    def test_noiterkeys_empty(self, temp_dict):
        assert (temp_dict.noiterkeys() == [])

    def test_noitervalues_empty(self, temp_dict):
        assert (temp_dict.noitervalues() == [])

    def test_noiteritems_empty(self, temp_dict):
        assert (temp_dict.noiteritems() == [])

    def test_noiterkeys(self, init_temp_dict):
        assert (init_temp_dict.noiterkeys() == [b"1", b"2", b"4", b"7", b"8", b"9"])
        del init_temp_dict[b'4']
        assert (init_temp_dict.noiterkeys() == [b"1", b"2", b"7", b"8", b"9"])

    def test_noitervalues(self, init_temp_dict):
        assert (init_temp_dict.noitervalues() == [u"bar1", u"bar2", u"bar4", u"bar7", u"bar8", u"bar9"])
        del init_temp_dict[b'7']
        assert (init_temp_dict.noitervalues() == [u"bar1", u"bar2", u"bar4", u"bar8", u"bar9"])

    def test_noiteritems(self, init_temp_dict):
        assert (init_temp_dict.noiteritems() == [
            (b'1', u'bar1'),
            (b'2', u'bar2'),
            (b'4', u'bar4'),
            (b'7', u'bar7'),
            (b'8', u'bar8'),
            (b'9', u'bar9'),
        ])
        del init_temp_dict[b'8']
        assert (init_temp_dict.noiteritems() == [
            (b'1', u'bar1'),
            (b'2', u'bar2'),
            (b'4', u'bar4'),
            (b'7', u'bar7'),
            (b'9', u'bar9'),
        ])

    def test_empty_pop(self, temp_dict):
        with pytest.raises(NotFound):
            temp_dict.pop('foo')

    def test_pop(self, init_temp_dict):
        with pytest.raises(NotFound):
            init_temp_dict.pop('foo')
        assert (init_temp_dict.pop(b'4') == u'bar4')
        with pytest.raises(NotFound):
            init_temp_dict.pop(b'4')
        assert (init_temp_dict.pop(b'7') == u'bar7')

    def test_empty_popitem(self, temp_dict):
        with pytest.raises(EmptyDatabase):
            temp_dict.popitem()

    def test_popitem(self, init_temp_dict):
        assert (init_temp_dict.popitem() == (b'1', u'bar1'))
        assert (init_temp_dict.popitem() == (b'2', u'bar2'))
        assert (init_temp_dict.popitem() == (b'4', u'bar4'))
        assert (init_temp_dict.popitem() == (b'7', u'bar7'))
        assert (init_temp_dict.popitem() == (b'8', u'bar8'))
        assert (init_temp_dict.popitem() == (b'9', u'bar9'))
        with pytest.raises(EmptyDatabase):
            init_temp_dict.popitem()

    def test_empty_iter(self, temp_dict):
        assert (list(iter(temp_dict)) == [])
        assert (list(temp_dict.keys(True)) == [])

    def test_iter(self, init_temp_dict):
        assert (list(iter(init_temp_dict)) == [b'1', b'2', b'4', b'7', b'8', b'9'])
        assert (list(init_temp_dict.keys(True)) == [b'9', b'8', b'7', b'4', b'2', b'1'])

    def test_empty_values(self, temp_dict):
        assert (list(temp_dict.values()) == [])
        assert (list(temp_dict.values(True)) == [])

    def test_values(self, init_temp_dict):
        assert (list(init_temp_dict.values()) == [u'bar1', u'bar2', u'bar4', u'bar7', u'bar8', u'bar9'])
        assert (list(init_temp_dict.values(True)) == [u'bar9', u'bar8', u'bar7', u'bar4', u'bar2', u'bar1'])

    def test_empty_items(self, temp_dict):
        assert (list(temp_dict.items()) == [])
        assert (list(temp_dict.items(True)) == [])

    def test_items(self, init_temp_dict):
        assert (list(init_temp_dict.items()) == [(b'1', u'bar1'), (b'2', u'bar2'), (b'4', u'bar4'), (b'7', u'bar7'),
                                                 (b'8', u'bar8'), (b'9', u'bar9')])

        assert (list(init_temp_dict.items(True)) == [(b'9', u'bar9'), (b'8', u'bar8'), (b'7', u'bar7'), (b'4', u'bar4'),
                                                     (b'2', u'bar2'), (b'1', u'bar1')])

    def test_empty_bool(self, temp_dict):
        assert (bool(temp_dict))

    def test_bool(self, init_temp_dict):
        assert (bool(init_temp_dict))

    def test_empty_len(self, temp_dict):
        assert (len(temp_dict) == 0)

    def test_len(self, init_temp_dict):
        assert (len(init_temp_dict) == 6)

    def test_empty_clear(self, temp_dict):
        assert (len(temp_dict) == 0)
        temp_dict.clear()
        assert (len(temp_dict) == 0)

    def test_clear(self, init_temp_dict):
        assert (len(init_temp_dict) == 6)
        init_temp_dict.clear()
        assert (len(init_temp_dict) == 0)

    def test_empty_contains(self, temp_dict):
        assert (b"foo" not in temp_dict)
        assert (b"1" not in temp_dict)

    def test_contains(self, init_temp_dict):
        assert (b"foo" not in init_temp_dict)
        assert (b"1" in init_temp_dict)
        assert (u"1" in init_temp_dict)

    def test_update(self, temp_dict):
        g = ((bytes(i), u"foo" + unicode(i)) for i in xrange(1000))
        temp_dict.update(g)
        assert (temp_dict.noiterkeys() == sorted([bytes(i) for i in xrange(1000)]))
        assert (temp_dict.noitervalues() == sorted([u'foo' + unicode(i) for i in xrange(1000)]))

    def test_remove_if(self, init_temp_dict):
        for v in init_temp_dict.values():
            assert (not v.startswith(u"z"))
        init_temp_dict[b'10'] = u'zog'
        init_temp_dict[b'bla'] = u'zogi'
        init_temp_dict[b'john'] = u'doh'
        init_temp_dict.remove_if(lambda key, val: val.startswith(u"z"))
        for v in init_temp_dict.values():
            assert (not v.startswith(u"z"))
        assert (b'10' not in init_temp_dict)
        assert (b'bla' not in init_temp_dict)
        assert (init_temp_dict[b'john'] == u'doh')

    def test_empty_transform_values(self, temp_dict):
        assert (len(temp_dict) == 0)
        temp_dict.transform_values(lambda key, val: val + u"foo")
        assert (len(temp_dict) == 0)

    def test_transform_values(self, init_temp_dict):
        f = lambda key, val: val.upper()
        initial_content = dict(init_temp_dict)
        transformed_content = {key: f(key, val) for key, val in initial_content.items()}
        init_temp_dict.transform_values(f)
        assert (dict(init_temp_dict) == transformed_content)

    def test_empty_remove_duplicates(self, temp_dict):
        assert (len(temp_dict) == 0)
        temp_dict.remove_duplicates()
        assert (len(temp_dict) == 0)

    def test_remove_duplicates(self, init_temp_dict):
        l = len(init_temp_dict)
        init_vals = set(init_temp_dict.noitervalues())
        k, v = init_temp_dict.popitem()
        init_temp_dict[b'foo'] = v
        init_temp_dict[b'zog'] = v
        init_temp_dict[b'bla'] = v
        assert (len(init_temp_dict) == (l + 2))
        init_temp_dict.remove_duplicates()
        assert (len(init_temp_dict) == l)
        assert (set(init_temp_dict.noitervalues()) == init_vals)

    def test_write_batch(self, init_temp_dict):
        l = len(init_temp_dict)
        with init_temp_dict.write_batch():
            init_temp_dict[b'foo'] = u'bar'
            with pytest.raises(NotFound):
                val = init_temp_dict[b'foo']
            init_temp_dict[b'foo2'] = u'bar2'
        assert (len(init_temp_dict) == (l + 2))
        assert (init_temp_dict[b'foo'] == u'bar')
        assert (init_temp_dict[b'foo2'] == u'bar2')

    def test_move_to(self, init_temp_dict):
        other = PDict.make_temp(value_chain=init_temp_dict.value_chain)
        other[b'foo'] = u'bar'
        l = len(init_temp_dict)
        init_temp_dict.move_to(other)
        assert (len(init_temp_dict) == 0)
        assert (len(other) == (l + 1))
        assert (other['foo'] == u'bar')


# noinspection PyCompatibility
class TestPDict(object):

    def test_getitem_with_empty_dict(self, temp_all_dict):
        with pytest.raises(NotFound):
            val = temp_all_dict[u'foo']

    def test_getitem_empty_key(self, temp_all_dict):
        if isinstance(temp_all_dict.key_chain.serializer, NoneSerializer) and isinstance(temp_all_dict.key_chain.signer, NoneSigner):
            with pytest.raises(EmptyKey):
                val = temp_all_dict[u'']
        else:
            with pytest.raises(NotFound):
                val = temp_all_dict[u'']

    def test_get_with_empty_dict(self, temp_all_dict):
        assert (temp_all_dict.get(u'foo') is None)
        assert (temp_all_dict.get(u'foo', u'bar') == u'bar')

    def test_get_empty_key(self, temp_all_dict):
        if isinstance(temp_all_dict.key_chain.serializer, NoneSerializer) and isinstance(temp_all_dict.key_chain.signer, NoneSigner):
            with pytest.raises(EmptyKey):
                val = temp_all_dict.get(u'')
            with pytest.raises(EmptyKey):
                val = temp_all_dict.get(u'', u'foo')
        else:
            val = temp_all_dict.get(u'')
            val = temp_all_dict.get(u'', u'foo')

    def test_get_set_only_one(self, temp_all_dict):
        assert (temp_all_dict.get(u'foo') is None)
        temp_all_dict[u'foo'] = u'bar'
        assert (temp_all_dict.get(u'foo') == u'bar')
        temp_all_dict[b'foo2'] = u'bar2'
        assert (temp_all_dict[b'foo2'] == u'bar2')

    def test_setdefault(self, temp_all_dict):
        with pytest.raises(NotFound):
            val = temp_all_dict[u'foo']
        temp_all_dict[u'foo'] = u'bar'
        assert (temp_all_dict.setdefault(u'foo', u'zogzog') == u'bar')
        assert (temp_all_dict.setdefault(u'foo2', u'zogzog') == u'zogzog')
        assert (temp_all_dict[u'foo'] == u'bar')
        assert (temp_all_dict[u'foo2'] == u'zogzog')

    # noinspection PyCompatibility
    def test_setitem(self, temp_all_dict):
        all_keys = set()
        all_values = set()
        for i in range(1000):
            all_keys.add(unicode(i))
            all_values.add(u"foo" + unicode(i))
            temp_all_dict[unicode(i)] = u"foo" + unicode(i)
        assert (set(temp_all_dict.noiterkeys()) == all_keys)
        assert (set(temp_all_dict.noitervalues()) == all_values)

    def test_setitem_empty_key_or_value(self, temp_all_dict):
        if isinstance(temp_all_dict.key_chain.serializer, NoneSerializer) and isinstance(temp_all_dict.key_chain.signer, NoneSigner):
            with pytest.raises(EmptyKey):
                temp_all_dict[u''] = u'bla'
        else:
            temp_all_dict[u''] = u'bla'

    def test_delitem_empty(self, temp_all_dict):
        if isinstance(temp_all_dict.key_chain.serializer, NoneSerializer) and isinstance(temp_all_dict.key_chain.signer, NoneSigner):
            with pytest.raises(EmptyKey):
                del temp_all_dict[u'']
        else:
            with pytest.raises(NotFound):
                del temp_all_dict[u'']
        with pytest.raises(NotFound):
            del temp_all_dict[u'foo']

    def test_delitem(self, temp_all_dict):
        temp_all_dict[u'foo'] = u'bar'
        assert (temp_all_dict[u'foo'] == u'bar')
        del temp_all_dict[u'foo']
        with pytest.raises(NotFound):
            del temp_all_dict[u'foo']

    def test_erase(self, init_temp_all_dict):
        with pytest.raises(NotImplementedError):
            init_temp_all_dict.erase('2', '8')

    def test_noiterkeys_empty(self, temp_all_dict):
        assert (temp_all_dict.noiterkeys() == [])

    def test_noitervalues_empty(self, temp_all_dict):
        assert (temp_all_dict.noitervalues() == [])

    def test_noiteritems_empty(self, temp_all_dict):
        assert (temp_all_dict.noiteritems() == [])

    def test_noiterkeys(self, init_temp_all_dict):
        assert (init_temp_all_dict.noiterkeys() == [u"1", u"2", u"4", u"7", u"8", u"9"])
        del init_temp_all_dict[u'4']
        assert (init_temp_all_dict.noiterkeys() == [u"1", u"2", u"7", u"8", u"9"])

    def test_noitervalues(self, init_temp_all_dict):
        assert (init_temp_all_dict.noitervalues() == [u"bar1", u"bar2", u"bar4", u"bar7", u"bar8", u"bar9"])
        del init_temp_all_dict[u'7']
        assert (init_temp_all_dict.noitervalues() == [u"bar1", u"bar2", u"bar4", u"bar8", u"bar9"])

    def test_noiteritems(self, init_temp_all_dict):
        assert (init_temp_all_dict.noiteritems() == [
            (u'1', u'bar1'),
            (u'2', u'bar2'),
            (u'4', u'bar4'),
            (u'7', u'bar7'),
            (u'8', u'bar8'),
            (u'9', u'bar9'),
        ])
        del init_temp_all_dict[u'8']
        assert (init_temp_all_dict.noiteritems() == [
            (u'1', u'bar1'),
            (u'2', u'bar2'),
            (u'4', u'bar4'),
            (u'7', u'bar7'),
            (u'9', u'bar9'),
        ])

    def test_empty_pop(self, temp_all_dict):
        with pytest.raises(NotFound):
            temp_all_dict.pop('foo')

    def test_pop(self, init_temp_all_dict):
        with pytest.raises(NotFound):
            init_temp_all_dict.pop('foo')
        assert (init_temp_all_dict.pop(u'4') == u'bar4')
        with pytest.raises(NotFound):
            init_temp_all_dict.pop(u'4')
        assert (init_temp_all_dict.pop(u'7') == u'bar7')

    def test_empty_popitem(self, temp_all_dict):
        with pytest.raises(EmptyDatabase):
            temp_all_dict.popitem()

    def test_popitem(self, init_temp_all_dict):
        assert (init_temp_all_dict.popitem() == (u'1', u'bar1'))
        assert (init_temp_all_dict.popitem() == (u'2', u'bar2'))
        assert (init_temp_all_dict.popitem() == (u'4', u'bar4'))
        assert (init_temp_all_dict.popitem() == (u'7', u'bar7'))
        assert (init_temp_all_dict.popitem() == (u'8', u'bar8'))
        assert (init_temp_all_dict.popitem() == (u'9', u'bar9'))
        with pytest.raises(EmptyDatabase):
            init_temp_all_dict.popitem()

    def test_empty_iter(self, temp_all_dict):
        assert (list(iter(temp_all_dict)) == [])
        assert (list(temp_all_dict.keys(True)) == [])

    def test_iter(self, init_temp_all_dict):
        assert (list(iter(init_temp_all_dict)) == [u'1', u'2', u'4', u'7', u'8', u'9'])
        assert (list(init_temp_all_dict.keys(True)) == [u'9', u'8', u'7', u'4', u'2', u'1'])

    def test_empty_values(self, temp_all_dict):
        assert (list(temp_all_dict.values()) == [])
        assert (list(temp_all_dict.values(True)) == [])

    def test_values(self, init_temp_all_dict):
        assert (list(init_temp_all_dict.values()) == [u'bar1', u'bar2', u'bar4', u'bar7', u'bar8', u'bar9'])
        assert (list(init_temp_all_dict.values(True)) == [u'bar9', u'bar8', u'bar7', u'bar4', u'bar2', u'bar1'])

    def test_empty_items(self, temp_all_dict):
        assert (list(temp_all_dict.items()) == [])
        assert (list(temp_all_dict.items(True)) == [])

    def test_items(self, init_temp_all_dict):
        assert (list(init_temp_all_dict.items()) == [(u'1', u'bar1'), (u'2', u'bar2'), (u'4', u'bar4'), (u'7', u'bar7'),
                                                     (u'8', u'bar8'), (u'9', u'bar9')])

        assert (list(init_temp_all_dict.items(True)) == [(u'9', u'bar9'), (b'8', u'bar8'), (u'7', u'bar7'), (u'4', u'bar4'),
                                                         (u'2', u'bar2'), (u'1', u'bar1')])

    def test_empty_bool(self, temp_all_dict):
        assert (bool(temp_all_dict))

    def test_bool(self, init_temp_all_dict):
        assert (bool(init_temp_all_dict))

    def test_empty_len(self, temp_all_dict):
        assert (len(temp_all_dict) == 0)

    def test_len(self, init_temp_all_dict):
        assert (len(init_temp_all_dict) == 6)

    def test_empty_clear(self, temp_all_dict):
        assert (len(temp_all_dict) == 0)
        temp_all_dict.clear()
        assert (len(temp_all_dict) == 0)

    def test_clear(self, init_temp_all_dict):
        assert (len(init_temp_all_dict) == 6)
        init_temp_all_dict.clear()
        assert (len(init_temp_all_dict) == 0)

    def test_empty_contains(self, temp_all_dict):
        assert (u"foo" not in temp_all_dict)
        assert (1 not in temp_all_dict)

    def test_contains(self, init_temp_all_dict):
        assert ("foo" not in init_temp_all_dict)
        assert (u"1" in init_temp_all_dict)
        assert (u"2" in init_temp_all_dict)

    def test_update(self, temp_all_dict):
        g = ((i, u"foo" + unicode(i)) for i in xrange(1000))
        temp_all_dict.update(g)
        assert(len(temp_all_dict) == 1000)
        assert (sorted(temp_all_dict.noitervalues()) == sorted([u'foo' + unicode(i) for i in xrange(1000)]))

    def test_remove_if(self, init_temp_all_dict):
        for v in init_temp_all_dict.values():
            assert (not v.startswith(u"z"))
        init_temp_all_dict[b'10'] = u'zog'
        init_temp_all_dict[b'bla'] = u'zogi'
        init_temp_all_dict[3.14] = u'doh'
        init_temp_all_dict.remove_if(lambda key, val: val.startswith(u"z"))
        for v in init_temp_all_dict.values():
            assert (not v.startswith(u"z"))
        assert (b'10' not in init_temp_all_dict)
        assert (b'bla' not in init_temp_all_dict)
        assert (init_temp_all_dict[3.14] == u'doh')

    def test_empty_transform_values(self, temp_all_dict):
        assert (len(temp_all_dict) == 0)
        temp_all_dict.transform_values(lambda key, val: val + u"foo")
        assert (len(temp_all_dict) == 0)

    def test_transform_values(self, init_temp_all_dict):
        f = lambda key, val: val.upper()
        initial_content = dict(init_temp_all_dict)
        transformed_content = {key: f(key, val) for key, val in initial_content.items()}
        init_temp_all_dict.transform_values(f)
        assert (dict(init_temp_all_dict) == transformed_content)

    def test_empty_remove_duplicates(self, temp_all_dict):
        assert (len(temp_all_dict) == 0)
        temp_all_dict.remove_duplicates()
        assert (len(temp_all_dict) == 0)

    def test_remove_duplicates(self, init_temp_all_dict):
        l = len(init_temp_all_dict)
        init_vals = set(init_temp_all_dict.noitervalues())
        k, v = init_temp_all_dict.popitem()
        init_temp_all_dict[b'foo'] = v
        init_temp_all_dict[b'zog'] = v
        init_temp_all_dict[b'bla'] = v
        assert (len(init_temp_all_dict) == (l + 2))
        init_temp_all_dict.remove_duplicates()
        assert (len(init_temp_all_dict) == l)
        assert (set(init_temp_all_dict.noitervalues()) == init_vals)

    def test_write_batch(self, init_temp_all_dict):
        l = len(init_temp_all_dict)
        with init_temp_all_dict.write_batch():
            init_temp_all_dict[b'foo'] = u'bar'
            with pytest.raises(NotFound):
                val = init_temp_all_dict[b'foo']
            init_temp_all_dict[b'foo2'] = u'bar2'
        assert (len(init_temp_all_dict) == (l + 2))
        assert (init_temp_all_dict[b'foo'] == u'bar')
        assert (init_temp_all_dict[b'foo2'] == u'bar2')

    def test_move_to(self, init_temp_all_dict):
        other = PDict.make_temp(key_chain=init_temp_all_dict.key_chain, value_chain=init_temp_all_dict.value_chain)
        other[u'foo'] = u'bar'
        l = len(init_temp_all_dict)
        init_temp_all_dict.move_to(other)
        assert (len(init_temp_all_dict) == 0)
        assert (len(other) == (l + 1))
        assert (other[u'foo'] == u'bar')

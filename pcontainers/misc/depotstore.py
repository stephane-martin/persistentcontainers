# -*- coding: utf-8 -*-

"""
Provides Depot FileStorage implementation with LMDB.
"""

from __future__ import unicode_literals
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import uuid
import json
from datetime import datetime

try:
    from depot.io.interfaces import FileStorage, StoredFile
    from depot.io import utils
    from depot._compat import unicode_text
except ImportError:
    raise ImportError("filedepot is not installed. try 'pip install filedepot'")

from .. import PRawDict, LmdbOptions, NotFound


class LMDBStoredFile(StoredFile):
    def __init__(self, file_id, files_pdict, metadatas_pdict):
        _check_file_id(file_id)

        if file_id not in metadatas_pdict:
            raise IOError('File %s not existing' % file_id)

        metadata_info = {'filename': 'unnamed',
                         'content_type': 'application/octet-stream',
                         'last_modified': None}

        metadata_content = metadatas_pdict[file_id]
        try:
            metadata_info.update(json.loads(metadata_content))

            last_modified = metadata_info['last_modified']
            if last_modified:
                metadata_info['last_modified'] = datetime.strptime(last_modified, '%Y-%m-%d %H:%M:%S')
        except Exception:
            raise ValueError('Invalid metadata for %s' % file_id)

        self._file = files_pdict.get_direct(file_id)
        super(LMDBStoredFile, self).__init__(file_id=file_id, **metadata_info)

    def read(self, n=-1):
        return self._file.read(n)

    def close(self):
        self._file = None

    @property
    def closed(self):
        return self._file is None


class LMDBFileStorage(FileStorage):
    """
    :class:`depot.io.interfaces.FileStorage` implementation that stores files in LMDB.
    """
    def __init__(self, lmdb_path):
        self.lmdb_path = lmdb_path
        options = LmdbOptions(write_map=True, map_async=True)
        self.files_pdict = PRawDict(self.lmdb_path, "files", opts=options)
        self.metadata_pdict = PRawDict(self.lmdb_path, "metadatas", opts=options)

    def get(self, file_or_id):
        return LMDBStoredFile(self.fileid(file_or_id), self.files_pdict, self.metadata_pdict)

    def __save_file(self, file_id, content, filename, content_type=None):

        if hasattr(content, 'read'):
            c = content.read()
            l = len(c)
            self.files_pdict[file_id] = c
            del c
        else:
            if isinstance(content, unicode_text):
                raise TypeError('Only bytes can be stored, not unicode')
            l = len(content)
            self.files_pdict[file_id] = content

        metadata = {'filename': filename,
                    'content_type': content_type,
                    'content_length': l,
                    'last_modified': utils.timestamp()}

        self.metadata_pdict[file_id] = json.dumps(metadata)

    def create(self, content, filename=None, content_type=None):
        new_file_id = str(uuid.uuid1())
        content, filename, content_type = self.fileinfo(content, filename, content_type)
        self.__save_file(new_file_id, content, filename, content_type)
        return new_file_id

    def replace(self, file_or_id, content, filename=None, content_type=None):
        fileid = self.fileid(file_or_id)
        _check_file_id(fileid)

        # First check file existed and we are not using replace
        # as a way to force a specific file id on creation.
        if fileid not in self.metadata_pdict:
            raise IOError('File %s not existing' % file_or_id)

        content, filename, content_type = self.fileinfo(content, filename, content_type,
                                                        lambda: self.get(fileid))

        self.delete(fileid)
        self.__save_file(fileid, content, filename, content_type)
        return fileid

    def delete(self, file_or_id):
        fileid = self.fileid(file_or_id)
        _check_file_id(fileid)

        try:
            del self.metadata_pdict[fileid]
            del self.files_pdict[fileid]
        except NotFound:
            pass

    def exists(self, file_or_id):
        fileid = self.fileid(file_or_id)
        _check_file_id(fileid)
        return fileid in self.metadata_pdict

    def list(self):
        return self.metadata_pdict.noiterkeys()


def _check_file_id(file_id):
    # Check that the given file id is valid, this also
    # prevents unsafe paths.
    try:
        uuid.UUID('{%s}' % file_id)
    except:
        raise ValueError('Invalid file id %s' % file_id)


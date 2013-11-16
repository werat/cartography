import os
import fcntl

import json

from extsort import external_sort


_metadata_directory = 'METADATA'
_data_directory = 'CHUNKS'


class Metadata(object):
    def __init__(self, filename):
        self.fd = open(filename, 'r+')
        self.data = None

    def __enter__(self):
        fcntl.flock(self.fd, fcntl.LOCK_EX)
        self.data = json.load(self.fd)
        return self.data

    def __exit__(self, type, value, traceback):
        self.fd.seek(0)
        json.dump(self.data, self.fd, indent=True, separators=(',', ': '))
        self.fd.truncate()
        self.fd.close()



class GeminiFileSystem(object):

    def __init__(self, working_dir='.'):
        self.dir = working_dir
        self._ensure_directory(os.path.join(self.dir, _metadata_directory))


    def _ensure_directory(self, directory):
        if not os.path.isdir(directory):
            os.makedirs(directory)

    def _get_metadata_file(self, table):
        return os.path.join(self.dir, _metadata_directory, table)

    def metadata_exists(self, table):
        return os.path.isfile(self._get_metadata_file(table))

    def open_metadata(self, table):
        return Metadata(self._get_metadata_file(table))

    def read_metadata(self, table):
        with open(self._get_metadata_file(table)) as fd:
            fcntl.flock(fd, fcntl.LOCK_SH)
            return json.load(fd)


class GeminiFileSystemNode(GeminiFileSystem):

    def __init__(self, node, working_dir='.'):
        super(GeminiFileSystemNode, self).__init__(working_dir)

        self.node = node
        self._ensure_directory(os.path.join(self.dir, _data_directory))


    def _empty_chunk_metadata(self, table):
        return {
            'id':               self.node.name + '~' + table,
            'node':             self.node.name,
            'table':            table,
            'records_count':    0,
            'size':             0,
        }

    def _get_chunk_file(self, table):
        return os.path.join(self.dir, _data_directory, table)

    def open_chunk(self, table, mode='r'):
        fd = open(self._get_chunk_file(table), mode)
        fcntl.flock(fd, fcntl.LOCK_SH if 'r' in mode else fcntl.LOCK_EX)
        return fd

    def real_chunk_size(self, table):
        return os.stat(self._get_chunk_file(table)).st_size

    def sort_chunk(self, table):
        chunk_file = self._get_chunk_file(table)
        external_sort(chunk_file, chunk_file)

    def remove_chunk(self, table):
        os.remove(self._get_metadata_file(table))
        os.remove(self._get_chunk_file(table))

    def create_empty_chunk(self, table):
        with open(self._get_metadata_file(table), 'w') as f:
            json.dump(self._empty_chunk_metadata(table), f, indent=True, separators=(',', ': '))
        with open(self._get_chunk_file(table), 'w') as f:
            pass
        return self._empty_chunk_metadata(table)


class GeminiFileSystemMaster(GeminiFileSystem):

    def __init__(self, communicator, working_dir='.'):
        super(GeminiFileSystemMaster, self).__init__(working_dir)
        self.communicator = communicator


    def _empty_metadata(self, table):
        return {
            'table':            table,
            'records_count':    0,
            'size':             0,
            'sorted':           False,

            'chunks':           {},
            'chunks_order':     [],
        }


    def remove_table(self, table):
        for _, chunk in self.read_metadata(table)['chunks'].items():
            self.communicator.request(chunk['node'], 'NODE_OP_DELETE_CHUNK', table)
        os.remove(self._get_metadata_file(table))


    def create_empty_table(self, table):
        with open(self._get_metadata_file(table), 'w') as f:
            json.dump(self._empty_metadata(table), f, indent=True, separators=(',', ': '))
        return self._empty_metadata(table)

    def reset_metadata(self, table):
        with self.open_metadata(table) as metadata:
            metadata['chunks_order'] = []
            metadata['sorted']       = False

            metadata['chunks']           = {}
            metadata['records_count']    = 0
            metadata['size']             = 0

    def update_metadata(self, chunk_metadata, sorted_append=False):
        chunk_id    = chunk_metadata['id']
        table       = chunk_metadata['table']

        with self.open_metadata(table) as metadata:

            # TODO: chunks should be unique with different ids
            # assert chunk_id not in metadata['chunks_order']

            if chunk_id in metadata['chunks_order']:
                metadata['chunks_order'].remove(chunk_id)

            metadata['chunks_order'].append(chunk_id)
            metadata['sorted'] = sorted_append

            metadata['chunks'][chunk_id] = chunk_metadata
            metadata['records_count']    = 0
            metadata['size']             = 0

            for _, chunk in metadata['chunks'].items():
                metadata['records_count'] += chunk['records_count']
                metadata['size']          += chunk['size']

#!/usr/bin/env python

import os
import sys
import shutil
import math
import random
import json

import shlex
import subprocess
from threading import Thread

import network
import fs

from communicator import NodeCommunicator, MasterCommunicator


class CartographyNodeHandler(object):

    def _map_operation(self, conn, m):
        transaction_id  = m['transaction_id']
        input_table     = m['input_table']
        output_table    = m['output_table']
        map_command     = m['map_command']
        replace_mode    = m['replace_mode'] if 'replace_mode' in m else False

        self.node.run_map(transaction_id, input_table, output_table, map_command, replace_mode)
        conn.send('MAP_OPERATION OK')

    def _partition_operation(self, conn, m):
        input_table  = m['input_table']
        output_table = m['output_table']
        routes       = m['routes']

        self.node.run_partition(input_table, output_table, routes)
        conn.send('PARTITION_OPERATION OK')

    def _op_transaction_end(self, conn, m):
        transaction_id = m['transaction_id']

        resu = self.node._op_transaction_end(transaction_id)
        conn.send('OK' if resu else 'FAILED')

    def _op_upload_file(self, conn, m):
        transaction_id = m['transaction_id']
        filename = m['filename']
        data = m['data']

        resu = self.node._op_upload_file(transaction_id, filename, data)
        conn.send('OK' if resu else 'FAILED')

    def _rq_chunk_metadata(self, conn, m):
        table = m['table']
        metadata = self.node.fs.read_metadata(table)
        conn.send(json.dumps(metadata))

    def _delete_chunk(self, conn, m):
        table = m['table']
        if self.node.fs.metadata_exists(table):
            self.node.fs.remove_chunk(table)
        conn.send('DELETE_CHUNK OK')

    def _read_chunk(self, conn, m):
        table = m['table']
        if self.node.fs.metadata_exists(table):

            block_size = 1024 * 1024 * 8

            with self.node.fs.open_chunk(table) as fi:
                while True:
                    chunk = fi.readlines(block_size)
                    if not chunk:
                        break

                    chunk = [l[:-1] for l in chunk]  # strip just ONE newline
                    try:
                        conn.send(json.dumps({'chunk': chunk}))
                    except IOError:
                        # client probably disconnected
                        break

            # TODO: bring back when all chunk are unique and small
            # conn.send('READ_CHUNK OK')

    def _write_chunk(self, conn, m):
        table = m['table']
        chunk = m['chunk']

        if not self.node.fs.metadata_exists(table):
            self.node.fs.create_empty_chunk(table)

        with self.node.fs.open_chunk(table, 'a') as fo:
            for line in chunk:
                fo.write(line + '\n')

        with self.node.fs.open_metadata(table) as meta:
            meta['records_count'] += len(chunk)
            meta['size'] = self.node.fs.real_chunk_size(table)

        self.node.notify_chunk_written(meta)
        conn.send('WRITE_CHUNK OK')

    def _rq_get_sort_sample(self, conn, m):
        table = m['table']

        # TODO: sort small tables in-place
        meta = self.node.fs.read_metadata(table)
        records_count = meta['records_count']
        sample_length = min(math.log10(records_count) * 20, records_count)

        indexes = set()
        while len(indexes) < sample_length:
            indexes.add(random.randrange(0, records_count))

        with self.node.fs.open_chunk(table) as fi:
            chunk = [l[:l.index('\t')] for i, l in enumerate(fi) if i in indexes]  # take just key
            conn.send(json.dumps({'chunk': chunk}))

    def _op_sort_chunk(self, conn, m):
        table = m['table']

        self.node.fs.sort_chunk(table)
        conn.send('SORT_CHUNK OK')

    def __init__(self, node):
        self.node = node
        self.handlers = {
            'NODE_OP_MAP'               : self._map_operation,
            'NODE_OP_PARTITION'         : self._partition_operation,

            'NODE_OP_TRANSACTION_END'   : self._op_transaction_end,

            'NODE_OP_UPLOAD_FILE'       : self._op_upload_file,

            'NODE_RQ_READ_CHUNK'        : self._read_chunk,
            'NODE_OP_WRITE_CHUNK'       : self._write_chunk,
            'NODE_OP_DELETE_CHUNK'      : self._delete_chunk,

            'NODE_RQ_CHUNK_METADATA'    : self._rq_chunk_metadata,

            'NODE_RQ_SORT_SAMPLE'       : self._rq_get_sort_sample,
            'NODE_OP_SORT_CHUNK'        : self._op_sort_chunk,
        }

    def handle_request(self, conn):
        try:
            message = json.loads(conn.receive())
            message_type = message['operation']

            print 'Message from {0} : {1}'.format(conn.get_peername(), message_type)

            if message_type not in self.handlers:
                conn.send('UNKNOWN OPERATION')
            else:
                self.handlers[message_type](conn, message)

        finally:
            conn.close()


class CartographyNode(object):

    def __init__(self, name, master_endpoint, port=0, local_mode=True, working_dir='.'):
        self.name = name
        self.working_dir = working_dir
        self.master = MasterCommunicator(master_endpoint)

        self.fs = fs.GeminiFileSystemNode(self, working_dir)
        self.handler = CartographyNodeHandler(self)

        self.listener = network.ConnectionListener(port, local_mode)
        self.host, self.port = self.listener.getsockname()
        self._register_on_master()

    def _transaction_dir(self, transaction_id):
        return os.path.join(self.working_dir, transaction_id)

    def _register_on_master(self):
        self.master.request('INT_NODE_GREET', self.name, self.port)

    def notify_chunk_written(self, chunk_metadata):
        self.master.request('INT_CHUNK_WRITTEN', chunk_metadata)

    def run_map(self, transaction_id, input_table, output_table, command, replace_mode=False):
        self.fs.create_empty_chunk(output_table)

        cwd = self._transaction_dir(transaction_id)
        if not os.path.isdir(cwd):
            cwd = None

        records_count = 0
        with self.fs.open_chunk(input_table) as fi:
            p = subprocess.Popen(shlex.split(command), cwd=cwd, stdin=fi, stdout=subprocess.PIPE)
            with self.fs.open_chunk(output_table, 'w') as fo:
                # TODO: add timeout and kill jobs
                for line in p.stdout:
                    fo.write(line)
                    records_count += 1
            p.wait()

        with self.fs.open_metadata(output_table) as meta:
            meta['records_count'] = records_count
            meta['size'] = self.fs.real_chunk_size(output_table)

    def run_partition(self, input_table, output_table, routes):
        nodes = [(node, routes[node]['endpoint']) for node in routes]
        communicator = NodeCommunicator(nodes)

        def key(record):
            return line[:line.index('\t')]

        def find_shard(record):
            k = key(record)
            for node, value in routes.iteritems():
                if value['lower'] <= k and (k <= value['upper'] or value['upper'] is None):
                    return node

        buffers = {}

        with self.fs.open_chunk(input_table) as fi:
            for line in fi:
                record = line[:-1]  # strip just ONE newline
                node = find_shard(record)
                buffers.setdefault(node, []).append(record)

                if len(buffers[node]) > 1024 * 256:  # chunk_size ~= 8 MB, average record size ~= 32 B
                    communicator.request(node, 'NODE_OP_WRITE_CHUNK', output_table, buffers[node])
                    buffers[node] = []

        for node in buffers:
            if buffers[node]:
                communicator.request(node, 'NODE_OP_WRITE_CHUNK', output_table, buffers[node])
                buffers[node] = []

    def _op_transaction_end(self, transaction_id):
        shutil.rmtree(self._transaction_dir(transaction_id))

    def _op_upload_file(self, transaction_id, filename, data):
        directory = self._transaction_dir(transaction_id)

        # TODO: race
        if not os.path.isdir(directory):
            os.makedirs(directory)

        with open(os.path.join(directory, filename), 'wb') as fo:
            fo.write(data)
        return True

    def loop(self):
        try:
            for conn in self.listener.accept_connections(10):
                Thread(target=self.handler.handle_request, args=(conn,)).start()

        except KeyboardInterrupt:
            print 'Stopping node...'
        finally:
            self.listener.socket.close()

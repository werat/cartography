#!/usr/bin/env python

import os
import sys
import shutil

import uuid
import random
import json

from threading import Thread, Lock

import network
import fs

from communicator import NodeCommunicator, AsyncTask


class CartographyMasterHandler(object):

    def _map(self, conn, m):
        transaction_id  = m['transaction_id']
        input_table     = m['input_table']
        output_table    = m['output_table']
        map_command     = m['map_command']
        replace_mode    = m['replace_mode'] if 'replace_mode' in m else False

        self.master._map(transaction_id, input_table, output_table, map_command, replace_mode)
        conn.send('OP_MAP OK')

    def _sort(self, conn, m):
        input_table  = m['input_table']
        output_table = m['output_table']

        self.master._sort(input_table, output_table)
        conn.send('OP_SORT OK')

    def _remove_table(self, conn, m):
        table = m['table']

        self.master._remove_table(table)
        conn.send('OP_REMOVE_TABLE OK')

    def _rq_transaction_start(self, conn, m):
        transaction_id = self.master._rq_transaction_start()
        conn.send(transaction_id or '')
        # responce = {'result': resu, 'transaction_id': transaction_id}
        # conn.send(json.dumps(responce))

    def _op_transaction_end(self, conn, m):
        transaction_id = m['transaction_id']

        resu = self.master._op_transaction_end(transaction_id)
        responce = {'result': resu}
        conn.send(json.dumps(responce))

    def _op_lock_table(self, conn, m):
        transaction_id = m['transaction_id']
        table = m['table']

        resu = self.master._op_lock_table(transaction_id, table)
        conn.send('OK' if resu else 'FAILED')

    def _op_unlock_table(self, conn, m):
        transaction_id = m['transaction_id']
        table = m['table']

        resu = self.master._op_unlock_table(transaction_id, table)
        conn.send('OK' if resu else 'FAILED')

    def _op_upload_file(self, conn, m):
        transaction_id = m['transaction_id']
        filename = m['filename']
        data = m['data']

        resu = self.master._op_upload_file(transaction_id, filename, data)
        conn.send('OK' if resu else 'FAILED')

    def _rq_table_metadata(self, conn, m):
        table = m['table']
        table_metadata = self.master._read_table_metadata(table)
        conn.send(table_metadata)

    def _rq_random_holder(self, conn, m):
        node_name = self.master._random_holder()
        conn.send(node_name.encode('utf8'))

    def _rq_node_address(self, conn, m):
        node = m['node']
        node_address = self.master.communicator.nodes[node]
        conn.send(node_address.encode('utf8'))


    def _int_chunk_written(self, conn, m):
        chunk_metadata = m['chunk_metadata']
        table          = chunk_metadata['table']

        if not self.master.fs.metadata_exists(table):
            self.master.fs.create_empty_table(table)

        self.master.fs.update_metadata(chunk_metadata)
        conn.send('INT_CHUNK_WRITTEN OK')

    def _int_node_greet(self, conn, m):
        node_name = m['name']
        node_port = m['port']

        node_endpoint = network.endpoint_from_tuple((conn.get_peeraddress(), node_port))

        # if node_endpoint.startswith('127'):
        #     node_endpoint = '192.168.0.101:'+str(node_port)

        self.master._int_node_greet(node_name, node_endpoint)
        conn.send('INT_NODE_GREET OK')


    def __init__(self, master):
        self.master = master
        self.handlers = {
            'OP_MAP'                : self._map,
            'OP_SORT'               : self._sort,
            'OP_REMOVE_TABLE'       : self._remove_table,

            'RQ_TRANSACTION_START'  : self._rq_transaction_start,
            'OP_TRANSACTION_END'    : self._op_transaction_end,

            'OP_LOCK_TABLE'         : self._op_lock_table,
            'OP_UNLOCK_TABLE'       : self._op_unlock_table,

            'OP_UPLOAD_FILE'        : self._op_upload_file,

            'RQ_TABLE_METADATA'     : self._rq_table_metadata,
            'RQ_RANDOM_HOLDER'      : self._rq_random_holder,
            'RQ_NODE_ADDRESS'       : self._rq_node_address,

            'INT_CHUNK_WRITTEN'     : self._int_chunk_written,
            'INT_NODE_GREET'        : self._int_node_greet,
        }

    def handle_request(self, conn):
        try:
            message = json.loads(conn.receive())
            message_type = message['operation']

            print 'Message from {0} : {1}'.format(conn.get_peername(), message_type)

            if message_type in self.handlers:
                self.handlers[message_type](conn, message)
            else:
                conn.send('UNKNOWN_OPERATION')

        finally:
            conn.close()

class Transaction(object):
    def __init__(self, t_id):
        self.id = t_id
        self.locked_tables = []
        self.affected_nodes = []

    def __hash__(self):
        return self.id
    def __eq__(self):
        return self.id

class CartographyMaster(object):

    def __init__(self, port=8090, local_mode=True, working_dir='.'):
        self.port = port
        self.local_mode = local_mode

        self.working_dir=working_dir

        self.communicator = NodeCommunicator()
        self.fs = fs.GeminiFileSystemMaster(self.communicator, working_dir)
        self.handler = CartographyMasterHandler(self)

        self.transactions = {}
        self.transactions_lock = Lock()

        self.locked_tables = set()
        self.locked_tables_lock = Lock()

    def _transaction_dir(self, transaction_id):
        return os.path.join(self.working_dir, transaction_id)

    def _run_async_tasks(self, method, args):
        tasks = []
        for arg in args:
            task = AsyncTask(target=method, args=(arg,))
            tasks.append((task, arg))

        for task, arg in tasks:
            yield arg, task.wait()


    def _rq_transaction_start(self):
        try:
            self.transactions_lock.acquire()

            transaction = Transaction(uuid.uuid4().hex)
            self.transactions[transaction.id] = transaction
            os.mkdir(self._transaction_dir(transaction.id))

            return transaction.id
        finally:
            self.transactions_lock.release()

        return None

    def _op_transaction_end(self, transaction_id):
        try:
            self.transactions_lock.acquire()

            transaction = self.transactions[transaction_id]

            for table in transaction.locked_tables:
                self._op_unlock_table(transaction.id, table)

            for node in transaction.affected_nodes:
                self.communicator.request(node, 'NODE_OP_TRANSACTION_END', transaction_id)

            self.transactions.pop(transaction_id)
            shutil.rmtree(self._transaction_dir(transaction_id))

        finally:
            self.transactions_lock.release()

        return True


    def _op_lock_table(self, transaction_id, table):
        try:
            self.transactions_lock.acquire()
            self.locked_tables_lock.acquire()

            if transaction_id in self.transactions and table not in self.locked_tables:
                self.locked_tables.add(table)
                self.transactions[transaction_id].locked_tables.append(table)
                return True

        finally:
            self.locked_tables_lock.release()
            self.transactions_lock.release()

        return False

    def _op_unlock_table(self, transaction_id, table):
        try:
            self.transactions_lock.acquire()
            self.locked_tables_lock.acquire()

            transaction = self.transactions[transaction_id]

            if table in self.locked_tables and table in transaction.locked_tables:
                self.locked_tables.remove(table)
                transaction.locked_tables.remove(table)
                return True

        finally:
            self.locked_tables_lock.release()
            self.transactions_lock.release()

        return False

    def _op_upload_file(self, transaction_id, filename, data):
        # TODO: race
        if transaction_id not in self.transactions:
            return False

        transaction = self.transactions[transaction_id]

        for node, _ in self.communicator.broadcast('NODE_OP_UPLOAD_FILE', transaction_id, filename, data):
            transaction.affected_nodes.append(node)


        # directory = self._transaction_dir(transaction_id)
        # with open(os.path.join(directory, filename), 'w') as fo:
        #     fo.write(data)
        return True


    def _map(self, transaction_id, input_table, output_table, map_command, replace_mode=False):
        if not self.fs.metadata_exists(input_table):
            raise Exception("Input table {0} does not exist!".format(input_table))
        if self.fs.metadata_exists(output_table):
            if replace_mode:
                self.fs.remove_table(output_table)
            else:
                raise Exception("Output table {0} does exist! Set replace_mode=True to replace it.".format(output_table))
        if input_table == output_table:
            raise Exception("Can't map to self.")

        self.fs.create_empty_table(output_table)

        def run_map_on_node(node):
            resu = self.communicator.request(node, 'NODE_OP_MAP', transaction_id, input_table, output_table, map_command, replace_mode)
            resu = self.communicator.request(node, 'NODE_RQ_CHUNK_METADATA', output_table)
            return json.loads(resu)

        nodes = [chunk['node'] for _, chunk in self.fs.read_metadata(input_table)['chunks'].items()]
        for node, chunk_meta in self._run_async_tasks(run_map_on_node, nodes):
            self.fs.update_metadata(chunk_meta)

    def _sort(self, input_table, output_table):
        if not self.fs.metadata_exists(input_table):
            raise Exception("Input table {0} does not exist!".format(input_table))

        if self.fs.metadata_exists(output_table):
            self.fs.remove_table(output_table)

        nodes = [chunk['node'] for _, chunk in self.fs.read_metadata(input_table)['chunks'].items()]

        def _get_sort_sample(node):
            resu = self.communicator.request(node, 'NODE_RQ_SORT_SAMPLE', input_table)
            return json.loads(resu)

        sample = [
            key
            for node, chunk_sample in self._run_async_tasks(_get_sort_sample, nodes)
            for key in chunk_sample['chunk']
        ]
        sample.sort()

        # random order to nodes
        target_nodes = self.communicator.nodes.keys()
        random.shuffle(target_nodes)

        def _medians(l, n):
            step = (len(l)-1)/(n+1)+1
            return [l[i] for i in range(step, len(l), step)]

        medians = _medians(sample, len(target_nodes)-1)
        medians = [None] + medians + [None]

        route_data = {}
        for i, node in enumerate(target_nodes):
            route_data[node] = {
                'endpoint': self.communicator.nodes[node],
                'lower':    medians[i],
                'upper':    medians[i+1]
            }

        self.fs.create_empty_table(output_table)

        def _run_partition_on_node(node):
            return self.communicator.request(node, 'NODE_OP_PARTITION', input_table, output_table, route_data)

        def _run_sort_on_node(node):
            resu = self.communicator.request(node, 'NODE_OP_SORT_CHUNK', output_table)
            resu = self.communicator.request(node, 'NODE_RQ_CHUNK_METADATA', output_table)
            return json.loads(resu)

        for node, chunk_meta in self._run_async_tasks(_run_partition_on_node, nodes):
            print 'node {0} finished partition'.format(node)

        self.fs.reset_metadata(output_table)

        for node, chunk_meta in self._run_async_tasks(_run_sort_on_node, target_nodes):
            print 'node {0} finished sort'.format(node)
            self.fs.update_metadata(chunk_meta, sorted_append=True)


    def _remove_table(self, table):
        if not self.fs.metadata_exists(table):
            print 'Table does not exist: {0}'.format(table)
        else:
            self.fs.remove_table(table)


    def _read_table_metadata(self, table):
        return json.dumps(self.fs.read_metadata(table))

    def _random_holder(self):
        return random.choice(self.communicator.nodes.values())

    def _int_node_greet(self, node_name, node_endpoint):
        print "node connected: {0} : {1}".format(node_name, node_endpoint)
        self.communicator.register_node(node_name, node_endpoint)


    def loop(self):
        listener = network.ConnectionListener(self.port, self.local_mode)

        try:
            for conn in listener.accept_connections(10):
                Thread(target=self.handler.handle_request, args=(conn,)).start()

        except KeyboardInterrupt:
            print 'Stopping master...'
        finally:
            listener.socket.close()

            for node in self.communicator.nodes:
                self.communicator.request(node, 'NODE_OP_SHUTDOWN')

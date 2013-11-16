#!/usr/bin/env python

import os
import sys
import json

import network
from communicator import MasterCommunicator


def start_transaction_or_fail(master):
    t_id = master.request('RQ_TRANSACTION_START')
    if not t_id:
        print 'Failed to start transaction'
        sys.exit(1)

    return t_id


def end_transaction(master, transaction_id):
    master.request('OP_TRANSACTION_END', transaction_id)


def lock_table_or_fail(master, transaction_id, table):
    lock = master.request('OP_LOCK_TABLE', transaction_id, table)
    if lock != 'OK':
        print 'Failed to lock table:', table
        print 'End transaction:', transaction_id
        end_transaction(master, transaction_id)
        sys.exit(1)


def unlock_table(master, transaction_id, table):
    master.request('OP_UNLOCK_TABLE', transaction_id, table)


if __name__ == '__main__':

    if sys.argv[1] == 'master':
        from master import CartographyMaster

        working_dir = sys.argv[2] if len(sys.argv) > 2 else '.'
        host = CartographyMaster(8090, False, working_dir=working_dir)
        host.loop()

    if sys.argv[1] == 'node':
        from node import CartographyNode

        node_name   = sys.argv[2]
        master_host = sys.argv[3]
        working_dir = sys.argv[4] if len(sys.argv) > 4 else '.'

        host = CartographyNode(node_name, master_host + ':8090', working_dir=working_dir, local_mode=False)
        host.loop()

    master = MasterCommunicator('localhost:8090')

    if sys.argv[1] == 'map':
        input_table = sys.argv[2]

        t_id = start_transaction_or_fail(master)
        lock_table_or_fail(master, t_id, input_table)

        for f in sys.argv[5:]:
            filename = os.path.basename(f)
            bin = open(f, 'rb').read()
            master.request('OP_UPLOAD_FILE', t_id, filename, bin)

        master.request('OP_MAP', t_id, input_table, sys.argv[3], sys.argv[4].strip("\'\""), True)

        unlock_table(master, t_id, input_table)
        end_transaction(master, t_id)

    if sys.argv[1] == 'sort':
        input_table = sys.argv[2]

        t_id = start_transaction_or_fail(master)
        lock_table_or_fail(master, t_id, input_table)

        master.request('OP_SORT', input_table, sys.argv[3])

        unlock_table(master, t_id, input_table)
        end_transaction(master, t_id)

    if sys.argv[1] == 'drop':
        input_table = sys.argv[2]

        t_id = start_transaction_or_fail(master)
        lock_table_or_fail(master, t_id, input_table)

        master.request('OP_REMOVE_TABLE', input_table)

        unlock_table(master, t_id, input_table)
        end_transaction(master, t_id)

    if sys.argv[1] == 'read':
        table = sys.argv[2]

        t_id = start_transaction_or_fail(master)
        lock_table_or_fail(master, t_id, table)

        resu = master.request('RQ_TABLE_METADATA', table)
        table_metadata = json.loads(resu)

        for chunk_id in table_metadata['chunks_order']:
            chunk = table_metadata['chunks'][chunk_id]

            node_endpoint = master.request('RQ_NODE_ADDRESS', chunk['node'])

            with network.create_connection(node_endpoint) as c:
                j = {'table': chunk['table'], 'operation': 'NODE_RQ_READ_CHUNK'}
                c.send(json.dumps(j))

                while True:
                    received = c.receive()
                    if not received:
                        break

                    try:
                        for line in json.loads(received)['chunk']:
                            sys.stdout.write(line + '\n')
                    except IOError:
                        unlock_table(master, t_id, table)
                        end_transaction(master, t_id)
                        sys.exit(0)

        unlock_table(master, t_id, table)
        end_transaction(master, t_id)

    if sys.argv[1] == 'write':
        table = sys.argv[2]

        t_id = start_transaction_or_fail(master)
        lock_table_or_fail(master, t_id, table)

        def send_chunk(chunk):
            node = master.request('RQ_RANDOM_HOLDER')

            j = {'table': table, 'operation': 'NODE_OP_WRITE_CHUNK', 'chunk': chunk}
            network.request(node, j)

        chunk_size = 0
        chunk = []
        for line in sys.stdin:
            line = line[:-1]  # strip just ONE newline
            chunk_size += len(line)
            chunk.append(line)

            if chunk_size >= 1024 * 1024 * 2:
                send_chunk(chunk)
                chunk_size = 0
                chunk = []

        if len(chunk) > 0:
            send_chunk(chunk)

        unlock_table(master, t_id, table)
        end_transaction(master, t_id)

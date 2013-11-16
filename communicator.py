from threading import Thread

import network


class AsyncTask(Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs={}, Verbose=None):
        super(AsyncTask, self).__init__(group, target, name, args, kwargs, Verbose)
        self._return = None
        self.start()

    def run(self):
        if self._Thread__target is not None:
            self._return = self._Thread__target(*self._Thread__args, **self._Thread__kwargs)

    def wait(self):
        Thread.join(self)
        return self._return


def _node_op_map(transaction_id, input_table, output_table, map_command, replace_mode=False):
    return {
        'operation':        'NODE_OP_MAP',
        'transaction_id':   transaction_id,
        'input_table':      input_table,
        'output_table':     output_table,
        'map_command':      map_command,
        'replace_mode':     replace_mode
    }
def _node_op_partition(input_table, output_table, routes):
    return {
        'operation':        'NODE_OP_PARTITION',
        'input_table':      input_table,
        'output_table':     output_table,
        'routes':           routes
    }

def _node_op_transaction_end(transaction_id):
    return {
        'operation'         : 'NODE_OP_TRANSACTION_END',
        'transaction_id'    : transaction_id
    }

def _node_rq_read_chunk(table):
    return {
        'operation': 'NODE_RQ_READ_CHUNK',
        'table': table
    }
def _node_op_write_chunk(table, chunk):
    return {
        'operation':    'NODE_OP_WRITE_CHUNK',
        'table':        table,
        'chunk':        chunk
    }
def _node_op_delete_chunk(table):
    return {
        'operation':    'NODE_OP_DELETE_CHUNK',
        'table':        table
    }

def _node_op_upload_file(transaction_id, filename, data):
    return {
        'operation'         : 'NODE_OP_UPLOAD_FILE',
        'transaction_id'    : transaction_id,
        'filename'          : filename,
        'data'              : data
    }

def _node_rq_chunk_metadata(table):
    return {
        'operation':    'NODE_RQ_CHUNK_METADATA',
        'table':        table
    }

def _node_rq_sort_sample(table):
    return {
        'operation':    'NODE_RQ_SORT_SAMPLE',
        'table':        table
    }
def _node_op_sort_chunk(table):
    return {
        'operation':    'NODE_OP_SORT_CHUNK',
        'table':        table
    }

def _node_op_shutdown():
    return {
        'operation':    'NODE_OP_SHUTDOWN'
    }


def _op_map(transaction_id, input_table, output_table, map_command, replace_mode=False):
    return {
        'operation'     : 'OP_MAP',
        'transaction_id': transaction_id,
        'input_table'   : input_table,
        'output_table'  : output_table,
        'map_command'   : map_command,
        'replace_mode'  : replace_mode
    }
def _op_sort(input_table, output_table):
    return {
        'operation'     : 'OP_SORT',
        'input_table'   : input_table,
        'output_table'  : output_table
    }
def _op_remove_table(table):
    return {
        'operation'     : 'OP_REMOVE_TABLE',
        'table'         : table
    }

def _rq_transaction_start():
    return {
        'operation' : 'RQ_TRANSACTION_START'
    }
def _op_transaction_end(transaction_id):
    return {
        'operation'         : 'OP_TRANSACTION_END',
        'transaction_id'    : transaction_id
    }

def _op_lock_table(transaction_id, table):
    return {
        'operation'         : 'OP_LOCK_TABLE',
        'transaction_id'    : transaction_id,
        'table'             : table
    }
def _op_unlock_table(transaction_id, table):
    return {
        'operation'         : 'OP_UNLOCK_TABLE',
        'transaction_id'    : transaction_id,
        'table'             : table
    }

def _op_upload_file(transaction_id, filename, data):
    return {
        'operation'         : 'OP_UPLOAD_FILE',
        'transaction_id'    : transaction_id,
        'filename'          : filename,
        'data'              : data
    }

def _rq_table_metadata(table):
    return {
        'operation'     : 'RQ_TABLE_METADATA',
        'table'         : table
    }
def _rq_random_holder():
    return {
        'operation'     : 'RQ_RANDOM_HOLDER'
    }
def _rq_node_address(node):
    return {
        'operation' : 'RQ_NODE_ADDRESS',
        'node'      : node
    }

def _int_chunk_written(chunk_metadata):
    return {
        'operation'         : 'INT_CHUNK_WRITTEN',
        'chunk_metadata'    : chunk_metadata
    }
def _int_node_greet(name, port):
    return {
        'operation' : 'INT_NODE_GREET',
        'name'      : name,
        'port'      : port,
    }

class MasterCommunicator(object):
    def __init__(self, master_endpoint):
        self.messages = {
            'OP_MAP'                : _op_map,
            'OP_SORT'               : _op_sort,
            'OP_REMOVE_TABLE'       : _op_remove_table,

            'RQ_TRANSACTION_START'  : _rq_transaction_start,
            'OP_TRANSACTION_END'    : _op_transaction_end,

            'OP_LOCK_TABLE'         : _op_lock_table,
            'OP_UNLOCK_TABLE'       : _op_unlock_table,

            'OP_UPLOAD_FILE'        : _op_upload_file,

            'RQ_TABLE_METADATA'     : _rq_table_metadata,
            'RQ_RANDOM_HOLDER'      : _rq_random_holder,
            'RQ_NODE_ADDRESS'       : _rq_node_address,

            'INT_CHUNK_WRITTEN'     : _int_chunk_written,
            'INT_NODE_GREET'        : _int_node_greet,
        }
        self.master_endpoint = master_endpoint

    def request(self, message_type, *args):
        message = self.messages[message_type](*args)
        return network.request(self.master_endpoint, message)

class NodeCommunicator(object):

    def __init__(self, nodes=[]):
        self.messages = {
            'NODE_OP_MAP'               : _node_op_map,
            'NODE_OP_PARTITION'         : _node_op_partition,

            'NODE_OP_TRANSACTION_END'   : _node_op_transaction_end,

            'NODE_RQ_READ_CHUNK'        : _node_rq_read_chunk,
            'NODE_OP_WRITE_CHUNK'       : _node_op_write_chunk,
            'NODE_OP_DELETE_CHUNK'      : _node_op_delete_chunk,

            'NODE_OP_UPLOAD_FILE'       : _node_op_upload_file,

            'NODE_RQ_CHUNK_METADATA'    : _node_rq_chunk_metadata,

            'NODE_RQ_SORT_SAMPLE'       : _node_rq_sort_sample,
            'NODE_OP_SORT_CHUNK'        : _node_op_sort_chunk,

            'NODE_OP_SHUTDOWN'          : _node_op_shutdown
        }
        self.nodes = {}

        for node, endpoint in nodes:
            self.nodes[node] = endpoint


    def register_node(self, node, endpoint):
        self.nodes[node] = endpoint


    def request(self, node, message_type, *args):
        message = self.messages[message_type](*args)
        return network.request(self.nodes[node], message)

    def broadcast(self, message_type, *args):
        for node in self.nodes:
            yield node, self.request(node, message_type, *args)

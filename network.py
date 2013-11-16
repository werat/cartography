import sys
import socket as sc

try:
    # use super-fast json
    import ujson as json
except ImportError:
    print >> sys.stderr, "WARNING: Cannot use ujson, fallback to json"
    import json


def endpoint_from_tuple(endpoint):
    return '{0}:{1}'.format(*endpoint)

def endpoint_to_tuple(endpoint):
    host, port = endpoint.split(':')
    return (host, int(port))


class Connection(object):
    def __init__(self, socket):
        self.socket = socket

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def get_peername(self):
        peername = self.socket.getpeername()
        return endpoint_from_tuple(peername)

    def get_peeraddress(self):
        addr, _ = self.socket.getpeername()
        return addr

    def receive(self):
        # receive bytes from socket until newline
        received = ''
        while not received.endswith('\n'):
            chunk = self.socket.recv(4096)
            if not chunk: return ''

            received += chunk

        # strip our newline and decode string
        return received[:-1].decode('hex')

    def receive_json(self):
        return json.loads(self.receive())

    def send(self, data):
        # encode data
        bytes = data.encode('hex') + '\n'
        while bytes:
            sent = self.socket.send(bytes)
            bytes = bytes[sent:]

    def send_json(self, data):
        self.send(json.dumps(data))

    def request(self, data):
        self.send(data)
        return self.receive()

    def request_json(self, data):
        self.send(data)
        return self.receive_json()

    def close(self):
        self.socket.close()


class ConnectionListener(object):
    def __init__(self, port=0, local_mode=True):
        self.local_mode = local_mode
        self.active = False

        self.socket = sc.socket()
        self.socket.setsockopt(sc.SOL_SOCKET, sc.SO_REUSEADDR, 1)
        self.socket.bind(('localhost' if local_mode else '', port))


    def accept_connections(self, clients=1):
        self.socket.listen(clients)

        self.active = True
        while self.active:
            sock, addr = self.socket.accept()
            yield Connection(sock)

    def getsockname(self):
        return self.socket.getsockname()


def create_connection(endpoint):
    endpoint = endpoint_to_tuple(endpoint)
    return Connection(sc.create_connection(endpoint))

def request(endpoint, data):
    with create_connection(endpoint) as c:
        # if we got json-like data
        if isinstance(data, dict):
            data = json.dumps(data)

        return c.request(data)

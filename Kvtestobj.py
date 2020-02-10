#!/usr/bin/env python
from __future__ import print_function
import socket
import sys

sys.path.append("../")
from pysyncobj import SyncObj, replicated, SyncObjConf, FAIL_REASON
import selectors
import types
import time
import _thread
import logging

# CONSTANTS
ADDR_FAMILY = socket.AF_INET
SOCK_TYPE = socket.SOCK_STREAM
HOST = '0.0.0.0'
SERVERS_IP = ['localhost:65433', 'localhost:65434']
PORT = 6541
START_CLIENT_PORT = 5432
CLIENT_PORT = 65430
BUF_SIZE = 32000
_c_iteration = 25
_c_maxvalue = 2 ** 31
_c_delay = True
self_addr = 'localhost:65432'
clients_inp = []
connections = []
connections_id = {}
cur_id = 0
sel = selectors.DefaultSelector()
entries_queue = []
_g_dealy = []
processed_entries = 0
logger = logging.getLogger()

'''The synced key-value class'''


class KVStorage(SyncObj):
    def __init__(self, self_address, partnerAddrs):
        super(KVStorage, self).__init__(self_address, partnerAddrs)
        self.__data = {}

    @replicated
    def set(self, key, value, addr, server_st_time):
        self.__data[key] = value
        client_st_time = value[value.find('#') + 1:]
        return addr, client_st_time, server_st_time

    def get(self, key):
        return self.__data.get(key, None)


'''
This function is called synchronously after the entry is synced. 
It return the id of the client who send this entry and the hash value(whether 
it is the start time or a specific id) that the client attached to the value.
It sends back to the client the result of the packet (success or fail) and the 
hash value
'''


def clbck(res, err):
    global connections, _g_dealy
    if not res:
        return
    if err == 0:
        st = float(res[2])
        _g_dealy.append(time.time() - st)
    id = res[0]
    ret = str(err) + '#' + str(res[1]) + '$'
    try:
        if connections[id]:
            connections[id].sendall(str(ret).encode())
    except socket.error:
        return


'''
Open a new socket and register it to the selector.
'''


def establishConnection():
    global sel
    sock = socket.socket(ADDR_FAMILY, SOCK_TYPE)
    sock.bind((HOST, CLIENT_PORT))
    sock.listen()
    logger.info("Server %s is listening...", self_addr)
    sock.setblocking(False)
    sel.register(sock, selectors.EVENT_READ, data=None)


'''
This function is used to accept a new connection.
If the server is the Leader then it notify the client and register the client 
in the selector. Otherwise, it will notify the client with the IP of the Leader.
'''


def acceptWrapper(sock):
    global sel, connections, cur_id, clients_inp
    conn, addr = sock.accept()
    connections_id[addr] = cur_id
    logger.info("Connected to client: %s", str(addr))
    while not kv_obj._getLeader():
        pass
    leader = kv_obj._getLeader()
    conn.sendall(leader.encode())
    if leader != self_addr:
        return
    conn.setblocking(False)
    connections.append(conn)
    clients_inp.append('')
    data = types.SimpleNamespace(id=cur_id, inb=b'', outb=b'')
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
    sel.register(conn, events, data=data)
    cur_id += 1


'''
This function is used to sync a new entry. it check whether the server is the
leader. If not it sends a '-1' error to the client and the IP of the new leader
to the client.
'''


def addEntry(id, key, value):
    global entries_queue
    if (kv_obj._getLeader() != self_addr):
        entries_queue.clear()
        kv_obj.printStatus()
        while not kv_obj._getLeader():
            pass
        leader = str(kv_obj._getLeader())
        ret = "-1#" + leader + '$'
        try:
            connections[id].send(ret.encode())
        except socket.error:
            return
        return
    kv_obj.set(key, value, id, time.time(), callback=clbck)


'''
This function is called on a separated thread to process the entries once an
entry is received from a client and added to "entries_queue"
'''


def processEntries():
    processed_entries_cnt = 0
    while True:
        if processed_entries_cnt < len(entries_queue):
            addEntry(entries_queue[processed_entries_cnt][0], entries_queue[processed_entries_cnt][1],
                     entries_queue[processed_entries_cnt][2])
            processed_entries_cnt += 1


'''
This function is used to handle an received entry from a client who is registered
in the selector
'''


def serviceConnection(key, mask):
    global clients_inp
    sock = key.fileobj
    data = key.data
    id = data.id
    if mask & selectors.EVENT_READ:
        try:
            recv_data = sock.recv(BUF_SIZE).decode()
        except socket.error:
            return
        if recv_data:
            '''
            When receiving a new data separate it using the '$' indicator to
            multiple packets. For each packet separate the key from the value using the
            ':' indicator 
            '''
            clients_inp[id] += recv_data
            ind, last = clients_inp[id].find('$'), 0
            while ind != -1:
                sep = clients_inp[id].find(':', last)
                key = clients_inp[id][last:sep]
                value = clients_inp[id][sep + 1:ind]
                entries_queue.append([id, key, value])
                last = ind + 1
                ind = clients_inp[id].find('$', last)
            clients_inp[id] = clients_inp[id][last:]
        else:
            logger.info("Closing the connection with " + str(id))
            sel.unregister(sock)
            sock.close()


'''
This is a simple event loop function to handle the selector events
'''


def eventLoop():
    global sel
    while True:
        events = sel.select(timeout=None)
        for key, mask in events:
            if key.data is None:
                acceptWrapper(key.fileobj)
            else:
                serviceConnection(key, mask)


def trackLeader():
    global kv_obj
    leader = kv_obj._getLeader()
    while True:
        if leader != kv_obj._getLeader():
            while not kv_obj._getLeader():
                pass
            logger.debug("Change in leader....")
            logger.debug("Server %s became a follower", str(leader))
            logger.debug("Server %s became a leader", str(kv_obj._getLeader()))
            leader = kv_obj._getLeader()
        time.sleep(0.1)


def setLogger():
    global logger
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler("server_side.log")
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)


'''
To make the program compatible with the other benchmarks, make the CLIENT_PORT
the self_addr port plus one.
'''


def main():
    global self_addr, SERVERS_IP, connection, kv_obj, CLIENT_PORT
    setLogger()
    logger.info("New server %s launched..." % (self_addr))
    CLIENT_PORT = int(sys.argv[1])
    self_addr = sys.argv[2]
    SERVERS_IP = sys.argv[3:]
    kv_obj = KVStorage(self_addr, SERVERS_IP)
    while not kv_obj._getLeader():
        time.sleep(0.1)
    logger.info("Initial Leader is %s", str(kv_obj._getLeader()))
    establishConnection()
    _thread.start_new_thread(processEntries, ())
    # _thread.start_new_thread(trackLeader, ())
    eventLoop()


if __name__ == '__main__':
    main()

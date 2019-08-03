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


# CONSTANTS
ADDR_FAMILY = socket.AF_INET
SOCK_TYPE = socket.SOCK_STREAM
HOST = '0.0.0.0'
SERVERS_IP = ['172.16.114.63', '172.16.112.12', '172.16.114.35']
PORT = 65432
CLIENT_PORT = 65430
BUF_SIZE = 1024
_c_iteration = 25
_c_maxvalue = 2 ** 31
_c_delay = True
SelfAddr = '172.16.112.12'
clients_inp = []
connections = []
connections_id = {}
cnt_id = 0
sel = selectors.DefaultSelector()
entries_queue = []
_g_dealy = []

class KVStorage(SyncObj):
    def __init__(self, selfAddress, partnerAddrs):
        cfg = SyncObjConf(dynamicMembershipChange = True, commandsWaitLeader= True)
        super(KVStorage, self).__init__(selfAddress, partnerAddrs, cfg)
        self.__data = {}

    @replicated
    def set(self, key, value, addr, tm):
        self.__data[key] = value
        st = value[value.index('#')+1:-1]
        return addr, st, tm


    def get(self, key):
        return self.__data.get(key, None)



def clbck(res, err):
    global connections, _g_dealy
    if err == 0:
        st = float(res[2])
        print(time.time()-st)
        _g_dealy.append(time.time()-st)
    id = res[0]
    ret = str(err) + '#' + str(res[1]) + '#'
    try:
        if connections[id]:
            connections[id].sendall(str(ret).encode())
    except socket.error:
        return

def establish_connection():
    global sel
    sock = socket.socket(ADDR_FAMILY, SOCK_TYPE)
    sock.bind((HOST, CLIENT_PORT))
    sock.listen()
    print("Server is listening...")
    sock.setblocking(False)
    sel.register(sock, selectors.EVENT_READ, data = None)


def accept_wrapper(sock):
    global sel, connections, cnt_id, clients_inp, _g_dealy
    if len(_g_dealy):
        print(sum(_g_dealy)/len(_g_dealy))
    conn, addr = sock.accept()
    connections_id[addr] = cnt_id
    id = cnt_id
    print("Connected to client: " + str(addr))
    while not kvObj._getLeader():
        pass
    leader = kvObj._getLeader()
    leader = leader[:leader.index(':')]
    print(leader)
    conn.sendall(leader.encode())
    if(kvObj._getLeader() != SelfAddr):
        return
    conn.setblocking(False)
    connections.append(conn)
    clients_inp.append('')
    cnt_id += 1
    data = types.SimpleNamespace(id = id, inb = b'', outb = b'')
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
    sel.register(conn, events, data = data)

def add_entry(id, key ,value):
    if(kvObj._getLeader() != SelfAddr):
        leader = str(kvObj._getLeader())
        ret = "-1#" + leader[:leader.find(':')]+'#'
        try:
            connections[id].send(ret.encode())
        except socket.error:
            return
        return
    kvObj.set(key, value, id, time.time(), callback=clbck)

def process_entries():
    print("start thread")
    while True:
        if len(entries_queue):
            add_entry(entries_queue[0][0], entries_queue[0][1], entries_queue[0][2])
            entries_queue.pop(0)

def service_connection(key, mask):
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
            #print(recv_data)
            clients_inp[id] += recv_data
            ind1 = ind2 = -1
            if len(clients_inp[id]):
                ind1, ind2 = clients_inp[id].find(':', 1), -1
            while len(clients_inp[id]) and ind1 != -1:
                key = clients_inp[id][ind2 + 1:ind1]
                tem = clients_inp[id].find(':', ind1+1)
                if tem == -1:
                    break
                ind2 = tem
                value = clients_inp[id][ind1+1:ind2]
                entries_queue.append([id, key, value])
                ind1 = clients_inp[id].find(':', ind2 + 1)
            if ind2:
                clients_inp[id] = clients_inp[id][ind2+1:]
            #print(repr(data.outb) + " Was received from " + data.addr)
        else:
            print("Closing the connection with " + str(id))
            sel.unregister(sock)
            sock.close()

    # if mask & selectors.EVENT_WRITE and data.outb:
    #     sent = 0
    #     try:
    #         sent = sock.send(data.outb)
    #     except socket.error:
    #         print("Closing the connection with " + str(data.addr))
    #         sel.unregister(sock)
    #         sock.close()
    #     #print("the server sent: " + repr(data.outb[:sent]) + "to client " + data.addr)
    #     data.outb = data.outb[sent:]


def event_loop():
    global sel
    while True:
        events = sel.select(timeout = None)
        for key, mask in events:
            if key.data is None:
                accept_wrapper(key.fileobj)
            else:
                service_connection(key, mask)


def main():
    global SelfAddr, SERVERS_IP, connection, kvObj
    SERVERS_IP.remove(SelfAddr)
    SelfAddr += ":%d" %(PORT)
    for i in range(0, len(SERVERS_IP)):
        SERVERS_IP[i] += ":%d" %(PORT)
    kvObj = KVStorage(SelfAddr, SERVERS_IP)
    while not kvObj._getLeader():
        time.sleep(0.1)
    print(kvObj._getLeader())
    establish_connection()
    _thread.start_new_thread(process_entries, ())
    event_loop()


if __name__ == '__main__':
    main()

#!/usr/bin/env python
from __future__ import print_function
import socket
import sys
sys.path.append("../")
from pysyncobj import SyncObj, replicated, SyncObjConf, FAIL_REASON
import time
import _thread



# CONSTANTS
ADDR_FAMILY = socket.AF_INET
SOCK_TYPE = socket.SOCK_STREAM
HOST = '0.0.0.0'
SERVERS_IP = ['172.16.114.94', '172.16.112.12', '172.16.114.35']
PORT = 65432
CLIENT_PORT = 65430
BUF_SIZE = 1024
_c_iteration = 25
_c_maxvalue = 2 ** 31
_c_delay = True
SelfAddr = '172.16.112.12'


class KVStorage(SyncObj):
    def __init__(self, selfAddress, partnerAddrs):
        cfg = SyncObjConf(dynamicMembershipChange = True, commandsWaitLeader= True)
        super(KVStorage, self).__init__(selfAddress, partnerAddrs, cfg)
        self.__data = {}

    @replicated
    def set(self, key, value, st_time):
        self.__data[key] = value
        return st_time, time.time()


    def get(self, key):
        return self.__data.get(key, None)



def clbck(res, err):
    if err == FAIL_REASON.SUCCESS:
        a, b = res
        with open("delay_servers_analysis.txt", "a+") as f:
            f.write(SelfAddr + " " + str(a) + " " + str(b) + '\n');
    else:
        with open("error_servers_analysis.txt", "a+") as f:
            f.write(SelfAddr + " " + str(err) + '\n')
    conn.sendall(str(err).encode())


def on_new_connection(conn, addr):
    with conn:
        print("Client ", addr, " has connected to server: " + str(socket.gethostname()))
        while True:
            data = conn.recv(BUF_SIZE)
            if not data:
                continue
            # print(data.decode())
            key, value = data.decode().split(':')
            # print(key, value)
            kvObj.set(key, value, time.time(), callback=clbck)


def main():
    global SelfAddr, SERVERS_IP, conn, kvObj
    SERVERS_IP.remove(SelfAddr)
    SelfAddr += ":%d" %(PORT)
    for i in range(0, len(SERVERS_IP)):
        SERVERS_IP[i] += ":%d" %(PORT)
    kvObj = KVStorage(SelfAddr, SERVERS_IP)
    with socket.socket(ADDR_FAMILY, SOCK_TYPE) as mySocket:
        mySocket.bind((HOST, CLIENT_PORT))
        mySocket.listen()
        print("Server is listening now")
        while True:
            conn, addr = mySocket.accept()
            _thread.start_new_thread(on_new_connection, (conn, addr))



if __name__ == '__main__':
    main()

import sys
import random
import socket
import time
import _thread
from threading import Lock
from subprocess import Popen, PIPE
import logging
ADDR_FAMILY = socket.AF_INET
SOCK_TYPE = socket.SOCK_STREAM
SERVERS_IP = ['localhost:4322', 'localhost:4324', 'localhost:4326']
CLIENT_PORT = 5432
BUF_SIZE = 1024
VALID_ERROR_PERCENTAGE = 0.1
MIN_RPS = 100
MAX_RPS = 1000
generated_packets = []
logger = logging.getLogger(__name__)
committed_cnt = 0
err_cnt = 0
packets_latency_analysis = []
error_analysis = []
change_in_leader = False
packets_latency = []
sum_packets_latency = 1.0
lock = Lock()
connecting = False

def generateString(Size):
    f = ""
    for _ in range(0, Size):
        f += chr(random.randrange(0, 26) + ord('a'))
    return f


'''
Start a new connection and 
generate a new set of random packets.
'''
def reset(generated_packetsutSize):
    global committed_cnt, generated_packets
    generated_packets = []
    for _ in range(0, generated_packetsutSize+10):
        generated_packets.append(generateString(keySize) + ":" + generateString(valueSize))
    committed_cnt = 0


'''
Apply binary search to check the maximum RPS such the the error rate is 
less than the valid error percentage.
'''
def findMaxRps(lo, hi):
    while lo < hi:
        print(lo, hi)
        if lo == hi - 1:
            reset(25 * hi)
            if checkErr(hi) <= VALID_ERROR_PERCENTAGE:
                lo = hi
            break
        mid = int((lo + hi) / 2)
        reset(25 * mid)
        if checkErr(mid) <= VALID_ERROR_PERCENTAGE:
            lo = mid
        else:
            hi = mid - 1
    return lo


'''
for a specific RPS check the average error percentage
'''
def checkErr(rps):
    global err_cnt, committed_cnt
    err_cnt = 0
    i = 0
    startTime = time.time()
    iterations = 0
    while time.time()- startTime < 25.0:
        st = time.time()
        i = 0
        while i < rps:
            packet = ''
            while i < rps and len(packet)+len(generated_packets[(iterations * rps + i)]) < BUF_SIZE:
                packet += str(generated_packets[(iterations * rps + i)] + '#' + str(time.time()) + '$')
                i += 1
            try:
                mySocket.sendall(packet.encode())
            except socket.error:
                logger.error("Socket error in sending the packet")
                pass
        dif = time.time()-st
        if dif < 1.0:
            time.sleep(1.0 - dif)
        iterations += 1
    time.sleep(16.0)
    sent = 25 * rps
    err_rate = 1.0 - 1.0 * committed_cnt / (1.0 * sent)
    print("error rate: ", err_rate)
    return err_rate


'''
close the current socket and create a new connection. Wait for the server to send the Leader IP.
In case the leader IP was different that the connected server, connect again 
to the leader.
Note that the client port should be the same as the sync port plus one.
'''
def connect(IP = None):
    global connecting,mySocket, change_in_leader
    logger.info("start a new connection attempt")
    if connecting:
        return
    connecting = True
    lock.acquire()
    try:
        mySocket.close()
    except:
        pass
    mySocket = socket.socket(ADDR_FAMILY, SOCK_TYPE)
    i = 0
    while i < len(SERVERS_IP):
        is_changed = True
        targetAddr, port = SERVERS_IP[i].split(':')
        port = int(port)
        if IP:
            targetAddr, port = IP.split(':')
            port = int(port)
            port += 1
            IP = None
            i -= 1
        try:
            logger.info("Connecting to %s %d", targetAddr, port)
            mySocket.connect((targetAddr, port))
            leader = str(mySocket.recv(BUF_SIZE).decode())
            print(leader)
            while leader != str(targetAddr)+":"+str(port-1):
                targetAddr, port = leader.split(':')
                port = int(port)
                port += 1
                mySocket.close()
                logger.info("Closing the current connection")
                try:
                    mySocket = socket.socket(ADDR_FAMILY, SOCK_TYPE)
                    logger.info("Connecting to %s %d", targetAddr, port)
                    mySocket.connect((targetAddr, port))
                except socket.error:
                    is_changed = False
                    break
                leader = str(mySocket.recv(BUF_SIZE).decode())
            if is_changed:
                logger.info("Connected to the leader")
                change_in_leader = True
                lock.release()
                connecting = False
                return
        except socket.error:
            logger.error("Socket error when connecting")
            i += 1
            continue
        i += 1
    lock.release()
    connecting = False
    connect()


'''
Receive responses from the server and append the send time (that was included in
the packet) and the receive time.
In case the error_state was:
    0 then the packet was synced successfully.
    larger than 0 then the packet failed.
    -1 then the leader changed. Connect to the leader
'''
def receiveResponse():
    global mySocket, committed_cnt, packets_latency_analysis, error_analysis, err_cnt, sum_packets_latency
    mySocket.settimeout(200.0)
    received_data = ''
    while True:
        try:
            received_data += mySocket.recv(BUF_SIZE).decode()
            ind, last = received_data.find('$'), 0
            while ind != -1:
                sep = received_data.find('#', last)
                err = received_data[last:sep]
                res = received_data[sep + 1:ind]
                err = int(err)
                if err == -1:
                    connect(res)
                    received_data = ''
                elif err == 0:
                    packets_latency_analysis.append((res, time.time()))
                    packets_latency.append(time.time() - float(res))
                    sum_packets_latency += time.time() - float(res)
                    committed_cnt += 1
                else:
                    error_analysis.append((err, res, time.time()))
                    err_cnt += 1
                last = ind + 1
                ind = received_data.find('$', last)
            received_data = received_data[last:]
        except socket.timeout:
            logger.error("Socket timeout in receiving from the server")
            continue
        except socket.error as e:
            logger.error("Socket error in receiving the response from the server")
            pass


def setLogger():
    global logger
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler("client_side.log")
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)


def main():
    global generated_packets, conn, keySize, valueSize
    if(len(sys.argv) != 3):
        print("the size of the key - the size of the value")
        sys.exit(0)
    keySize = int(sys.argv[1])
    valueSize = int(sys.argv[2])
    # print("All the data in packets_latency_client_analysis and error_client_analysis will be delated y|n ?")
    # res = input()
    # if(res != 'y'):
    #     sys.exit(0)
    connect()
    _thread.start_new_thread(receiveResponse, ())
    mx_rps = findMaxRps(MIN_RPS, MAX_RPS)
    print(mx_rps)
    # with open("dealy_client_analysis.txt", 'w+') as f:
    #     for i in range(0, len(packets_latency_analysis)):
    #         f.write(str(packets_latency_analysis[i][0]) + " " + str(packets_latency_analysis[i][1]) + '\n')
    # with open("error_client_analysis.txt", 'w+') as f:
    #     for i in range(0, len(error_analysis)):
    #         f.write(str(error_analysis[i][0]) + " " + str(error_analysis[i][1]) +
    #                 " " + str(error_analysis[i][2]) + '\n')


if __name__ == '__main__':
    main()
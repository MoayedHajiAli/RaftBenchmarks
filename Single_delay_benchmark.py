import sys
import random
import socket
import time
import _thread
import selectors
import types
import logging

generated_packets = []
ADDR_FAMILY = socket.AF_INET
SOCK_TYPE = socket.SOCK_STREAM
SERVERS_IP = ['localhost:4322', 'localhost:4324', 'localhost:4326']
BUF_SIZE = 8000
committed_packets_cnt = 0
err_packets_cnt = 0
latency_analysis = []
error_analysis = []
packets_latency = []
sum_packets_latency = 0.0
sent_packets_cnt = 0
change_in_leader = False
defaultSelector = selectors.DefaultSelector()
connection_id = {}
received_data = {}
logger = logging.getLogger(__name__)
'''
Generate random strings of specific size (in bits).
'''


def generateString(Size):
    f = ""
    for _ in range(0, Size):
        f += chr(random.randrange(0, 26) + ord('a'))
    return f


'''
Send packets to the server and hash the send time with it.
Send a specific RPS for 25 iterations.
In case of a failure in the connection, try to connect again and send the
pending packets again without exceeding one second.
'''


def sendPackets():
    global change_in_leader, committed_packets_cnt, my_socket, sent_packets_cnt
    start_time = time.time()
    iterations = 0
    while time.time() - start_time < 25.0:
        st = time.time()
        change_in_leader = False
        i = 0
        while i < rps:
            packet = ''
            tem_cnt = 0
            while i < rps and len(packet) + len(generated_packets[(iterations * rps + i)]) < BUF_SIZE:
                packet += str(generated_packets[(iterations * rps + i)] + '#' + str(time.time()) + '$')
                i += 1
                tem_cnt += 1
            try:
                my_socket.sendall(packet.encode())
                sent_packets_cnt += tem_cnt
            except socket.error:
                pass
        time_elapsed = time.time() - st
        if time_elapsed < 1.0:
            time.sleep(1.0 - time_elapsed)
        iterations += 1


'''
close the current socket and create a new connection. Wait for the server to send the Leader IP.
In case the leader IP was time_elapsedferent that the connected server, connect again 
to the leader.
Note that the client port should be the same as the sync port plus one.
'''


def connect(IP=None):
    logger.debug("Start an attempt for a new connection")
    global my_socket, change_in_leader
    try:
        my_socket.close()
    except:
        pass
    my_socket = socket.socket(ADDR_FAMILY, SOCK_TYPE)
    i = 0
    while i < len(SERVERS_IP):
        is_changed = True
        targer_addr, port = SERVERS_IP[i].split(':')
        port = int(port)
        if IP:
            targer_addr, port = IP.split(':')
            port = int(port)
            port += 1
            IP = None
            i -= 1
        try:
            logger.info("Connecting to %s:%d....", targer_addr, port)
            my_socket.connect((targer_addr, port))
            leader = str(my_socket.recv(BUF_SIZE).decode())
            while leader != str(targer_addr) + ":" + str(port - 1):
                targer_addr, port = leader.split(':')
                port = int(port)
                port += 1
                my_socket.close()
                logger.info("Server responded that it is not the leader")
                logger.info("Closing the connection with the last server")
                try:
                    my_socket = socket.socket(ADDR_FAMILY, SOCK_TYPE)
                    logger.info("connecting to %s:%d....", targer_addr, port)
                    my_socket.connect((targer_addr, port))
                except socket.error:
                    is_changed = False
                    break
                leader = str(my_socket.recv(BUF_SIZE).decode())
            if is_changed:
                logger.info("Connected to the leader %s", leader)
                change_in_leader = True
                return
        except socket.error:
            logger.error("Socket error when connecting to the server")
            continue
        i += 1
    connect()


'''
Receive responses from the server and append the send time (that was included in
the packet) and the receive time.
In case the error_state was:
    0 then the packet was synced successfully.
    larger than 0 then the packet failed.
    -1 then the leader changed. connect to the leader
'''


def receiveResponse():
    global my_socket, committed_packets_cnt, latency_analysis, error_analysis, err_packets_cnt, sum_packets_latency, my_socket
    my_socket.settimeout(20.0)
    received_data = ''
    while True:
        try:
            received_data += my_socket.recv(BUF_SIZE).decode()
            ind, last = received_data.find('$'), 0
            while ind != -1:
                sep = received_data.find('#', last)
                err = received_data[last:sep]
                res = received_data[sep + 1:ind]
                err = int(err)
                if err == -1:
                    connect(res)
                elif err == 0:
                    latency_analysis.append((res, time.time()))
                    packets_latency.append(time.time() - float(res))
                    sum_packets_latency += time.time() - float(res)
                    committed_packets_cnt += 1
                else:
                    error_analysis.append((err, res, time.time()))
                    err_packets_cnt += 1
                    committed_packets_cnt += 1
                last = ind + 1
                ind = received_data.find('$', last)
            received_data = received_data[last:]
        except socket.timeout:
            logger.error("Socket timeout in receiving from the server")
            connect()
        except socket.error as e:
            connect()


def startConnections():
    global connection_id
    for i in range(0, len(SERVERS_IP)):
        host, port = SERVERS_IP[i].split(':')
        port = int(port)
        my_socket = socket.socket(ADDR_FAMILY, SOCK_TYPE)
        my_socket.setblocking(False)
        my_socket.connect_ex((host, port))
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        data = types.SimpleNamespace(connid=i + 1,
                                     msg_total=0,
                                     recv_total=0,
                                     messages=[],
                                     outb=b'')
        defaultSelector.register(my_socket, events, data=data)


def serviceConnections(key, mask):
    global my_socket, committed_packets_cnt, latency_analysis, error_analysis, err_packets_cnt, sum_packets_latency, received_data
    sock = key.fileobj
    data = key.data
    connid = data.connid
    if mask & selectors.EVENT_WRITE:
        recv_data = sock.recv(BUF_SIZE)
        if recv_data:
            received_data[connid] += my_socket.recv(BUF_SIZE).decode()
            ind, last = received_data[connid].find('$'), 0
            while ind != -1:
                sep = received_data[connid].find('#', last)
                err = received_data[connid][last:sep]
                res = received_data[connid][sep + 1:ind]
                err = int(err)
                if err == -1:
                    connect(res)
                elif err == 0:
                    latency_analysis.append((res, time.time()))
                    packets_latency.append(time.time() - float(res))
                    sum_packets_latency += time.time() - float(res)
                    committed_packets_cnt += 1
                else:
                    error_analysis.append((err, res, time.time()))
                    err_packets_cnt += 1
                    committed_packets_cnt += 1
                last = ind + 1
                ind = received_data[connid].find('$', last)
            received_data[connid] = received_data[connid][last:]
        else:
            logger.info("Closing connection with server")


def setLogger():
    global logger
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler("client_side.log")
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)


def main():
    global generated_packets, rps
    setLogger()
    logger.info("New Dealy Test......")
    if len(sys.argv) != 4:
        print("RPS - the size of the key - the size of the value")
        sys.exit(0)
    rps = int(sys.argv[1])
    key_size = int(sys.argv[2])
    value_size = int(sys.argv[3])
    # print("All the data in packets_latency_client_analysis and error_client_analysis will be delated y|n ?")
    # res = generated_packetsut()
    # if(res != 'y'):
    #     sys.exit(0)
    for _ in range(0, 26 * rps):
        generated_packets.append(generateString(key_size) + ":" + generateString(value_size))
    connect()
    _thread.start_new_thread(sendPackets, ())
    _thread.start_new_thread(receiveResponse, ())
    i = 0
    while i < 60:
        time.sleep(1.0)
        logger.info("%d entries committed in the %dth second", committed_packets_cnt, i)
        if committed_packets_cnt > 0:
            logger.info("Average latency in the %dth second is %s", i, str(sum(packets_latency) / len(packets_latency)))
        else:
            logger.info("No committed entries in the %dth second", i)
        i += 1
    #     if len(packets_latency):
    #         print(sum_packets_latency / len(packets_latency), len(packets_latency))
    # with open("dealy_client_analysis.txt", 'w+') as f:
    #     for i in range(0, len(latency_analysis)):
    #         f.write(str(latency_analysis[i][0]) + " " + str(latency_analysis[i][1]) + " " +
    #             str(float(latency_analysis[i][1]) - float(latency_analysis[i][0])) +  '\n')
    # with open("error_client_analysis.txt", 'w+') as f:
    #     for i in range(0, len(error_analysis)):
    #         f.write(str(error_analysis[i][0]) + " " + str(error_analysis[i][1]) +
    #                 " " + str(error_analysis[i][2]) + '\n')
    #    print(len(packets_latency))
    logger.info("Test finished...")
    logger.info("%d entries committed out of %d sent entries", committed_packets_cnt, sent_packets_cnt)
    logger.info("Success rate was %s", str(committed_packets_cnt / sent_packets_cnt))
    if len(packets_latency) == 0:
        logger.error("THE PROGRAM FINISHED AND THERE WAS ZERO COMMITTED ENTRIES")
    else:
        print(sum(packets_latency) / len(packets_latency))
        logger.info("Average latency was %s", str(sum(packets_latency) / len(packets_latency)))


if __name__ == '__main__':
    main()

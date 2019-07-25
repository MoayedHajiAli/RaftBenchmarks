import sys
import random
from subprocess import Popen, PIPE

def generateString(Size):
    f = ""
    for _ in range(0, Size):
        f += chr(random.randrange(0, 26) + ord('a'))
    return f

def main():
    if(len(sys.argv) < 5):
        print("Please enter the number of iterations - the number of packets - the size of the key - the size of the value")
    print (len(sys.argv))
    iterations = int(sys.argv[1])
    print(iterations)
    cmdSize = int(sys.argv[2])
    keySize = int(sys.argv[3])
    valueSize = int(sys.argv[4])
    print("All the data in delay_client_analysis and error_client_analysis will be delated y|n ?")
    res = input()
    if(res == 'n'):
        sys.exit(0)
    with open("dealy_client_analysis.txt", 'w+') as f:
        pass
    with open("error_client_analysis.txt", 'w+') as f:
        pass
    cmd = [sys.executable, 'client.py']
    for _ in range(0, iterations):
        p = Popen(cmd, stdin=PIPE, stdout=PIPE)
        inp = []
        for __ in range(0, cmdSize):
            inp.append(generateString(keySize) + " " + generateString(valueSize))
        s = '\n'.join(inp)
        out = p.communicate(input=s.encode())[0]
        print(out.decode())


if __name__ == '__main__':
    main()
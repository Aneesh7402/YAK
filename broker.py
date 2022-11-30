import socket
import os
from _thread import *
import time
ServerSideSocket = socket.socket()
host = '127.0.0.1'
port = 2004
ThreadCount = 0
no_reads={}
try:
    ServerSideSocket.bind((host, port))
except socket.error as e:
    print(str(e))
print('Socket is listening..')
ServerSideSocket.listen(5)
def producer(connection,topic):
    not_replicate={}
    while len(no_reads[topic])!=0:
        pass
    connection.send(str.encode("Send File"))
    try:
        file=connection.recv()
    except Exception as e:
        print(e)
        connection.close()
        return
    else:
        file=file.decode()
        #partitioning function
        try:
            all_brokers=connection.recv()
        except Exception as e:
            print(e)
            connection.close()
            return
        else:
            all_brokers=all_brokers.decode()
            all_brokers=list(map(int,all_brokers.strip('[').strip(']').split(',')))
            all_brokers=[x for x in all_brokers if x!=port]
            for i in range(len(all_brokers)):
                followerSocket=socket.socket()
                try:
                    followerSocket.connect((host, all_brokers[i]))
                except socket.error as e:
                    print(str(e))
                else:
                    rec1 = followerSocket.recv(1024)
                    followerSocket.send(topic.encode())
                    try:
                        rec2=followerSocket.recv()
                        followerSocket.send(file.encode())
                    except Exception as e:
                        print("Couldn't replicate with this broker connected to"+str(all_brokers[i])+", moving on")
                        not_replicate[topic]=not_replicate[topic].append(all_brokers[i])
                    else:
                        try:
                            rec3=followerSocket.recv()
                        except Exception as e:
                            print("Couldn't replicate with this broker connected to"+str(all_brokers[i])+", moving on")
                            not_replicate[topic]=not_replicate[topic].append(all_brokers[i])
                        else:
                            followerSocket.close()
            if i!=(len(all_brokers)-1):
                print("Auto-replication failed for the following ports, please replicate manually: ")
                for i in range(len(not_replicate[topic])):
                    print(not_replicate[topic][i])
            connection.send(str.encode("Task complete!"))
            connection.close()
            


             
        
    
while True:
    try:
        Client, address = ServerSideSocket.accept()
        print('Connected to: ' + address[0] + ':' + str(address[1]))
        Client.send(str.encode("Server working"))
        recv=Client.recv(2048)
        recv=recv.decode
        if recv=='1':
            producer(Client)

        ThreadCount += 1
        print('Thread Number: ' + str(ThreadCount))
    except socket.error as e:
        print(str(e))
ServerSideSocket.close()
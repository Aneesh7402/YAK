import socket
import os
from _thread import *
import time
ServerSideSocket = socket.socket()
host = '127.0.0.1'
port = 2005
ThreadCount = 0
no_reads={}
try:
    ServerSideSocket.bind((host, port))
except socket.error as e:
    print(str(e))
print('Socket is listening..')
ServerSideSocket.listen()
def producer(connection,topic):
    not_replicate={}
    not_replicate[topic]=list()
    connection.send(str.encode("Send File"))
    try:
        file=connection.recv(1024)
    except Exception as e:
        print(e)
        connection.close()
        return
    else:
        file=file.decode()
        print(file)
        #partitioning function
        try:
            connection.send(str.encode("send all brokers"))
            all_brokers=connection.recv(1024)
            
        except Exception as e:
            print(e)
            connection.close()
            return
        else:
            all_brokers=all_brokers.decode()
            print(all_brokers)
            all_brokers=list(map(int,all_brokers.strip('[').strip(']').split(',')))
            all_brokers=[x for x in all_brokers if x!=port]
            for i in range(len(all_brokers)):
                followerSocket=socket.socket()
                try:
                    followerSocket.connect((host, all_brokers[i]))
                except socket.error as e:
                    print("Couldn't replicate with this broker connected to"+str(all_brokers[i])+", moving on")
                    not_replicate[topic].append(str(all_brokers[i]))
                    print(not_replicate[topic])
                else:
                    rec1 = followerSocket.recv(1024)
                    followerSocket.send(topic.encode())
                    try:
                        rec2=followerSocket.recv(1024)
                        followerSocket.send(file.encode())
                    except Exception as e:
                        print("Couldn't replicate with this broker connected to"+str(all_brokers[i])+", moving on")
                        not_replicate[topic].append(str(all_brokers[i]))
                    else:
                        try:
                            rec3=followerSocket.recv(1024)
                        except Exception as e:
                            print("Couldn't replicate with this broker connected to"+str(all_brokers[i])+", moving on")
                            not_replicate[topic].append(str(all_brokers[i]))
                        else:
                            followerSocket.close()
            if i==(len(all_brokers)-1):
                print("Auto-replication failed for the following ports, please replicate manually: ")
                for i in range(len(not_replicate[topic])):
                    print(not_replicate[topic][i])
            connection.send(str.encode("Task complete!"))
            connection.close()
def follower(connection):
    connection.send(str.encode("Connection successful, send Topic!"))
    try:
        topic=connection.recv(1024).decode
    except Exception as e:
        print("Connection unsuccessful please replicate manually")
    else:
        connection.send(str.encode("Topic received, send file!"))
        try:
            file=connection.recv(1024).decode
        except Exception as e:
            print("Connection unsuccessful please replicate manually")
        else:
            #replicate()
            connection.send(str.encode("Replication successful"))
            connection.close()
                
def consumer(connection):
    try:
        l=connection.recv(1024)
    except Exception as e:
        connection.close()
        print(e)
    else:
        l=l.decode().split(',')
        print(l)
        topic=l[1]
        flag=l[0]
        if flag=="1":
            file="hi123g"#get_from_beg(topic)
        else:
            file="hi"#get_latest(topic)
        connection.send(file.encode())
        connection.close()

             
        
    
while True:
    try:
        Client, address = ServerSideSocket.accept()
        print('Connected to: ' + address[0] + ':' + str(address[1]))
        Client.send(str.encode("Server working"))
        recv=Client.recv(1024)
        recv=recv.decode()
        if recv=='1':
            topic=Client.recv(1024)
            topic=topic.decode()
            producer(Client,topic)
        elif recv=='2':
            consumer(Client)
        elif recv=="3":
            follower(Client)
        ThreadCount += 1
        print('Thread Number: ' + str(ThreadCount))
    except socket.error as e:
        print(str(e))
ServerSideSocket.close()
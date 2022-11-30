import socket
import os
from _thread import *
import time
import random
ServerSideSocket = socket.socket()
host = '127.0.0.1'
port = 2004
ThreadCount = 0
topic2broker={'1':['2005'],'2':['2006']}
topic2consumer={}
allbrokers=[2005,2006]

try:
    ServerSideSocket.bind((host, port))
except socket.error as e:
    print(str(e))
print('Socket is listening..')
ServerSideSocket.listen()
def consumer_read(topic):
    global topic2consumer
    global allbrokers
    try:
        l=topic2consumer[topic]
    except KeyError:
        print("no consumers for this topic yet")
        return
    while True:
        for i in range(len(allbrokers)):
            ClientSocket=socket.socket()
        
            try:
                ClientSocket.connect((host, int(allbrokers[i])))
            except socket.error as e:
                print(str(e))
            else:
                ClientSocket.send(str.encode("0,"+topic))
                try:
                    resv2 = ClientSocket.recv(1024)
                except Exception as e:
                    print(e)
                    ClientSocket.close()
                else:    
                    break
        if i!=len(allbrokers):
            break
    for i in range(len(topic2consumer[topic])):
        ClientSocket52=socket.socket()
        try:
            ClientSocket52.sendall(resv2)
        except Exception:
            topic2consumer[topic][i]=-1
        else:
            ClientSocket52.close()
    topic2consumer[topic]=[x for x in topic2consumer[topic] if x!=-1]
        
def multi_threaded_client(connection):
        global topic2consumer
        global topic2broker
        global allbrokers
        connection.send(str.encode('Server is working:'))
        data = connection.recv(1024)
        data=data.decode()
        print(data)
        port=data
        
        if(port=="1"):
             connection.send(str.encode("Send the Topic"))
             for j in range(5):
                topic = connection.recv(1024)
                if data:
                    break
             connection.send(str.encode("Send the File"))
             for j in range(5):
                file = connection.recv(1024)
                if data:
                    break
             topic=topic.decode()
             file=file.decode()
             print(file)
             try:
                l=topic2broker[topic]
             except KeyError:
                topic2broker[topic]=list()
                topic2broker[topic]=topic2broker[topic].append(str(random.choice[allbrokers]))
             for i in range(len(topic2broker[topic])):
                ClientMultiSocket = socket.socket()
                try:
                    ClientMultiSocket.connect((host, int(topic2broker[topic][i])))
                    
                except Exception as e:
                    print(str(e))
                else:
                                res=ClientMultiSocket.recv(1024)
                                print(res.decode())
                                ClientMultiSocket.send(str.encode("1"))
                                ClientMultiSocket.send(topic.encode())
                                try:
                                    res=ClientMultiSocket.recv(1024)
                                    
                                except Exception as e:
                                    print(e)
                                else:
                                    print(res.decode())
                                    ClientMultiSocket.sendall(file.encode())
                                    try:
                                        res=ClientMultiSocket.recv(1024)
                                    except Exception as e:
                                        print(e)
                                    else:
                                        print(res.decode())
                                        ClientMultiSocket.sendall(str.encode(str(allbrokers)))
                                        try:
                                            res=ClientMultiSocket.recv(1024)
                                        except Exception as e:
                                            print(e)
                                        else:
                                            ClientMultiSocket.close()
                                            connection.send(str.encode(res.decode('utf-8')))
                                            connection.close()
                                            consumer_read(topic)
                                            break
             if i==len(topic2broker[topic]):
                topic2broker[topic]=list()
                for i in range(len(allbrokers)):
                    try:
                        ClientMultiSocket.connect((host, allbrokers[i]))
                        res=ClientMultiSocket.recv(1024)
                    except Exception as e:
                        print(str(e))
                    else:
                        topic2broker[topic]=topic2broker[topic].append(str(allbrokers[i]))
                        ClientMultiSocket.send(str.encode("1"))
                        try:
                            res=ClientMultiSocket.recv(1024)
                        except Exception as e:
                            print(e)
                        else:
                            ClientMultiSocket.sendall(file.encode())
                            try:
                                res=ClientMultiSocket.recv(1024)
                            except Exception as e:
                                print(e)
                            else:
                                        ClientMultiSocket.sendall(str.encode(str(allbrokers)))
                                        try:
                                            res=ClientMultiSocket.recv(1024)
                                        except Exception as e:
                                            print(e)
                                        else:
                                            connection.send(str.encode(res.decode('utf-8')))
                                            connection.close()
                                            consumer_read(topic)
                                            break
                        break
             if i==len(allbrokers):
                print("The leaders are unavailable at the moment")
                connection.close()
             

        if(port==2):
             topic=data[1]
             portno=data[2]
             flag=data[3]
             topic2consumer[topic]=topic2consumer[topic].append(portno)
             res6="Connection successful"
             connection.send(str.encode(res6))
             connection.close()
             
             time.sleep(5)
             if flag==1:
                try:
                    ClientMultiSocket1 = socket.socket()
                    ClientMultiSocket1.connect((host,portno))
                except socket.error as e:
                    print(str(e))
                    topic2consumer[topic].remove(portno)
                else:
                    while True:
                        for i in range(len(allbrokers)):
                            ClientMultiSocket2 = socket.socket()
                            try:
                                ClientMultiSocket2.connect((host, allbrokers[i]))
                            except socket.error as e:
                                ClientMultiSocket2.close()
                                print(str(e))
                            else:
                                ClientMultiSocket2.send(str.encode(flag+","+topic))
                                try:
                                    res1 = ClientMultiSocket2.recv(1024)#sendflag
                                except Exception as e:
                                    ClientMultiSocket2.close()
                                    print("flag/topic not received")
                                else:
                                    file=res1.decode()
                                    ClientMultiSocket2.close()
                    
                        if i!=len(allbrokers):
                            break
                    ClientMultiSocket1.sendall(str.encode(file))
                    res1 = ClientMultiSocket1.recv(1024)
                    ClientMultiSocket1.close()

                    
            
             
             connection.close()
             
        
    
while True:
    try:
        Client, address = ServerSideSocket.accept()
        print('Connected to: ' + address[0] + ':' + str(address[1]))
        start_new_thread(multi_threaded_client, (Client, ))
        ThreadCount += 1
        print('Thread Number: ' + str(ThreadCount))
    except socket.error as e:
        print(str(e))
ServerSideSocket.close()

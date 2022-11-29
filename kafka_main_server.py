import socket
import os
from _thread import *
import time
ServerSideSocket = socket.socket()
host = '127.0.0.1'
port = 2004
ThreadCount = 0
topic2broker={1:2005,2:2006,3:2007,4:2008}
topic2consumer={}
allbrokers=[2005,2006,2007,2008,2009,2010,2020]
try:
    ServerSideSocket.bind((host, port))
except socket.error as e:
    print(str(e))
print('Socket is listening..')
ServerSideSocket.listen(5)
def consumer_read(topic):
    global topic2consumer
    global allbrokers
    for i in range(len(allbrokers)):
        ClientSocket=socket.socket()
        while True:
            try:
                ClientSocket.connect((host, allbrokers[i]))
            except socket.error as e:
                print(str(e))
            else:
                resv2 = ClientSocket.recv(1024)
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
    while True:
        data = connection.recv(2048)
        if not data:
            connection.close()
            break
        data=data.decode('utf-8')
        data= data.split(",")
        port=data[0]
        
        if(port==1):
             topic=data[1]
             file=data[2]
             file='1,'+file
             
             for i in range(len(topic2broker[topic])):
                ClientMultiSocket = socket.socket()
                try:
                    ClientMultiSocket.connect((host, topic2broker[topic][i]))
                except socket.error as e:
                    print(str(e))
                else:
                    break
             if i==len(topic2broker[topic]):
                topic2broker[topic]=list()
                for i in range(len(allbrokers)):
                    try:
                        ClientMultiSocket.connect((host, allbrokers[i]))
                    except socket.error as e:
                        print(str(e))
                    else:
                        topic2broker[topic]=topic2broker[topic].append(allbrokers[i])
                        break
             if i==len(allbrokers):
                print("The leaders are unavailable at the moment")
                connection.close()
                break
             ClientMultiSocket.sendall(file.encode())
             
             res = ClientMultiSocket.recv(1024)
             connection.send(str.encode(res.decode('utf-8')))
             connection.close()
             consumer_read(topic)
             

        if(port==2):
             topic=data[1]
             portno=data[2]
             flag=data[3]
             topic2consumer[topic]=portno
             res6="Connection successful"
             connection.send(str.encode(res6))
             connection.close()
             ClientMultiSocket1 = socket.socket()
             time.sleep(5)
             if flag==1:
                try:
                    ClientMultiSocket1.connect((host,portno))
                except socket.error as e:
                    print(str(e))
                    topic2consumer[topic].remove(portno)
                    break
                else:
                    while True:
                        for i in range(len(allbrokers)):
                            ClientMultiSocket2 = socket.socket()
                            try:
                                ClientMultiSocket2.connect((host, allbrokers[i]))
                            except socket.error as e:
                                print(str(e))
                            else:
                                res = ClientMultiSocket2.recv(1024)
                                if res:
                                ClientMultiSocket2.close()  
                                break
                    
                        if i!=len(allbrokers):
                            break
                    ClientMultiSocket1.sendall(str.encode(res.decode('utf-8')))
                    res1 = ClientMultiSocket1.recv(1024)

                    
            
             
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
import socket
import os
from _thread import *

ServerSideSocket1 = socket.socket()
ServerSideSocket2 = socket.socket()
ServerSideSocket3 = socket.socket()
ServerSideSocket4 = socket.socket()
ServerSideSocket5 = socket.socket()
ServerSideSocket=[ServerSideSocket1,ServerSideSocket2,ServerSideSocket3,ServerSideSocket4,ServerSideSocket5]
host = '127.0.0.1'
port = [2004,2005,2006,2007,2008]
ThreadCount = 0
for i in range(len(ServerSideSocket)):

    try:
        ServerSideSocket[i].bind((host, port[i]))
    except socket.error as e:
        print(str(e))
    print('Socket is listening..')
    ServerSideSocket[i].listen(5)
def port1(connection):
    
    connection.send(str.encode('Server is working port1:'))
    
    while True:
        data = connection.recv(2048)
        response = 'Server message 1: ' + data.decode('utf-8')
        if not data:
            break
        connection.sendall(str.encode(response))
    connection.close()
def port2(connection):
    connection.send(str.encode('Server is working port2:'))
    
    while True:
        data = connection.recv(2048)
        response = 'Server message 2: ' + data.decode('utf-8')
        if not data:
            break
        connection.sendall(str.encode(response))
    connection.close()
def port3(connection):
    connection.send(str.encode('Server is working port3:'))
    
    while True:
        data = connection.recv(2048)
        response = 'Server message: ' + data.decode('utf-8')
        if not data:
            break
        connection.sendall(str.encode(response))
    connection.close()
def port4(connection):
    connection.send(str.encode('Server is working:'))
    print("You are connected to port4")
    while True:
        data = connection.recv(2048)
        response = 'Server message: ' + data.decode('utf-8')
        if not data:
            break
        connection.sendall(str.encode(response))
    connection.close()
def port5(connection):
    connection.send(str.encode('Server is working:'))
    print("You are connected to port5")
    while True:
        data = connection.recv(2048)
        response = 'Server message: ' + data.decode('utf-8')
        if not data:
            break
        connection.sendall(str.encode(response))
    connection.close()
def multi_threaded_client(i):
    global ThreadCount
    
    if i==0:
        Client, address = ServerSideSocket[i].accept()
        print('Connected to: ' + address[0] + ':' + str(address[1]))
        start_new_thread(port1, (Client,))
        ThreadCount += 1
        print('Thread Number: ' + str(ThreadCount))
    elif i==1:
        Client, address = ServerSideSocket[i].accept()
        print('Connected to: ' + address[0] + ':' + str(address[1]))
        start_new_thread(port2, (Client,))
        ThreadCount += 1
        print('Thread Number: ' + str(ThreadCount))
    elif i==2:
        Client, address = ServerSideSocket[i].accept()
        print('Connected to: ' + address[0] + ':' + str(address[1]))
        start_new_thread(port3, (Client,),)
        ThreadCount += 1
        print('Thread Number: ' + str(ThreadCount))
    elif i==3:
        Client, address = ServerSideSocket[i].accept()
        print('Connected to: ' + address[0] + ':' + str(address[1]))
        start_new_thread(port4, (Client,))
        ThreadCount += 1
        print('Thread Number: ' + str(ThreadCount))
    elif i==4:
        Client, address = ServerSideSocket[i].accept()
        print('Connected to: ' + address[0] + ':' + str(address[1]))

        start_new_thread(port5, (Client,))
        ThreadCount += 1
        print('Thread Number: ' + str(ThreadCount))


while True:
    for i in range(len(ServerSideSocket)):
        start_new_thread(multi_threaded_client,(i,))
ServerSideSocket.close()
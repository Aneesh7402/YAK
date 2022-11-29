import socket
ClientMultiSocket = socket.socket()
host = '127.0.0.1'
port = 2004
print('Waiting for connection response')
try:
    ClientMultiSocket.connect((host, port))
except socket.error as e:
    print(str(e))
res = ClientMultiSocket.recv(1024)
while True:
    Input1 = input('Enter topic: ')
    File_object=None
    while File_object==None:
        Input2=input("Enter file path: ")
        try:
            File_object = open(Input2)
        except Exception:
            print("File does not exist")
    Final="1,"+Input1+','+File_object.read()
    while True:
        try:
            ClientMultiSocket.sendall(str.encode(Final))
        except socket.error as e:
            print(e)
        else:
            break

    res = ClientMultiSocket.recv(1024)
    print(res.decode('utf-8'))
ClientMultiSocket.close()
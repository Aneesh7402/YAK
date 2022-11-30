import socket
s = socket.socket()
host = '127.0.0.1'
portC = 2004
portS=2008
print('Waiting for connection response')
try:
    s.connect((host, portC))
except socket.error as e:
    print(str(e))
res = s.recv(1024)
print(res.decode())
input1= input("Enter file path:\n")
file_obj = open(input1,"a")
topicName = input("Enter the topic name you want to subscribe to:\n")
flag = input("Enter the flag value:")
try:
    s.send(str.encode("2"))
except socket.error as e:
    print(str(e))
else:
    s.send(str.encode(str(topicName)+','+str(portS)+','+str(flag)))
    try:
        recv=s.recv(1024).decode()
    except Exception as e:
        print(e)
    else:
        print(recv)
        s.close()

        sck = socket.socket()
        sck.bind((host,portS))
        sck.listen()
        
        while True:
            try:
                conn,addr = sck.accept()
                file=conn.recv(1024).decode()
                print(file)
                conn.send(str.encode("Received file"))
                conn.close()
            except Exception as e:
                print(e)
                break
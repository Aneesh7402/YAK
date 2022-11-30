import os
def partitioner(topicName,value,msg):
    is_exist = os.path.exists(topicName)
    if is_exist == False:
        os.makedirs(topicName)
    if value%3==0 : 
        f1 = open("/Users/akashshankar/"+topicName+"/part1.txt","a")
        f1.append(value+", "+msg)
        f1.close()
    elif value%3==1:
        f2 = open("/Users/akashshankar/"+topicName+"/part2.txt","a")
        f2.append(value+", "+msg)
        f2.close()
    else:
        f3 = open("/Users/akashshankar/"+topicName+"/part3.txt","a")
        f3.append(value+", "+msg)
        f3.close()
def ReadFromBeginning(topicName):
    f1 = open("/Users/akashshankar/"+topicName+"/part1.txt","w")
    f2 = open("/Users/akashshankar/"+topicName+"/part2.txt","w")
    f3 = open("/Users/akashshankar/"+topicName+"/part1.txt","w")
    str = ""
    while True:
        c = f1.read()
        if c == "":
            break
        str = str + c
        d = f2.read()
        if d == "":
            break
        str = str + "\n" + d
        e = f3.read()
        if e == "":
            break
        e = str + "\n" + e + "\n"
    f1.close()
    f2.close()
    f3.close()
    return str
def GetNewMessage(topicName,value):
    if value % 3 == 0:
        f1 = open("/Users/akashshankar/"+topicName+"/part1.txt","r")
        for line in f1:
            data = line.split(", ")
            if int(data[0]) == value:
                f1.close()
                return data[1]
    elif value % 3 == 1:
        f2 = open("/Users/akashshankar/"+topicName+"/part2.txt","r")
        for line in f2:
            data = line.split(", ")
            if int(data[0]) == value:
                f2.close()
                return data[1]
                
    elif value % 3 == 2:
        f3 = open("/Users/akashshankar/"+topicName+"/part3.txt","r")
        for line in f3:
            data = line.split(", ")
            if int(data[0]) == value:
                f3.close()
                return data[1]
    else:
        str = ""
        return str
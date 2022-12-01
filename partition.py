import os
file = os.path.dirname(os.path.abspath(__file__))
def partitioner(topicName,value,msg):
    is_exist = os.path.exists(topicName)
    if is_exist == False:
        os.makedirs(file +"\\"+topicName)
    f1 = open(file+"\\"+topicName+"\\part1.txt","a")
    f2 = open(file +"\\"+topicName+"\\part2.txt","a")
    f3 = open(file +"\\"+topicName+"\\part3.txt","a")
    if value%3==0 : 
        f1.write(str(value)+", "+msg)
        f1.close()
    elif value%3==1:
        f2.write(str(value)+", "+msg)
        f2.close()
    else:
        f3.write(str(value)+", "+msg)
        f3.close()
def ReadFromBeginning(topicName):
    is_exist = os.path.exists(topicName)
    if is_exist == False:
        os.makedirs(file +"\\"+topicName)
    f1 = open(file+"\\"+topicName+"\\part1.txt","r")
    f2 = open(file+"\\"+topicName+"\\part2.txt","r")
    f3 = open(file+"\\"+topicName+"\\part3.txt","r")
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
        str = str + "\n" + e + "\n"
    f1.close()
    f2.close()
    f3.close()
    return str
def GetNewMessage(topicName,value,final):
    is_exist = os.path.exists(topicName)
    if is_exist == False:
        os.makedirs(file +"\\"+topicName)
    f1 = open(file+"\\"+topicName+"\\part1.txt","r")
    f2 = open(file+"\\"+topicName+"\\part2.txt","r")
    f3 = open(file+"\\"+topicName+"\\part3.txt","r")
    str = ""
    while True:
        if value % 3 == 0:
            for line in f1:
                data = line.split(", ")
                if int(data[0]) == value:
                    str = str + data[1] + ", "
        elif value % 3 == 1:
            for line in f2:
                data = line.split(", ")
                if int(data[0]) == value:
                    str = str + data[1] + ", "
                
        elif value % 3 == 2:
            for line in f3:
                data = line.split(", ")
                if int(data[0]) == value:
                    str = str + data[1] + ", "
        else:
            str = "No new messages"
        if value == final:
            break
        else:
            value += 1
    f1.close()
    f2.close()
    f3.close()
    return str



      
        
    
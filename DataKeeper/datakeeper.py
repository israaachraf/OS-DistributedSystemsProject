import zmq
import time
import sys
from  multiprocessing import Process
import socket
from util import send_array, recv_array
import numpy as np
import math
import os 
#port = "5000"
#if len(sys.argv) > 1:
#    port =  sys.argv[1]
#    int(port)
#
#
#context = zmq.Context()
#socket = context.socket(zmq.REP)
#socket.bind("tcp://*:%s" % port)
def SendingDone(slavePort,filename,clientID):
    print("sendind done to master ..")
    masterSlavePorts = [9100,9101,9102,9103,9104]
    context = zmq.Context()
    print ("Connecting to server with ports %s" % masterSlavePorts)


    masterSlavesocket = context.socket(zmq.REQ)
    for port in masterSlavePorts:
        masterSlavesocket.connect ("tcp://192.168.1.6:%s" % port)

    dirpath = os.getcwd()
    msg = np.array(["1",filename,slavePort,clientID,str(socket.gethostbyname(socket.gethostname())),dirpath]) #modify # IP, FilePath
    send_array(masterSlavesocket,msg)
    
def DoneDownloading(slavePort):
    print("sendind done downloading to master ..")
    masterSlavePorts = [9100,9101,9102,9103,9104]
    context = zmq.Context()
    print ("Connecting to server with ports %s" % masterSlavePorts)


    masterSlavesocket = context.socket(zmq.REQ)
    for port in masterSlavePorts:
        masterSlavesocket.connect ("tcp://192.168.1.6:%s" % port)   
        
    print("i'm slave port ",slavePort)
    msg = np.array(["2",slavePort,str(socket.gethostbyname(socket.gethostname()))]) #modify # IP
    send_array(masterSlavesocket,msg)
    
def RecievingFile(filename,socket):
    print("recieving file func")
   # fileData = socket.recv_string()
    fileData = socket.recv()
    file = open(filename,"wb")
    file.write(fileData)
    file.close()
    newname = filename[:-4]
    chunk = math.ceil(len(fileData)/6)
    f = open(filename,"rb")
    for i in range (0,6):
        part = f.read(chunk)
        subfile= open(newname+str(i)+".mp4","wb")
        subfile.write(part)
        subfile.close()
    

    
def  UploadReq(socket,port,messageID,messageName):
    #print("requesting client ID")
    #socket.send_string("send ID")
    clientID = int(messageID)
    print(clientID)

    #socket.send_string("send filename")
    fileName=messageName

    socket.send_string("filename recieved , send file itself")
    RecievingFile(fileName,socket)

    print("file uploaded done")
    SendingDone(port,fileName,clientID)
    #take file and save it in a directory--done
    #send succes msg to master --done
    #replicate
    
def DownloadReq(socket,port,fileName):
    #print("requesting file name")
    #socket.send_string("send FN")
    #fileName=socket.recv_string()
    with open(fileName,"rb") as file:
        data = file.read()
        
    file.close()
    DoneDownloading(port)
    socket.send(data)
    
def Getting_requests(port):

    print("Hi from slave port %s",port)
    while(1):
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        #host_name = socket.gethostname() 
        #host_ip = socket.gethostbyname(host_name)
        socket.bind("tcp://*:%s" % port)
        #socket.setblocking(0)
        print ("Running slave node on port: ", port)
        # Wait for next request from client
        message = recv_array(socket)
        
        #print(message)
        #socket.send_string("request recieved in master")
        if message[0] == "1":
            UploadReq(socket,port,message[1],message[2])
            #socket.send_string(result)
        elif message[0] == "2":
            DownloadReq(socket,port,message[1])
            #socket.send_string(result)
        
 
if __name__ == "__main__":
    print("Heloo")
    slaveClientPorts = [6000,6001,6002,6003,6004,6005]
    #----------------------------------

    ps = [Process(target=Getting_requests, args=(port,)) for port in slaveClientPorts]
    for p in ps: 
        p.start()
    for p in ps:
        p.join()
        


import random
import time
import sys
import zmq
import socket
from  multiprocessing import Process
import json
import numpy as np
from util import send_array, recv_array
CHUNK_SIZE = 250000


def SendingFile(uploadSocket,name,directory):
    with open(directory+'/'+name,"rb") as file:
        data = file.read()
#        while chunk:
#            pushSocket.send(chunk)    
#            chunk = file.read(CHUNK_SIZE)
    #print(data)
    #js=str(json.dumps(data))
    #uploadSocket.send_string(str(data))
    uploadSocket.send(data)
    file.close()
    uploadSocket.close()
    
def  Upload(socket,myID):
    #  Get the reply.
    dataNode = recv_array(socket)
    if(len(dataNode) == 0):
        print("Sorry we are busy...Try again Later")
        return

    dataNodeIP = dataNode[0]#[0]: IP #modify
    dataNodePort = int(dataNode[1]) 
    print ("Received reply datanode port: ", dataNodePort)
    
    print ("Connecting to data node...")
    dataNodeContext = zmq.Context()
    dataNodeSocket = dataNodeContext.socket(zmq.REQ)
    #dataNodeSocket.connect ("tcp://localhost:%s" % dataNodePort)
    string = "tcp://"+str(dataNodeIP)+":%s"
        #DataNodeSocket.connect ("tcp://localhost:%s" % DataNodePort)
    dataNodeSocket.connect (string % dataNodePort)
    #dataNodeSocket.send_string("1")#req,file and file name and ID
    print("connection established between client and data node")
    #message=dataNodeSocket.recv_string()
    #print(message)
    #if message=="send ID":
    #myID = 1
    #dataNodeSocket.send_string(str(myID))
    #replay=dataNodeSocket.recv_string()
    #print(replay)
    fileName = input("enter the file name :")
    #dataNodeSocket.send_string(fileName)
    msg =np.array(["1",str(myID),fileName])
    send_array(dataNodeSocket,msg)
    replay=dataNodeSocket.recv_string()
    print(replay)       
    fileDir = input("enter the file directory :")
    SendingFile(dataNodeSocket,fileName,fileDir)
    dataNodeSocket.close()

def Download(socket,fileName):
    #  Get the reply.
    dataNode = recv_array(socket) #6 --> IP,Port
    print ("Received reply datanode ", dataNode)
    
    if(len(dataNode) == 0):
        print("Sorry we are busy...Try again Later and Double check the file name")
        return
    myFile = bytearray()
    for i in range(0,12,2):
        print ("In Download .. Connecting to dataNode port",i)
        DataNodeIP = dataNode[i] # for connecting on different machines
        DataNodePort = dataNode[i+1]
        DataNodeContext = zmq.Context()
        DataNodeSocket = DataNodeContext.socket(zmq.REQ)
        string = "tcp://"+str(DataNodeIP)+":%s"
        #DataNodeSocket.connect ("tcp://localhost:%s" % DataNodePort)
        DataNodeSocket.connect (string % DataNodePort)
        #DataNodeSocket.send_string("2") #--Requesting Download
        #message=DataNodeSocket.recv_string()
        newname = fileName[:-4]
        print(newname+str(int(i/2))+".mp4")
        #print(message)
        #if message=="send FN":
        msg = np.array(["2",newname+str(int(i/2))+".mp4"])
        send_array(DataNodeSocket,msg)
        chunk = DataNodeSocket.recv()
        myFile +=chunk
        DataNodeSocket.close()
        
    
    fileDir = input("enter the directory you want to download the file in:")
    file = open(fileDir+'/'+fileName,"wb")
    file.write(myFile)
    print("Done Downloading ;)")
    
    
def List(socket):
    files = recv_array(socket)
    print(files)
    


    
def InitSendingRequests(ports):
    context = zmq.Context()
    print ("Connecting to server with ports %s" % ports)
    socket = context.socket(zmq.REQ)
    for port in ports:
        socket.connect ("tcp://192.168.1.6:%s" % port)
    while(1):
        ID = input("Enter your ID: ")
        requestnum = input("choose one option: 1-Upload 2-Download 3-list: ")
        print ("Sending request to master requestnum: ", requestnum)
       # donemessage=socket.recv_string()
        msg = np.array([str(requestnum)])
        
        if requestnum == "1":
            send_array(socket,msg)
            Upload(socket,ID)
            
        elif requestnum == "2":
            fileName = input ("Enter the file name you want to download: ")
            msg = np.append(msg,ID) #ID #modify
            msg = np.append(msg,fileName)
            send_array(socket,msg)
            Download(socket,fileName)
            
        elif requestnum == "3":
            msg = np.append(msg,ID) #ID #modify
            send_array(socket,msg)
            List(socket)

if __name__ == "__main__":
    # master client server are [5550, 5552, 5554, 5556]
    masterClientPorts =[9000,9001,9002,9003,9004]
    InitSendingRequests(masterClientPorts)

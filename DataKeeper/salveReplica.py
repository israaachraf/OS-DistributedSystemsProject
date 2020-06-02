# -*- coding: utf-8 -*-
import zmq
import time
import sys
from  multiprocessing import Process
import socket
from util import send_array, recv_array
import numpy as np
import math
import os





def Download_Replica(port,Filename,Userid):
    while True:
        print("Download Replica Function ",Filename)
        context = zmq.Context()
        Downsocket = context.socket(zmq.REP)
        Downsocket.bind("tcp://*:%s" % port)
        filedata = Downsocket.recv()
        file = open("replica.mp4","wb")
        file.write(filedata)
        file.close()
        newname = Filename[:-4]
        chunk = math.ceil(len(filedata)/6)
        f = open(Filename,"rb")
        for i in range (0,6):
            part = f.read(chunk)
            subfile= open(newname+str(i)+".mp4","wb")
            subfile.write(part)
            subfile.close()
            
    
    
    

def Getting_Replica_Req_Src(port):
    while True:
        context = zmq.Context()
        slaveReplicasocket = context.socket(zmq.REP)
        #print("Src trying to connect to Dest port",port)
        print("")
        slaveReplicasocket.bind("tcp://*:%s" % port)
        print ("Running Slave Replica on port: ", port)
        request = recv_array(slaveReplicasocket)
        if request[0] == "1" : #---Src
            print("Getting Src request",request)
            Filename = request[1]
            Userid = request[2]
            Connectport = request[3]
            Connectip = request[4]
            print("Hi from Slave src to send file to port",Connectport)
            sistersocket = context.socket(zmq.REQ)
            sistersocket.connect ("tcp://"+str(Connectip)+":%s" % Connectport)
            with open(Filename,"rb") as file:
                data = file.read()
            
            sistersocket.send(data)
            sistersocket.close()  
            
            
def Getting_Replica_Req_Dest(port):
    while True:
        context = zmq.Context()
        slaveReplicasocket = context.socket(zmq.REP)
        slaveReplicasocket.bind("tcp://*:%s" % port)
        print ("Running Slave Replica on port: ", port)
        request = recv_array(slaveReplicasocket)        

        if request[0] == "2" :#--- Dest
            print("Getting Dest request",request)
            Filename = request[1]
            Userid = request[2]
            myPort = request[3]
            myip = request[4]
            
            #sisterPorts = [9300,9301,9302,9303]
            #for port in sisterPorts:
            print("Download Replica Function ",Filename)
            context = zmq.Context()
            Downsocket = context.socket(zmq.REP)
            Downsocket.bind("tcp://*:%s" % myPort)
            filedata = Downsocket.recv()
            file = open("replica.mp4","wb")
            file.write(filedata)
            file.close()
            newname = Filename[:-4]
            chunk = math.ceil(len(filedata)/6)
            f = open(Filename,"rb")
            for i in range (0,6):
                part = f.read(chunk)
                subfile= open(newname+str(i)+".mp4","wb")
                subfile.write(part)
                subfile.close()
            masterSlavePorts = [9100,9101,9102,9103,9104]
            mastercontext = zmq.Context()
            print ("Connecting to server with ports %s" % masterSlavePorts)
            masterSlavesocket = mastercontext.socket(zmq.REQ)
            for mport in masterSlavePorts:
                masterSlavesocket.connect ("tcp://192.168.1.6:%s" % mport)
            
            dirpath = os.getcwd()
            msg = np.array(["1",Filename,myPort,Userid,myip,dirpath]) #modify # IP, FilePath
            send_array(masterSlavesocket,msg)


if __name__ == "__main__":
    masterSlaveReplicaPorts1= [9200,9201,9202]
    masterSlaveReplicaPorts2= [9203,9204,9205]
    ms = [Process(target=Getting_Replica_Req_Src, args=(port,)) for port in masterSlaveReplicaPorts1]
    ms2 = [Process(target=Getting_Replica_Req_Dest, args=(port2,)) for port2 in masterSlaveReplicaPorts2]
    for m in ms: 
        m.start()
    for m2 in ms2: 
        m2.start()        
    for m in ms:
        m.join()
    for m2 in ms2:
        m2.join()

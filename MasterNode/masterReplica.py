# -*- coding: utf-8 -*-


import zmq
import time
import sys
from  multiprocessing import Process
import socket
from DB import *
from util import send_array, recv_array
import numpy as np
    
    
    
def NotifySrc(Srcip,Srcport,Desport,Desip,Filename,Userid):
    #---Srcip to connect -----------------------
    print("Hi notify Src with file",Filename)
    masterSlaveReplicaPorts = [9200,9201,9202]
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    for port in masterSlaveReplicaPorts:
        socket.connect ("tcp://"+str(Srcip)+":%s" % port)
    msg = np.array(["1",Filename,Userid,Desport,Desip]) 
    print("Send msg to Src slave",msg)
    send_array(socket,msg)
    socket.close()
def NotifyDest(Filename,Userid,Destip,Desport):   
    #---Destip to connect ---------------------  
    print("Hi notify dest with file",Filename)
    masterSlaveReplicaPorts = [9203,9204,9205]
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    for port in masterSlaveReplicaPorts:
        socket.connect ("tcp://"+str(Destip)+":%s" % port)
        
    msg = np.array(["2",Filename,Userid,Desport,Desip]) 
    print("Send msg to des slave",msg)
    send_array(socket,msg)
    socket.close()
    
    
#---------------------------------
while True :
    Files = get_files_count_less_than_three()
    size = int(len(Files))
    #print(Files)
    for i in range(0,size,3):
        Userid = int(float(Files[i]))
        Filename = Files[i+1]
        Count = Files[i+2]
        #print("File is : ",Filename)
        #print("Count is :",Count)
       # print ("Userid is : ",Userid)
        Src = get_Src_for_replicate(Userid,Filename)
        if len(Src)>0 :
            Srcip = Src[0]
            Srcport = Src[1]
            #print("Src port :",Srcport)
            Dest = get_Dst_for_replicate(Userid,Filename)
            if len(Dest) > 0:
                Desip = Dest[0]
                Desport = Dest[1]
                #print("Des port :",Desport)
                #print("Destination ip1  :",Desip)
                NotifyDest(Filename,Userid,Desip,Desport)
                NotifySrc(Srcip,Srcport,Desport,Desip,Filename,Userid)
                
                #time.sleep(10)
              #  Update lookuptable
                if Count == "1" and len(Dest) > 2:
                    Desip2 = Dest[2] 
                    #print("Destination ip2  :",Desip2)
                    Desport2 = Dest[3]
                    NotifyDest(Filename,Userid,Desip2,Desport2)
                    NotifySrc(Srcip,Srcport,Desport2,Desip2,Filename,Userid)
                    #time.sleep(10)    
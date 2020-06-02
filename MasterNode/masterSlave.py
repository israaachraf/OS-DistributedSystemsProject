# -*- coding: utf-8 -*-
"""
Created on Wed May  1 17:00:14 2019

@author: Esraa
"""
import zmq
import time
import sys
from  multiprocessing import Process,Lock
import socket
from DB import *
from util import send_array, recv_array


def Getting_Done(port,l):
    print("Hi from Getting Done  in master %s",port)
    while (1):
        context = zmq.Context()
        slavesocket = context.socket(zmq.REP)
        slavesocket.bind("tcp://*:%s" % port)
        print ("Running Master-Slave on port: ", port)
        #message = slavesocket.recv_string() #file name
        #print("file name: ", message)
        #filename = message

        #slavesocket.send_string("next")
        #slavePort=int(slavesocket.recv_string()) 
        #slaveIP = "" #IP to be modified
        
        #slavesocket.send_string("next")
        #clientID = int(slavesocket.recv_string())
        
        #filePath = "" #to be modified
        #l.acquire()
        msg = recv_array(slavesocket)
        
        if msg[0] == "1":
            filename = msg[1]
            slavePort = msg[2]
            clientID = msg[3]
            slaveIP = msg[4]
            filePath = msg[5]
            print("insert file in DB")
            print("msg is :",msg)
            
            insert_file(clientID, filename, slaveIP, filePath)
            update_busy_port(slaveIP, slavePort, 0)
            
        elif msg[0] =="2":
            slavePort = msg[1]
            slaveIP = msg[2]
            update_busy_port(slaveIP,slavePort,0)
        #l.release()
if __name__ == "__main__":
    masterSlavePorts = [9100,9101,9102,9103,9104]
    lock = Lock()
    ms = [Process(target=Getting_Done, args=(port,lock)) for port in masterSlavePorts]
    for m in ms: 
        m.start()
    for m in ms:
        m.join()

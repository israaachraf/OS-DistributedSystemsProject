import zmq
import time
import sys
from  multiprocessing import Process
import socket
import numpy as np
from DB import *
from util import send_array, recv_array

############################################nb3t IP m3 l port #########################
def  Upload():
    ###search in lookup table for available port##
    ret = np.array([])
    availableDataNodes = get_available_ports()
    
    if(availableDataNodes == ()):
         return ret
     
    availableDataNodeIP = availableDataNodes[0]["IP"]
    availableDataNodePort = availableDataNodes[0]["Port"]
    ret = np.append(ret,availableDataNodeIP)
    ret = np.append(ret, availableDataNodePort)
    
    print("IP: ", availableDataNodeIP, " Port: ", availableDataNodePort)
    update_busy_port(availableDataNodeIP, availableDataNodePort, '1')
    print("Return from upload: ", ret)
    return ret
    #socket.send(dataNodePort)
    
    #connect with datanode
    #receive 2sm l file, clientID
    #add file to lookup table
    
def Download(userID, fileName):
    #######search in lookup table for 6 available port fehom l file#####
    ret = np.array([])
    availableDataNodes = get_available_ports_file(userID, fileName)
    
    if len(availableDataNodes) < 6:
        return ret
     
    for i in range(0,6):
        availableDataNodeIP = availableDataNodes[i]["IP"]
        availableDataNodePort = availableDataNodes[i]["Port"]
        ret = np.append(ret,availableDataNodeIP)
        ret = np.append(ret, availableDataNodePort)
        update_busy_port(availableDataNodeIP, availableDataNodePort, 1)
    
    print("Return from Download: ", ret)
    return ret

def List(userID):
    ###search in lookup table and sent the list###
    return list_files(userID)


def Getting_requests(port):
    print("Hi from port %s",port)
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    #host_name = socket.gethostname() 
    #host_ip = socket.gethostbyname(host_name)
    socket.bind("tcp://*:%s" % port)
    print ("Running MasterNode on port: ", port)
    while(1):
        # Wait for next request from client
        #message = socket.recv_string()
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        #host_name = socket.gethostname() 
        #host_ip = socket.gethostbyname(host_name)
        socket.bind("tcp://*:%s" % port)
        message = recv_array(socket)
        print(message)
        #socket.send_string("request recieved in master")
        if message[0] == "1":
            result=Upload()
            send_array(socket,result)
        elif message[0] == "2":
            userID = message[1]
            fileName=  message[2]
            result=Download(userID, fileName)
            send_array(socket,result)
        elif message[0] == "3":
            result = List(message[1])
            send_array(socket,result)

#def Data_requests(port):
#    print("Hi from port %s",port)
#    context = zmq.Context()
#    slavesocket = context.socket(zmq.REP)
#    slavesocket.bind("tcp://*:%s" % port)
#    print ("Running Master-Slave on port: ", port)
#    while (1):
#        slavePort = int(slavesocket.recv_string())
#        print("slave port",slavePort)
#        slavesocket.send_string("portnumber recieved")


if __name__ == "__main__":
    print("Heloo")
    masterClientPorts = [9000,9001,9002,9003,9004]
    masterSlavePorts = [9100,9101,9102,9103,9104]
    #----------------------------------

    ps = [Process(target=Getting_requests, args=(port,)) for port in masterClientPorts]
    
    for p in ps: 
        p.start()
    for p in ps:
        p.join()

#    clientsProcesses = Pool(len(masterClientPorts))
#    #print(len(masterClientPorts))
#    clientsProcesses.map_async(Getting_requests,masterClientPorts) 
#    
#    clientsProcesses.join()


    
    
    

    


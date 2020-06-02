# import mysql.connector
import pymysql.cursors
import sys
import zmq
from multiprocessing import Process, Lock, Value

#busy = Value('i', 0)


def main_db_slave(port):
    l = Lock()
    port_db = port
    context = zmq.Context()
    socket_db = context.socket(zmq.SUB)
    socket_db.connect("tcp://localhost:%s" % port_db)
    topicfilter = b'10000'
    socket_db.setsockopt(zmq.SUBSCRIBE, topicfilter)
    while(True):
        topic, string = socket_db.recv_multipart()
        messagedata = string.decode("utf-8").split()
        l.acquire()
        insert_user(messagedata[0], messagedata[1], messagedata[2])
        l.release()


def main_login_connection(_port):
    port = _port
    # Socket to talk to server
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:%s" % port)
    while(True):
        string = socket.recv_string()
        messagedata = string.split()
        print("Login req received!")
        socket.send_string(select_user(messagedata[0], messagedata[1]))


def insert_user(_name, _mail, _pass):
#     mydb = mysql.connector.connect(
#       host="localhost",
#       user="root",
#       passwd="",
#       database="mydatabase1"
#     )
        mydb = pymysql.connect(
                host="localhost",
                user="root",
                passwd="",
                db='mydatabase1',
                charset='utf8',
                cursorclass=pymysql.cursors.DictCursor
        )

        print("slave connected\n")
        #mycursor = mydb.cursor()
        try:
                with mydb.cursor() as mycursor:
                        sql = "INSERT INTO users (name, mail, password) VALUES (%s, %s,%s)"
                        val = (_name, _mail, _pass)
                        mycursor.execute(sql, val)
                        mydb.commit()
                        print(mycursor.rowcount, "user inserted.")
        except pymysql.Error as error:
                print("Failed to insert: {}".format(error))


def select_user(_mail, _pass):
    mydb = pymysql.connect(
                host="localhost",
                user="root",
                passwd="",
                db='mydatabase1',
                charset='utf8',
                cursorclass=pymysql.cursors.DictCursor
        )

    print("slave connected\n")
    with mydb.cursor() as mycursor:
        #mycursor = mydb.cursor()
        sql = "SELECT ID, name FROM users WHERE mail  = %s AND password  = %s"
        data = (_mail, _pass)
        mycursor.execute(sql, data)
        myresult = mycursor.fetchone()
        print(myresult)
        if (myresult is None):
                return("error")
        result = str(myresult['ID']) + " "+myresult['name']
        return(str(result))

if __name__ == "__main__":
    LogPorts = 7000   # ,7001,7002,7003,7004,7005]
    InsertPorts = 5200   # ,5201,5202,5203,5204,5205]
    ps1 = [Process(target=main_db_slave, args=(InsertPorts,))]
    ps2 = [Process(target=main_login_connection, args=(LogPorts,))]
    for p in ps1:
            p.start()
    for p in ps2:
            p.start()
    for p in ps1:
            p.join()
    for p in ps2:
            p.join()

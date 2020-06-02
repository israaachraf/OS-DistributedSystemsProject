# import mysql.connector
import pymysql.cursors
import zmq
import random
import sys
import time
from multiprocessing import Process, Lock

# mydb = mysql.connector.connect(
#         host="localhost",
#         user="root",
#         passwd="",
#         database="mydatabase"
#     )

mydb = pymysql.connect(
  host="localhost",
  user="root",
  passwd="",
  db='mydatabase',
  charset='utf8',
  cursorclass=pymysql.cursors.DictCursor
)


print("master connected\n")
#mycursor = mydb.cursor()
# with client
context = zmq.Context()
# with slaves insert
socket_db = context.socket(zmq.PUB)
# with slaves login


def main_db_master():
    port = 5101
    port_db = 5200  # ,5201,5202,5203,5204,5205]
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:%s" % port)
    socket_db = context.socket(zmq.PUB)
    socket_db.bind("tcp://*:%s" % port_db)
    while True:
            # Wait for next request from client
            message = socket.recv_string()
            print("Received request from client: ", message)
            time.sleep(.01)
            # socket.send("World from %s" % port)
            m = message.split(" ")
            # l.acquire()
            in_check = insert_user(m[0], m[1], m[2])
            # l.release()
            if(in_check):
                socket.send_string("user added!")
                topic = b'10000'
                socket_db.send_multipart([topic, message.encode()])
                time.sleep(.01)
            else:
                socket.send_string("Failed to add user!")
            # socket_db.close()
            # socket_db.unbind("tcp://*:%s" % portdb)
            # socket_db.unbind()
            # print "%d %d" % (topic, messagedata)


def insert_user(_name, _mail, _pass):
    try:
        with mydb.cursor() as mycursor:
            sql = "INSERT INTO users (name, mail, password) VALUES (%s, %s,%s)"
            val = (_name, _mail, _pass)
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "user inserted.")
            return True
    except pymysql.Error as error:
        print("Failed to insert: {}".format(error))
        return False

main_db_master()
# if __name__ == "__main__":
#     ps = [Process(target=main_db_master, args=())]
#     ps2 = [Process(target=main_client_connection, args=())]
#     for p in ps2:
#       p.start()
#     for p in ps:
#       p.start()
#     for p in ps2:
#       p.join()
#     for p in ps:
#       p.join()

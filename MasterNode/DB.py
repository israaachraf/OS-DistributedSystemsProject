import pymysql.cursors
import numpy as np

connection = pymysql.connect(
  host="localhost",
  user="root",
  passwd="",
  db='lookuptable',
  charset='utf8',
  cursorclass=pymysql.cursors.DictCursor
)



def list_files(_userID):
        with connection.cursor() as cursor:
            ret = np.array([])
            sql = "SELECT FileName FROM fileinfo WHERE UserID = %s GROUP BY FileName"
            cursor.execute(sql, _userID)
            result = cursor.fetchall()
            for row in result:
                ret = np.append(ret,row["FileName"])
            return ret

def get_available_ports():
            with connection.cursor() as cursor:
                sql = "SELECT P.IP,P.Port FROM port AS P WHERE P.Busy = '0' AND P.IP IN (SELECT IP FROM datanode WHERE Alive = '1')"
                cursor.execute(sql)
                result = cursor.fetchall()
                return(result)

def get_available_ports_file(_userID,_fileName):
            with connection.cursor() as cursor:
                sql = "SELECT P.IP,P.Port FROM port AS P WHERE P.Busy = '0' AND P.IP IN (SELECT IP FROM datanode WHERE Alive = '1')AND IP IN (SELECT IP FROM fileinfo WHERE UserID = %s AND FileName = %s)"
                data = (_userID,_fileName)
                cursor.execute(sql,data)
                result = cursor.fetchall()
                return(result)

def insert_file(_userID, _fileName, _IP, _filePath):
        try:

            with connection.cursor() as cursor:
                sql = "INSERT INTO fileinfo (UserID, FileName, IP, FilePath) VALUES (%s, %s,%s,%s)"
                val= (_userID, _fileName, _IP, _filePath)
                cursor.execute(sql, val)
                connection.commit()
                print("file inserted.")
        except pymysql.Error as error:
            print(" ")

def update_busy_port(_IP, _port, _busy):
        with connection.cursor() as cursor:
            sql = "UPDATE port SET Busy = %s WHERE IP = %s AND Port = %s"
            print("busy ",_busy ,"IP: " , _IP, " port: ", _port)
            val = (_busy, _IP, _port)
            cursor.execute(sql, val)
            connection.commit()
            print("port busy updated.")


def update_busy_port_relicate(_IP, _port, _busy):
        with connection.cursor() as cursor:
            sql = "UPDATE portreplica SET Busy = %s WHERE IP = %s AND Port = %s"
            print("busy ",_busy ,"IP: " , _IP, " port: ", _port)
            val = (_busy, _IP, _port)
            cursor.execute(sql, val)
            connection.commit()
            print("port replica busy updated.")
            
#return Ip and ports that doesn't contain the file
#IP,Port
def get_Dst_for_replicate(_userID, _fileName):
    with connection.cursor() as cursor:
        sql = "SELECT P.IP,P.Port FROM portreplica AS P WHERE P.Busy = '0' AND P.IP IN (SELECT IP FROM datanode WHERE Alive = '1') AND IP NOT IN (SELECT IP FROM fileinfo WHERE UserID = %s AND FileName = %s) GROUP BY P.IP"
        data = (_userID,_fileName)
        cursor.execute(sql,data)
        result = cursor.fetchall()
        ret = np.array([])
        for i in range(0,len(result)):
            ret = np.append(ret,result[i]["IP"])
            ret = np.append(ret,result[i]["Port"])
        return(ret)

#return Ip and ports that contains the file
#IP,Port
def get_Src_for_replicate(_userID, _fileName):
    with connection.cursor() as cursor:
        sql = "SELECT P.IP,P.Port FROM portreplica AS P WHERE P.Busy = '0' AND P.IP IN (SELECT IP FROM datanode WHERE Alive = '1') AND IP IN (SELECT IP FROM fileinfo WHERE UserID = %s AND FileName = %s)"
        data = (_userID,_fileName)
        cursor.execute(sql,data)
        result = cursor.fetchall()
        ret = np.array([])
        for i in range(0,len(result)):
            ret = np.append(ret,result[i]["IP"])
            ret = np.append(ret,result[i]["Port"])
        return(ret)

#you have to do that: int(float(ret[0])) in userID
#userID,Filename
def get_files_count_less_than_three():
    with connection.cursor() as cursor:
        sql = "SELECT UserID, FileName,COUNT(*) FROM fileinfo GROUP BY UserID, FileName HAVING COUNT(*) < 3 "
        cursor.execute(sql)
        result = cursor.fetchall()
        ret = np.array([])
        for i in range(0,len(result)):
            ret = np.append(ret,result[i]["UserID"])
            ret = np.append(ret,result[i]["FileName"])
            ret = np.append(ret,result[i]["COUNT(*)"])
        return(ret)

#def update_alive_datanode(_dataNodeNumber, _alive):
#     try:
#        with connection.cursor() as cursor:
#            sql = "UPDATE datanode SET Alive = %s WHERE DataNodeNum = %s"
#            val = (_alive, _dataNodeNumber)
#            cursor.execute(sql, val)
#            connection.commit()
#     finally:
#            connection.close()
#
#def update_busy_datanode(_dataNodeNumber, _busy):
#     try:
#        with connection.cursor() as cursor:
#            sql = "UPDATE datanode SET Busy = %s WHERE DataNodeNum = %s"
#            val = (_busy, _dataNodeNumber)
#            cursor.execute(sql, val)
#            connection.commit()
#     finally:
#            connection.close()
#
#def get_alive_datanode():
#        try:
#            with connection.cursor() as cursor:
#                sql = "SELECT DataNodeNum FROM datanode WHERE Alive  = '1'"
#                cursor.execute(sql)
#                result = cursor.fetchall()
#                if (result == ()):
#                    return("ERROR 404")
#                return(result)
#        finally:
#            connection.close()
#            
#
#
#def get_datanode(_userID, _filename):
#     try:
#         with connection.cursor() as cursor:
#             sql = "SELECT DataNodeNum FROM fileinfo WHERE FileName  = %s AND UserID = %s"
#             data = (_filename,_userID)
#             cursor.execute(sql, data)
#             result = cursor.fetchall()
#             if (result == ()):
#                 return("ERROR 404")
#             return(result)
#     finally:
#            connection.close()
#
#def check_alive(_dataNodeNumber):
#        try:
#            with connection.cursor() as cursor:
#                sql = "SELECT Alive FROM datanode WHERE DataNodeNum  = %s"
#                data = (_dataNodeNumber,)
#                cursor.execute(sql, data)
#                result = cursor.fetchall()
#                if (result == ()):
#                    return("ERROR 404")
#                return(result)
#        finally:
#            connection.close()

#print(list_files("36"))
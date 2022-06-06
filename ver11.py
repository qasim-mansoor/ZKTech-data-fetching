from zk import ZK
import zk
import pandas as pd
import logging
import datetime
import psycopg2
from sqlalchemy import create_engine

TABLE = 'attendance_data'
DEVICE_IP = ['192.168.1.203']

conn_string = 'postgresql://postgres:admin@localhost'
db = create_engine(conn_string)
db_conn = db.connect()

formatter_data = logging.Formatter('%(asctime)s \n %(message)s')
formatter_error = logging.Formatter('[%(levelname)s]: [%(asctime)s] [%(message)s]', datefmt='%m/%d/%Y %I:%M:%S %p')

today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)

file = open("last_ran.txt")
file_text = file.readline()

if(file_text != ''):
    last_ran = datetime.datetime.strptime(file_text, '%Y-%m-%d').date()
else:
    last_ran = ' '
# last_ran = datetime.datetime.strptime(file.readline(), '%Y-%m-%d').date()

def setup_logger(name, log_file, format_type, level=logging.INFO):
    # """To setup as many loggers as you want"""

    handler = logging.FileHandler(log_file)        
    handler.setFormatter(format_type)

    logger = logging.getLogger(name)
    logger.setLevel(level)  
    logger.addHandler(handler)

    return logger

def get_today(attendences, user_id_names):
    
    all_attendances = []
    for attd in attendences:
        if(attd.timestamp.date() == yesterday):
            user = int(attd.user_id)
            username = user_id_names[user]
            ts = attd.timestamp
            temp = [user, username, ts]
            all_attendances.append(temp)
    
    records = pd.DataFrame(all_attendances, columns = ['User ID','User Name','Timestamp'])
    return records

def get_all(attendences, user_id_names):
    all_attendances = []
    for attd in attendences:
        user = int(attd.user_id)
        username = user_id_names[user]
        ts = attd.timestamp
        temp = [user, username, ts]
        all_attendances.append(temp)
    
    records = pd.DataFrame(all_attendances, columns = ['User ID','User Name','Timestamp'])
    return records

def get_range(attendences, user_id_names):
    all_attendances = []
    for attd in attendences:
        if(attd.timestamp.date() > last_ran):
            user = int(attd.user_id)
            username = user_id_names[user]
            ts = attd.timestamp
            temp = [user, username, ts]
            all_attendances.append(temp)
        
    records = pd.DataFrame(all_attendances, columns = ['User ID','User Name','Timestamp'])
    return records

# logging.basicConfig(filename='gunicon-server.log',level=logging.DEBUG,
#                     format='[%(levelname)s]: [%(asctime)s] [%(message)s]', datefmt='%m/%d/%Y %I:%M:%S %p')

error_logger = setup_logger('error_logger', 'error.log', formatter_error)
data_logger = setup_logger('data_logger', '{}.log'.format(str(yesterday)), formatter_data)

curr_time =  datetime.datetime.now
conn = None

zk = []
for IP in DEVICE_IP:
    zk = ZK(IP, port=4370, timeout=5, password=0, force_udp=False, ommit_ping=False)

    try:
        # connect to device
        conn = zk.connect()

    except Exception as e:
        error_logger.error(str(e))
        
    


    try:

        # disable device, this method ensures no activity on the device while the process is run
        conn.disable_device()
        
        # Fetch lists of user_id, names 
        users = conn.get_users()

        # Fetch lists of user_id, timestamp, status, punch, uid
        attendances = conn.get_attendance()

        user_ids = []
        user_names = []

        # Creating a dictionary with user_id as key and name as value
        for user in users:
            user_ids.append(int(user.user_id))
            user_names.append(user.name)

        zipObj = zip(user_ids,user_names)
        user_id_names = dict(zipObj)

        #Extra
        # user_df = pd.DataFrame(user_id_names)
        # user_df.to_excel("UserData.xlsx")
        
        if(last_ran == ' '):
            print("All")
            records = get_all(attendances, user_id_names)
        elif(yesterday - datetime.timedelta(days=1) == last_ran):
            print("Today")
            records = get_today(attendances, user_id_names)
        else:
            print("Range")
            records = get_range(attendances, user_id_names)
        
        if not db_conn.dialect.has_table(db_conn, TABLE):
            db_conn.execute('CREATE TABLE {}(  ID SERIAL, ACNO  INTEGER NOT NULL, EmpName varchar(30), Time timestamp, PRIMARY KEY (ID));'.format(TABLE))
            
        records.columns = ['acno','empname','time']
        records.to_sql('attendance_data', con=db_conn, if_exists='append', index=False)

        

        # db_conn = psycopg2.connect(conn_string)
        # db_conn.autocommit = True
        # cursor = db_conn.cursor()

        

        data_logger.info('{}'.format(records.to_string()))
        file.close()

        # records.to_excel("attendence2.xlsx")

    except Exception as e:
        error_logger.error(str(e))

    finally:
        if conn:
            conn.enable_device()
            conn.disconnect()
    
db_conn.close()
file = open("last_ran.txt", 'w')
file.write(str(yesterday))
file.close()
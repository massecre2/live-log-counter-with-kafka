import logging
import os
import shutil
import random
import json 
import datetime as dt



# =============================================================================
#  DEFAULT MAIN WORKING DIRECTORY
# =============================================================================
MAIN_WORK_DIRECTORY = r'C:/Users/OGUZ/Desktop/TEB/'
os.chdir(MAIN_WORK_DIRECTORY)

LOG_PATH = MAIN_WORK_DIRECTORY + 'LOG/'
FINISHED_LOG_FILE_PATH = MAIN_WORK_DIRECTORY + 'Docs/'

REAL_TIME_LOG_FILE_PATH = MAIN_WORK_DIRECTORY + 'LOG/logging.log'
ISTANBUL_FILE_PATH = MAIN_WORK_DIRECTORY + r'RealTime/istanbul.txt'
MOSKOW_FILE_PATH = MAIN_WORK_DIRECTORY + r'RealTime/moscow.txt'
TOKYO_FILE_PATH = MAIN_WORK_DIRECTORY + r'RealTime/tokyo.txt'
BEIJING_FILE_PATH = MAIN_WORK_DIRECTORY + r'RealTime/beijing.txt'
LONDON_FILE_PATH = MAIN_WORK_DIRECTORY + r'RealTime/london.txt'
TIME_PATH = MAIN_WORK_DIRECTORY + r'RealTime/time.txt'

# =============================================================================
# CHECK LOG FILE EXIST OR NOT 
# =============================================================================

exist = os.path.isfile(REAL_TIME_LOG_FILE_PATH)

if exist:
    print('File is already here,!')
else:
    fo = open(LOG_PATH +'logging.log' , 'w')
    fo.close()
    

# =============================================================================
# PRODUCERS SETTINGS FOR SENDING MESSAGE 
# =============================================================================
from kafka import KafkaProducer 
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# =============================================================================
# CONSUMER SETTINGS FOR READ MESSAGE FROM TOPICS
# =============================================================================
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'logfromcity',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     value_deserializer=lambda m: json.loads(m.decode('utf-8')))

# =============================================================================
#  MONGODB SETTINGS UP
# =============================================================================
import pymongo

myclient = pymongo.MongoClient('localhost', 27017) #erisim saglandi 
mydb = myclient["LogFilesDB"] #DATABASE
mycol = mydb["DetailOfLogFiles"] # collection(table)

#DAHA ONCE ISLEM YAPILIP YAPILMADIGINI KONTROL EDEBILIRIZ, 
#ILK DEFA ISLEM YAPACAKSAK , TABLO OLUSTURULMAMISTIR , 
#EGER IKINCI DEFA ISLEM YAPILACAKSA, TABLO OLUSTUURLMUS VERI EKLENMISTIR 

dblist = myclient.list_database_names()
if "mydatabase" in dblist:
  print("The database exists.")

temp_tokyo , temp_istanbul , temp_moskow , temp_beijing , temp_london = [0] , [0] , [0] , [0] , [0]
time = []
istanbul = 0
tokyo = 0
moskow = 0
beijing = 0
london = 0

temp_index = 0

# =============================================================================
# CHECK LOGGING FILE SIZE IS HIGHER THAN 2MB 
# =============================================================================

logging.basicConfig(filename=REAL_TIME_LOG_FILE_PATH,level=logging.DEBUG,format='%(asctime)s - %(levelname)s - %(message)s')

log_detail = ['Istanbul - Hello-from-Istanbul','Tokyo - Hello-from-Tokyo','Moskow - Hello-from-Moskow',
              'Beijing - Hello-from-Beijing','London - Hello-from-London']

size_of_log_files = 0
log_prefix_count = 0
#mongo_index = 0



while size_of_log_files <= 2000000:
    
    random_number = random.randint(0,5)
    rand_for_detal = random.randint(0,4)
    size_of_log_files = os.stat(REAL_TIME_LOG_FILE_PATH).st_size 
    
    #2 
    if random_number == 0 :
        logging.info(log_detail[rand_for_detal])
        
    elif random_number == 1:
        logging.warning(log_detail[rand_for_detal])
        
    elif random_number == 2:
        logging.warn(log_detail[rand_for_detal])
        
    elif random_number == 3:
        logging.error(log_detail[rand_for_detal])
        
    elif random_number == 4:
        logging.error(log_detail[rand_for_detal])
        
    elif random_number == 5: 
        logging.critical(log_detail[rand_for_detal])
        

        
    
    #3
    if(os.stat(REAL_TIME_LOG_FILE_PATH).st_size >= 10000 ):
        #### MONGO DB #####
        myclient = pymongo.MongoClient('localhost', 27017) #erisim saglandi 
        mydb = myclient["LogFilesDB"] #egzersiz adinda veritabani olusturduk .
        mycol = mydb["DetailOfLogFiles"] #customers adinda collection(tablo) olusturduk 
        
        myquery_istanbul = { "city": { "$regex": "^Istanbul" } }
        myquery_tokyo = { "city": { "$regex": "^Tokyo" } }
        myquery_moskow = { "city": { "$regex": "^Moskow" } }
        myquery_beijing = { "city": { "$regex": "^Beijing" } }
        myquery_london = { "city": { "$regex": "^London" } }
        
        
        mydoc_istanbul = mycol.find(myquery_istanbul)
        mydoc_tokyo = mycol.find(myquery_tokyo)
        mydoc_moskow = mycol.find(myquery_moskow)
        mydoc_beijing = mycol.find(myquery_beijing)
        mydoc_london = mycol.find(myquery_london)
        
        for x in mydoc_istanbul:
          istanbul += 1
        
        for x in mydoc_tokyo:
          tokyo += 1
          
        for x in mydoc_moskow:
          moskow += 1
          
        for x in mydoc_beijing:
          beijing += 1
          
        for x in mydoc_london:
          london += 1
      
        temp_istanbul.append(istanbul)
        temp_istanbul[temp_index] = temp_istanbul[temp_index+1]-temp_istanbul[temp_index]        
        with open(ISTANBUL_FILE_PATH, 'w') as f:
            for item in temp_istanbul:
                f.write('{0}\n'.format(item))

        temp_tokyo.append(tokyo)
        temp_tokyo[temp_index] = temp_tokyo[temp_index+1]-temp_tokyo[temp_index]
        with open(TOKYO_FILE_PATH, 'w') as f:
            for item in temp_tokyo:
                f.write('{0}\n'.format(item))
                
        temp_moskow.append(moskow)
        temp_moskow[temp_index] = temp_moskow[temp_index+1]-temp_moskow[temp_index]
        with open(MOSKOW_FILE_PATH, 'w') as f:
            for item in temp_moskow:
                f.write('{0}\n'.format(item))
        
        
        temp_beijing.append(beijing)
        temp_beijing[temp_index] = temp_beijing[temp_index+1]-temp_beijing[temp_index]
        with open(BEIJING_FILE_PATH, 'w') as f:
            for item in temp_beijing:
                f.write('{0}\n'.format(item))
                
                
        temp_london.append(london)
        temp_london[temp_index] = temp_london[temp_index+1]-temp_london[temp_index]
        with open(LONDON_FILE_PATH, 'w') as f:
            for item in temp_london:
                f.write('{0}\n'.format(item))
                
        time.append(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        with open(TIME_PATH, 'w') as f:
            for item in time:
                f.write('{0}\n'.format(item))
                
        istanbul = 0
        tokyo = 0
        moskow = 0
        beijing = 0
        london = 0


        
        logging.shutdown()
        new_log_name_path = LOG_PATH + 'LOG' + str(log_prefix_count)+ '.txt'
        os.rename(REAL_TIME_LOG_FILE_PATH, new_log_name_path)
        shutil.move(new_log_name_path, FINISHED_LOG_FILE_PATH)
        
        temp_index += 1

        #4
        filename = FINISHED_LOG_FILE_PATH+ 'LOG' + str(log_prefix_count)+ '.txt'
        with open(filename) as f:
            content = f.readlines()
        
        temporary = [x.strip() for x in content]
        
        for line in temporary:
            split_line_for_city = line.split(' ')
            city = split_line_for_city[5]
            producer.send('logfromcity' , { city : line })
            
        os.remove(filename)
        log_prefix_count += 1
        
        #make new log file 
        fo = open(LOG_PATH +'logging.log' , 'w')
        fo.close()

    
    for message in consumer:
        message = message.value

        main_topic = json.dumps(message)
        main_topic = main_topic.translate({ord(i): None for i in '{}"'})
        main_topic = main_topic.split(' ')
        
        date = main_topic[1] + ' ' + main_topic[2]
        # TODO : date getting some error . 
        #date = datetime.strptime(date,'%Y-%m-%d %H:%M:%S,%f')
        date = main_topic[1] + ' ' + main_topic[2]
        log_level = main_topic[4]
        city = main_topic[6]
        message_from_main_topic = main_topic[8:]
        
        post_to_mongo = {
                         #"_id"  : mongo_index ,
                         "city" : city , 
                         "date" : date , 
                         "log_level" : log_level ,
                         "message" : message_from_main_topic
                         }
        
        x = mycol.insert_one(post_to_mongo)

        print('THAT LOG MOVES TO DB SUCESSFULLY ' , x)
        #mongo_index += 1
      
        print('#################{} ADDED TO NODE'.format(message))
        break
    

      

                

        
        
        
        
        
    
    
    
    
	
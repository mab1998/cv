# -*- coding: utf-8 -*-
"""
Created on Tue Aug 11 19:53:36 2020

@author: ambs
"""
import sqlite3
from sqlite3 import Error
import numpy
#import schedule
import time
import MySQLdb as mdb
import socket
import json
import pandas as pd
import os
import argparse
import psycopg2
import numpy as np
import db_init
from faker import Faker
fake = Faker('en_GB')
import json
     
def get_list(conn):

    with conn:
        cur = conn.cursor()
#        cur.execute("SET NAMES utf8mb4;")
        
    #    cur.execute("SELECT MIN(user_id) AS id, user FROM tweets GROUP BY user")
        cur.execute("SELECT id,tweet,date,replies,likes,retweets,hashtags, user_id FROM tweets")

#        cur.execute("SELECT id, user_id,user FROM tweets")
     
        rows = cur.fetchall()
    conn.commit()
    conn.close()
    return rows

def user_insert(conn, entry):
    try:
        cursor = conn.cursor()
        cursor.executemany('''INSERT INTO twitter_authors (id,name) values (%s, %s)''', enrtry_users)    
        conn.commit()
        return conn
    except Exception as e:
        return str(e)
    
def filldata_result_json(conn,):
    filename='results.json'
    with open(filename, encoding='utf8') as json_file:
        data = json.load(json_file)
    datalist=[]
    for p in data:
            dd={}
            for pp in  p['label']:
              dd[pp['labels'][0]]=pp['text']
            try:  
                name=dd['Name']
            except:
                name=''
            try:
                email=dd['Email']
            except:
                email=''
            try:
                date_of_birth=dd['Date_birth']
            except:
                date_of_birth=''
            try:
                address_id=dd['Location']
            except:
                address_id
            try:
                experiance=dd['experiance']
            except:
                experiance=''
            try:
                education=dd['education']
            except:
                education=''
            try:
                skills=dd['skills']
            except:
                skills=''
            try:
                qualifications=dd['qualifications']
            except:
                qualifications=''
            try:
                phone=dd['mobile']
            except:
                phone=''
            
            datalist.append([name,email,date_of_birth,address_id,experiance,education,skills,qualifications,phone])
            enrtry_jobseeker=(name,email,date_of_birth,address_id,phone)
            cursor = conn.cursor()
            cursor.execute("""INSERT INTO jobseeker (name,email,date_of_birth,address_id,phone) values (%s, %s, %s, %s, %s)""", enrtry_jobseeker)    
            conn.commit()
            # time.sleep(1)
            jobseeker_id = cursor.lastrowid
            cursor.execute("""INSERT INTO jobseeker_qualifications (jobseeker_id) values( %s )""", (int(jobseeker_id),))    
            conn.commit()
            # time.sleep(1)
            qualification_id = cursor.lastrowid
            
            cursor.execute("""INSERT INTO qualifications (qualification_id,qualification,experience,education,skills) values( %s, %s, %s, %s, %s)""", (qualification_id,qualifications,experiance,education,skills))    
            conn.commit()    
    
def INSERTdataS_json(conn,dd):

            try:  
                name=dd['Name']
            except:
                name=''
            try:
                email=dd['Email']
            except:
                email=''
            try:
                date_of_birth=dd['Date_birth']
            except:
                date_of_birth=''
            try:
                address_id=dd['Location']
            except:
                address_id
            try:
                experiance=dd['experiance']
            except:
                experiance=''
            try:
                education=dd['education']
            except:
                education=''
            try:
                skills=dd['skills']
            except:
                skills=''
            try:
                qualifications=dd['qualifications']
            except:
                qualifications=''
            try:
                phone=dd['mobile']
            except:
                phone=''
            
            datalist.append([name,email,date_of_birth,address_id,experiance,education,skills,qualifications,phone])
            enrtry_jobseeker=(name,email,date_of_birth,address_id,phone)
            cursor = conn.cursor()
            cursor.execute("""INSERT INTO jobseeker (name,email,date_of_birth,address_id,phone) values (%s, %s, %s, %s, %s)""", enrtry_jobseeker)    
            conn.commit()
            # time.sleep(1)
            jobseeker_id = cursor.lastrowid
            cursor.execute("""INSERT INTO jobseeker_qualifications (jobseeker_id) values( %s )""", (int(jobseeker_id),))    
            conn.commit()
            # time.sleep(1)
            qualification_id = cursor.lastrowid
            
            cursor.execute("""INSERT INTO qualifications (qualification_id,qualification,experience,education,skills) values( %s, %s, %s, %s, %s)""", (qualification_id,qualifications,experiance,education,skills))    
            conn.commit()    






def from_csvfiles(conn):
        columnlist=['Resume_title','work_experiences','Educations','Skills','Qualifications','Additional_Information']
        df = pd.read_csv("resume_data.csv",header=0)
        DataFrame=pd.DataFrame()
        dd_list=[]
        i=0
        for i in range(1,len(df)):
          #  print(df['Resume_title'][i])
            # i=i+1
            # if i==10:
            #     break
            try:
        
                if 'NONE' not in df['Resume_title'][i]  :
                    if 'NONE' not in  df['Skills'][i] :
                        Resume_title=df['Resume_title'][i]
                        work_experiences=get_exp(df["work_experiences"][i])
                        Educations=get_edu(df['Educations'][i])
                        Skills=df['Skills'][i]
                        Additional_Information=df['Additional Information'][i]
                        Qualifications=df['Skills'][i]
                        
                        row = [Resume_title,work_experiences,Educations,Skills,Qualifications,Additional_Information]	
                #        print(row)
                        DataFrame = DataFrame.append(pd.DataFrame(pd.DataFrame(np.array(row).reshape(1,6), columns=columnlist)),ignore_index=True)
            except:
                    pass
        
        
        
        for i in range(1,len(DataFrame)):
            dd['Name']=fake.name()
            dd['Email']=''
            dd['Date_birth']=''
            dd['Location']=fake.address()
            dd['experiance']=DataFrame["work_experiences"][i]
            dd['education']=DataFrame['Educations'][i]
            dd['skills']=DataFrame['Skills'][i]
            dd['qualifications']=DataFrame['Qualifications'][i]
            dd['mobile']='0666666666'
            filldata_json(conn,dd)

def get_exp(ff):
    for i in range(0,10):
      if '{}:'.format(str(i)) in ff:
            ff=ff.replace('{}:'.format(str(i)),'"{}":'.format(str(i))).replace('{\'','{\"').replace(':\':',':\":').replace(': \'',': \"').replace('\'}','\"}')    
      else:
         break

    dd=json.loads(ff)  
    exp=''
    for k in range(0,i):
        ddd=dd['{}'.format(str(k))]    
        wtitle=ddd[0]['wtitle:']
        wcompany=ddd[1]['wcompany:']
        wduration=ddd[4]['wduration:']
        exp=exp+wtitle+':'+wcompany +':'+wduration+'\n'
    return exp

    

def get_edu(ff): 
    for i in range(0,10):
      if '{}:'.format(str(i)) in ff:
            ff=ff.replace('{}:'.format(str(i)),'"{}":'.format(str(i))).replace('{\'','{\"').replace(':\':',':\":').replace(': \'',': \"').replace('\'}','\"}')    
      else:
         break

    dd=json.loads(ff)  
    edu=''
    for k in range(0,i):
        ddd=dd['{}'.format(str(k))]    
        e_title=ddd[0]['e_title:']
        e_schoolname=ddd[1]['e_schoolname:']
        e_duration=ddd[4]['e_duration:']
        edu=edu+e_title+':'+e_schoolname +':'+e_duration+'\n'
    return edu    
        
if __name__ == '__main__':   
        

        with open("Config.json") as datafile:
                config = json.load(datafile)
            
        db_file="./resumes.db"
        
        
        host=config['host']
        port=config['port']
        user=config['user']
        password=config['password']
        db=config['database_name']
        charset=config['charset']
        db_type=config['db_type']       
        
        if  db_type=='msql':
            print('Mysql Database Selected............')
            conn = mdb.connect(host=host, user=user, password=password, db=db, charset=charset ,init_command='SET NAMES utf8mb4')
        elif db_type=='psql':
            print('PostgreSQL Database Selected............')
            conn = psycopg2.connect(host=host, user=user, password=password, dbname=db)
        elif db_type=='msqllite':
            print('MySQLLite Database Selected............')
            conn = sqlite3.connect(db_file)
        else:
            print('Type of databse not support  please entre :msql / psql')
            
        init(conn)

filename='result.json'        

with open(filename, encoding='utf8') as json_file:
    data = json.load(json_file)            

datalist=[]

Permanent
Contract
Temporary
          

def search(keywords_data,weightsJson):

    
weightsJson={experience:20
             skills:5
             qualification: 1  ,
             Years_experience:
             Salary:   
            Location
            Job_type
            }

input_data={
keywords:['bricklayer',"brick layer","brick-layer" ,"builder","brickmason", "brick mason", "stonemason"],
Job_type:,
Location:,
Salary:,
Years_experience:
 }



keywords=()

"""SELECT *,
    IF(
            `experience` LIKE "builder%",  20, 
         IF(`experience` LIKE "%builder%", 10, 0)
      )
      + IF(`skills` LIKE "%builder%", 5,  0)
      + IF(`qualification`         LIKE "%builder%", 1,  0)
    AS `weight`
FROM `qualifications`
WHERE (
    `experience` LIKE "%builder%" 
    OR `skills` LIKE "%builder%"
    OR `qualification`         LIKE "%builder%"
)
ORDER BY `weight` DESC
LIMIT 20"""






# SELECT * FROM `myTable` 
# WHERE (`name` LIKE "%searchterm%" OR `description` LIKE %searchterm%" OR `url` LIKE "%searchterm%")
# ORDER BY CASE
# WHEN `name`        LIKE "searchterm%"  THEN 20
# WHEN `name`        LIKE "%searchterm%" THEN 10
# WHEN `description` LIKE "%searchterm%" THEN 5
# WHEN `url`         LIKE "%searchterm%" THEN 1
# ELSE 0
# END
# LIMIT 20






























fake.name()
# 'Lucy Cechtelar'

print(fake.address())
# '426 Jordy Lodge
#  Cartwrightshire, SC 88120-6700'

fake.text()

fake.factories



 
from fastapi import FastAPI
from enum import Enum
import sqlite3
from sqlite3 import Error

import mysql.connector
from mysql.connector import Error

from pydantic import BaseModel
from typing import List

class keywords(BaseModel):
    # foo: Foo
    # bars: List[Bar]
    keywords_global:List[str]=["bricklayer",  "brick layer", "brick-layer"]
    personal_summary:List[str]=["ing","bash"]
    hobbies_interests:List[str]=["sport","travels"]
    references:List[str]
    awards:List[str]
    skills:List[str]=["it"]
    qualifications:List[str]
    employment_history:List[str]
    Address:List[str]
    it_skills:List[str]
    publications:List[str]
    accreditations:List[str]
    professional_development:List[str]
    voluntary_work:List[str]
    
    
class keywords_weitheds(BaseModel):
        personal_summary:int=1
        hobbies_interests:int=2
        references:int=3
        awards:int=4
        skills:int=5
        qaulifications:int=6
        employment_history:int=7
        Address:int=8
        it_skills:int=9
        publications:int=10
        accreditations:int=11
        professional_development:int=12
        voluntary_work:int=13
        
default_keywords_weight=keywords_weitheds(personal_summary=1,
                          hobbies_interests=2,
                          references=3,
                          awards=4,
                          skills=5,
                          qaulifications=6,
                          employment_history=7,
                          Address=8,
                          it_skills=9,
                          publications=10,
                          accreditations=11,
                          professional_development=12,
                          voluntary_work=13) 

default_keywords=keywords(keywords_global=["bricklayer",  "brick layer", "brick-layer"],
    personal_summary=["ing","bash"],
    hobbies_interests=["sport","travels"],
    skills=["it"],
        qualifications=[],
    employment_history=[],
    Address=[],
    it_skills=[],
    publications=[],
    accreditations=[],
    professional_development=[],
    voluntary_work=[],
    references=[],
    awards=[]
    
    )


class input_data(BaseModel):
    keywords: str
    # experience: str
    # keywords: str

class Weight(str, Enum):
    one="1"
    five = "5"
    ten = "10"
    twenty = "20"
    fifty = "50"


def create_connection():
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
#    conn = None
    connection=None
    try:
        # conn = sqlite3.connect(db_file)

        connection = mysql.connector.connect(host='localhost',
                                         database='cv',
                                         user='admin',
                                         password='vEvJVJXwJUjgLbR7')
    except Error as e:
        print(e)

    return connection



def select_all_tasks(keywords,keywords_weitheds,experience_weight,skills_weight,qualification_weight,education_weight):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
   #  keywords={"keywords_global":["bricklayer",  "brick layer", "brick-layer"],
   #            "personal_summary":["ing","bash"],
   #            "hobbies_interests":["sport","travels"],
   #            "references":[],
   #            "awards":[],
   #            "skills":["it"],
   #            "qualifications":[],
   #            "employment_history":[],
   #            "Address":[],
   #            "it_skills":[],
   #            "publications":[],
   #            "accreditations":[],
   #            "professional_development":[],
   #            "voluntary_work":[]}
    
   # keywords_weitheds={ "personal_summary":1,
   #            "hobbies_interests":2,
   #            "references":3,
   #            "awards":4,
   #            "skills":5,
   #            "qaulifications":6,
   #            "employment_history":7,
   #            "Address":8,
   #            "it_skills":9,
   #            "publications":10,
   #            "accreditations":11,
   #            "professional_development":12,
   #            "voluntary_work":13}    
    conn= create_connection()
    cur = conn.cursor()
  
    # experience=""
    # qualification=""
    # keywords=["pipe filter","pipe-filter","pipefilter","plumber","plumbing"]
    # keywords=["bricklayer",  "brick layer", "brick-layer"]
    rlist={}

    q1=""
    q2=""
    st=False
    for keys in keywords:
        if keywords[keys] ==[] or  keys=="keywords_global":
            pass
        else:
                kk=''
                 
                for kk in keywords[keys]:
                    if st:
                        q1=q1+'+ IF(`{0}` LIKE "%{1}%", {2}, 0)'.format(keys,kk,keywords_weitheds[keys])
                        q2=q2+'OR `{0}` LIKE "%{1}%"'.format(keys,kk)
                    else:
                        st=True
                        q1=q1+'IF(`{0}` LIKE "%{1}%", {2}, 0)'.format(keys,kk,keywords_weitheds[keys])
                        q2=q2+'`{0}` LIKE "%{1}%"'.format(keys,kk)
                                               
                      
         
    print(cur.execute( """SELECT qualification_id,{0} AS `weight`
    FROM `qualifications`
    WHERE ({1})
    ORDER BY `weight` DESC
    LIMIT 100""".format(q1,q2)))
    rows = cur.fetchall()   
    for row in rows:
        try:
            rlist[row[0]]= rlist[row[0]]+row[1]
        except:
            rlist[row[0]]= row[1]
            

    for keyword in keywords["keywords_global"]:
        
        print(cur.execute(    """SELECT qualification_id,
            IF(
                    `employment_history` LIKE "{0}%",  {1}, 
                IF(`employment_history` LIKE "%{0}%", {1}, 0)
            )
            + IF(`skills` LIKE "%{0}%", {2},  0)
            + IF(`qualifications`         LIKE "%{0}%", {3},  0)
            + IF(`education`         LIKE "%{0}%", {4},  0)
            AS `weight`
        FROM `qualifications`
        WHERE (
            `employment_history` LIKE "%{0}%" 
            OR `skills` LIKE "%{0}%"
            OR `qualifications`         LIKE "%{0}%"
            OR `education`         LIKE "%{0}%"
        )
        ORDER BY `weight` DESC
        LIMIT 100""".format(keyword,experience_weight,skills_weight,qualification_weight,education_weight)))
        rows = cur.fetchall()
        for row in rows:
            try:
                rlist[row[0]]= rlist[row[0]]+row[1]
            except:
                rlist[row[0]]= row[1]
            

    result=sorted(rlist.items(), key=lambda x: x[1], reverse=True)

    list_results=[]
    emails=[]
    for res in result:
        
        
        jobseeker={}

        cur.execute(    """SELECT jobseeker_id  FROM `jobseeker_qualifications` WHERE qualification_id=%s""", (int(res[0]),))

        jobseeker_id = cur.fetchall()[0][0]
        conn.commit()

        cur.execute(    """SELECT name,cv_doc_id,phone,email  FROM `jobseeker` WHERE id=%s""", (int(jobseeker_id),))
        rows=cur.fetchall()[0]
        conn.commit()

    

        jobseeker['name'] = rows[0]
        jobseeker['cv_doc_id'] = rows[1]
        # jobseeker['phone'] = rows[2]
        # jobseeker['email'] = rows[3]  
        jobseeker['Rating'] =res[1] 
        list_results.append(jobseeker)    
            

    return list_results




app = FastAPI()




@app.post("/")
def Search(keywords:keywords=default_keywords,keywords_weitheds:keywords_weitheds=default_keywords_weight,experience_weight: int =20,skills_weight: int=30,qualification_weight: int=40,education_weight:int=10):
    # return {"Weight": experience.value}
    return select_all_tasks(keywords,keywords_weitheds,experience_weight,skills_weight,qualification_weight,education_weight)

     
    

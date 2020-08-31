from fastapi import FastAPI
from enum import Enum
import sqlite3
from sqlite3 import Error

import mysql.connector
from mysql.connector import Error

from pydantic import BaseModel



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


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
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

def select_all_tasks(conn,keywords,experience_weight,skills_weight,qualification_weight,education_weight):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    # experience=""
    # qualification=""
    # keywords=["pipe filter","pipe-filter","pipefilter","plumber","plumbing"]
    # keywords=["bricklayer",  "brick layer", "brick-layer"]

            
            
            
    rlist={}

    for keyword in keywords:
        
        print(cur.execute(    """SELECT qualification_id,
            IF(
                    `experience` LIKE "{0}%",  {1}, 
                IF(`experience` LIKE "%{0}%", {1}, 0)
            )
            + IF(`skills` LIKE "%{0}%", {2},  0)
            + IF(`qualification`         LIKE "%{0}%", {3},  0)
            + IF(`education`         LIKE "%{0}%", {4},  0)
            AS `weight`
        FROM `qualifications`
        WHERE (
            `experience` LIKE "%{0}%" 
            OR `skills` LIKE "%{0}%"
            OR `qualification`         LIKE "%{0}%"
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
            
    result={k: v for k, v in sorted(rlist.items(), key=lambda item: item[1],reverse=True)}

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

conn=create_connection("d.db")


@app.post("/")
def Search(keywords:List[str],experience_weight: int =20,skills_weight: int=30,qualification_weight: int=40,education_weight:int=10):
    # return {"Weight": experience.value}
    return select_all_tasks(conn,keywords,experience_weight,skills_weight,qualification_weight,education_weight)

     
    

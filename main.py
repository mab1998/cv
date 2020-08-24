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

class Country(str, Enum):
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

def select_all_tasks(conn,experience,skills,qualification,experience_weight,skills_weight,qualification_weight):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    experience=""
    qualification=""
    skills=""
    cur.execute(    """SELECT *,
        IF(
                `experience` LIKE "{0}%",  {3}, 
            IF(`experience` LIKE "%{0}%", {3}, 0)
        )
        + IF(`skills` LIKE "%{1}%", {4},  0)
        + IF(`qualification`         LIKE "%{2}%", {5},  0)
        AS `weight`
    FROM `qualifications`
    WHERE (
        `experience` LIKE "%{0}%" 
        OR `skills` LIKE "%{1}%"
        OR `qualification`         LIKE "%{2}%"
    )
    ORDER BY `weight` DESC
    LIMIT 20""".format(experience,skills,qualification,experience_weight,skills_weight,qualification_weight))


    rows = cur.fetchall()

    return rows




app = FastAPI()

conn=create_connection("d.db")


@app.post("/")
def get_something(experience:str,skills:str,qualification:str,experience_weight: Country = Country.twenty,skills_weight: Country = Country.five,qualification_weight: Country = Country.one):
    # return {"country": experience.value}
    return select_all_tasks(conn,experience,skills,qualification,experience_weight,skills_weight,qualification_weight)

     
    

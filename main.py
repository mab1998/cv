from fastapi import FastAPI
from enum import Enum
import sqlite3
from sqlite3 import Error

from pydantic import BaseModel

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect("d.db")
    except Error as e:
        print(e)

    return conn

def select_all_tasks(conn):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute(    """SELECT *,
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
    LIMIT 20""")

    rows = cur.fetchall()

    for row in rows:
        print(row)

class input_data(BaseModel):
    keywords: str
    experience: str
    # keywords: str

class Country(str, Enum):
    one="1"
    five = "5"
    ten = "10"
    twenty = "20"
    fifty = "50"




app = FastAPI()

builder=""

slq_query="SELECT *,"
slq_query+="IF( `experience` LIKE" 
slq_query+=builder
slq_query+=

# input_data={
# keywords:['bricklayer',"brick layer","brick-layer" ,"builder","brickmason", "brick mason", "stonemason"],
# Job_type:,}


@app.post("/")
def get_something(input:input_data,experience: Country = Country.twenty,skills: Country = Country.five,qualification: Country = Country.one):
    # return {"country": experience.value}
    return input
    

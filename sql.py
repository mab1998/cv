import sqlite3
from sqlite3 import Error

import mysql.connector
from mysql.connector import Error

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
                                         user='root',
                                         password='')
    except Error as e:
        print(e)

    return connection

def select_all_tasks(conn):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    build="build"
    cur.execute(    """SELECT *,
        IF(
                `experience` LIKE "{0}%",  20, 
            IF(`experience` LIKE "%{0}%", 10, 0)
        )
        + IF(`skills` LIKE "%{0}%", 5,  0)
        + IF(`qualification`         LIKE "%{0}%", 1,  0)
        AS `weight`
    FROM `qualifications`
    WHERE (
        `experience` LIKE "%{0}%" 
        OR `skills` LIKE "%{0}%"
        OR `qualification`         LIKE "%{0}%"
    )
    ORDER BY `weight` DESC
    LIMIT 20""".format(build))


    rows = cur.fetchall()
    # print(rows)
    for row in rows:
        print(row)

conn=create_connection("d.db")
select_all_tasks(conn)
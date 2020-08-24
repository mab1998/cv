import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
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

conn=create_connection("d.db")
select_all_tasks(conn)
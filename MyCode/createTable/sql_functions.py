'''
 there are functions about sql
'''

import MySQLdb

def create_table(sql, cursor, name):
    '''
    create a table in database

    when you want to create an table named name in the cursor database and the sql quotes are param sql,
    you can use this function

    :param sql: the execution  sql
    :param cursor: the database cursor
    :param name: the table name
    :return: Exception
    '''
    try:
        cursor.execute(sql)
        print "you have create a new table %s" % name
    except:
        print "you create the table illegal"
        raise Exception

def insert_table(sql, cursor, conn):
    '''
    insert a sql into a table

    execute the sql and insert a row into a special table

    :param sql: the execution sql
    :param cursor: the database cursor
    :param conn: the database
    :return: Exception
    '''
    try:
        cursor.execute(sql)
        conn.commit()
    except:
        print "you insert the table illegal"
        raise Exception

def search_table(sql, cursor):
    '''
    insert a sql into a table

    execute the sql and insert a row into a special table

    :param sql: the execution sql
    :param cursor: the database cursor
    :param conn: the database
    :return: Exception
    '''
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        return result
    except:
        print "you search the table illegal"
        raise Exception

def connect_database():
    """
    get the the connection and cursor from a database

    :return: the database conn, and the cursor
    """
    conn = MySQLdb.connect(host="localhost", user="root", passwd="root", db="usermodel")
    cursor = conn.cursor()
    print "you have connect the database successfully"
    return conn, cursor

def close_database(conn):
    """"
      close the database

    :param conn: the database connection
    :return: you

    """
    conn.close()
    print "you have close the database successfully"
    return 0

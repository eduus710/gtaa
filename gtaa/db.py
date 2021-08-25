import mysql.connector


# wrapper for connection + cursor. rudimentary
class MyDB:
    def __init__(self):
        self._db_connection = mysql.connector.connect(
            host='127.0.0.1',
            user='root',
            password='Root1967!',
            autocommit=True)
        self._db_cur = self._db_connection.cursor()

    def query(self, query, params=''):
        return self._db_cur.execute(query, params)

    def __del__(self):
        self._db_connection.close()


# open a MYSQL cursor against db/schema
def mysql_db(schema):
    db = MyDB()
    db.query('USE {}'.format(schema))
    return db


# execute a statement with optional parameters
def execute(cursor, stmt, params=None):
    if cursor is not None:
        # todo - this smells, can we collapse to a single execute statement
        if params is None:
            cursor.execute(stmt)
        else:
            cursor.execute(stmt, params)
    #        print(cursor.statement)
    else:
        print(stmt)


# execute a statement with many parameters, optimized
def execute_many(cursor, stmt, many_params):
    if cursor is not None:
        #        print(stmt)
        #        print(many_params)
        cursor.executemany(stmt, many_params)
        #        print(cursor.statement)
        #        print(cursor.rowcount, "Record inserted successfully")
    else:
        print(stmt)

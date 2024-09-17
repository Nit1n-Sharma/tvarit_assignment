import mysql.connector
from resources.dev.config import *
def get_mysql_connection():
    connection = mysql.connector.connect(
        host = host,
        user = user,
        password = password,
        database = database
    )
    return connection
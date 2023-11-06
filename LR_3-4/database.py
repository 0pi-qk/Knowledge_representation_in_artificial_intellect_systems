import psycopg2
from psycopg2 import OperationalError


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Подключение к PostgreSQL БД выполнено успешно")
    except OperationalError as e:
        print(f"The error connection '{e}' occurred")
    return connection

def execute_query(connection, query):
    connection.commit()
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Запрос выполнен")
    except OperationalError as e:
        print(f"The error '{e}' occurred")


# SQL запрос для создания таблицы wordList
create_wordList_table = """
CREATE TABLE IF NOT EXISTS wordList(
rowid SERIAL NOT NULL PRIMARY KEY, 
word TEXT,
isFiltred boolean
);
"""

# SQL запрос для создания таблицы URLList
create_URLList_table = """
CREATE TABLE IF NOT EXISTS urllist(
rowid SERIAL NOT NULL PRIMARY KEY, 
URL TEXT
);
"""

# SQL запрос для создания таблицы linkBtwURL
create_linkBtwURL_table = """
CREATE TABLE IF NOT EXISTS linkBtwURL(
rowid SERIAL NOT NULL PRIMARY KEY, 
fk_FromURL_id INTEGER REFERENCES urllist (rowid),
fk_ToURL_id INTEGER REFERENCES urllist (rowid)
);
"""

# SQL запрос для создания таблицы wordLocation
create_wordLocation_table = """
CREATE TABLE IF NOT EXISTS wordLocation(
rowid SERIAL NOT NULL PRIMARY KEY, 
fk_word_id INTEGER REFERENCES wordList (rowid),
fk_URL_id INTEGER REFERENCES URLList (rowid),
location INTEGER
);
"""

# SQL запрос для создания таблицы linkWord
create_linkWord_table = """
CREATE TABLE IF NOT EXISTS linkWord(
rowid SERIAL NOT NULL PRIMARY KEY, 
fk_word_id INTEGER REFERENCES wordList (rowid),
fk_link_id INTEGER REFERENCES linkBtwURL (rowid)
);
"""


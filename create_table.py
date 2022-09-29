import pymysql as sql
from sql_queries import create_table_queries,drop_table_queries

def create_databases():
    conn = sql.connect(host="127.0.0.1",user="__myuser",passwd="__mypassword",db="test_table",port=3306,autocommit=True)
    cur = conn.cursor()
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb")
    conn.close()

    conn = sql.connect(host="127.0.0.1",user="__myuser",passwd="__mypassword",db="sparkifydb",port=3306)
    cur = conn.cursor()
    
    return cur,conn


def drop_table(cur,conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_table(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    cur, conn = create_databases()

    drop_table(cur, conn)
    print("Table dropped successfully!")

    create_table(cur, conn)
    print("Table create successfully!")

if __name__ == "__main__":
    main()



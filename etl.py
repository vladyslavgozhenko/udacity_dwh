import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries #, dell_rows_with_null


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.InternalError as e:
            print(e)

# def del_rows_with_null(cur, conn):            
#     for query in dell_rows_with_null:
#         cur.execute(query)
#         conn.commit()        
        
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
#     del_rows_with_null(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
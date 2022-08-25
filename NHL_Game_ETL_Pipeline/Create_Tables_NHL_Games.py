import psycopg2 as ps
from SQL_Queries_NHL_Games import *

###########################################################################################################################################
# NEW CODE BLOCK - Create nhlgamesdb
###########################################################################################################################################

def create_database():
    """
    - Creates and connects to the nhlgamesdb
    - Returns the connection and cursor to nhlgamesdb
    """

    # connect to default database port: 5432
    conn = ps.connect('''
    
        host=localhost
        dbname=postgres
        user=postgres
        password=iEchu133
           
    ''')

    conn.set_session(autocommit = True)
    cur = conn.cursor()

    # create nhlgamesdb database with UTF8 encoding
    cur.execute('DROP DATABASE IF EXISTS nhlgamesdb;')
    cur.execute("CREATE DATABASE nhlgamesdb WITH ENCODING 'utf8' TEMPLATE template0;")

    # close connection to default database
    conn.close()

    # connect to nhlgamesdb database
    conn = ps.connect('''
    
        host=localhost
        dbname=nhlgamesdb
        user=postgres
        password=iEchu133
        
    ''')

    cur = conn.cursor()

    return cur, conn


###########################################################################################################################################
# NEW CODE BLOCK - Create tables in nhlgamesdb
###########################################################################################################################################


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    
###########################################################################################################################################
# NEW CODE BLOCK - Runs etl pipeline
###########################################################################################################################################

def main():
    """
    - Drops (if exists) and creates the nhlgamesdb database
    - Establishes connection with the nhlgamesdb database and gets cursor to it
    - Drops all the tables
    - Closes the connection
    """

    try:
        cur, conn = create_database()
        
        # Drop tables
        drop_tables(
            cur = cur, 
            conn = conn
        )
        
        # Create tables
        create_tables(
            cur = cur, 
            conn = conn
        )
        
        print('Tables have been created: teams, opponents, date, and game_facts')
    
        cur.close()
        conn.close()

    except ps.Error as e:
        print('\n Error:')
        print(e)


if __name__ == "__main__":
    main()
    
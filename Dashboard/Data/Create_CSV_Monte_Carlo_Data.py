import pandas as pd
import psycopg2 as ps

def nhl_monte_carlo_csv():
    # Connect to database
    ############################################################
    try:
        conn = ps.connect('''
            host=localhost
            dbname=nhlgamesdb
            user=postgres
            password=iEchu133
        ''')

        cur = conn.cursor()

        print('Successfully connected to nhlgamesdb')

    except ps.Error as e:
        print('\n Database Error:')
        print(e)
    
    # monte_carlo_data
    ############################################################
    try:
        # Query monte_carlo_data
        query = '''
            SELECT *
            FROM analytics.monte_carlo_data_2021_2022;
        '''

        cur.execute(query)
        monte_carlo_data_df = cur.fetchall()

        monte_carlo_data_columns = [
            'sk_games',
            'Date',
            'Team',
            'Opponent',
            'Real_Score',
            'Opponent_Real_Score'
        ]


        # Convert view to pandas data frame
        monte_carlo_data_df = pd.DataFrame(monte_carlo_data_df, columns = monte_carlo_data_columns)
        
        print('Monte_Carlo_Data.csv created')

    except Exception as e:
        print('CSV error:')
        print(e)
        
    # Export data
    monte_carlo_data_df.to_csv(
        'Monte_Carlo_Data.csv', 
        index = False, 
        encoding = 'utf8'
    )

    
if __name__ == '__main__':
    nhl_monte_carlo_csv()
    
    
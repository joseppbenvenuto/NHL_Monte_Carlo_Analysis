import pandas as pd
import numpy as np

from io import StringIO
import psycopg2 as ps
from SQL_Queries_NHL_Games import *

import warnings
warnings.filterwarnings("ignore", category = DeprecationWarning) 

import os
import glob
import shutil
import datetime

###########################################################################################################################################
# NEW CODE BLOCK - Process NHL game data
###########################################################################################################################################

# Process full data
def process_full_data(df, cur):
    '''
    - Process new full NHL game data
    - Process old full NHL game data
    - Add unique ids
    - Create date column
    - Create opponent column
    '''
    # Replace "-" with null values
    df = df.replace("-", 0)
    
    # Create date column and remove excess columns
    df['Date'] = df['Game'].str[:10]
    df['Date'] = pd.to_datetime(df['Date'])

    # Create date column and remove excess columns
    df = df.drop(
        ['Game','Unnamed: 2'],
        axis = 1,
        errors = 'ignore'
    )

    # shift column Date to third position
    column = df.pop('Date')
    # insert column using insert(position, column_name, first_column) function
    df.insert(1, 'Date', column)
    
    # Concat old old data with new
    query = '''
        SELECT 
            t."Team",
            d."Date", 
            f."TOI",
            f."CF", 
            f."CA",
            f."CF%", 
            f."FF", 
            f."FA",
            f."FF%", 
            f."SF", 
            f."SA", 
            f."SF%", 
            f."GF", 
            f."GA",
            f."GF%", 
            f."xGF",
            f."xGA",
            f."xGF%", 
            f."SCF",
            f."SCA", 
            f."SCF%",
            f."HDCF",
            f."HDCA",
            f."HDCF%", 
            f."HDSF",
            f."HDSA", 
            f."HDSF%", 
            f."HDGF", 
            f."HDGA", 
            f."HDGF%",
            f."HDSH%",
            f."HDSV%",
            f."MDCF",
            f."MDCA", 
            f."MDCF%",
            f."MDSF", 
            f."MDSA", 
            f."MDSF%", 
            f."MDGF",
            f."MDGA",
            f."MDGF%", 
            f."MDSH%",
            f."MDSV%", 
            f."LDCF", 
            f."LDCA", 
            f."LDCF%", 
            f."LDSF",
            f."LDSA",
            f."LDSF%",
            f."LDGF", 
            f."LDGA",
            f."LDGF%",
            f."LDSH%",
            f."LDSV%",
            f."SH%",
            f."SV%", 
            f."PDO",
            f."Attendance"
        FROM game_facts AS f LEFT JOIN teams AS t
        ON f."Team_ID" = t."Team_ID"
        LEFT JOIN dates AS d
        ON f."Date_ID" = d."Date_ID";
    '''
    
    try:
        cur.execute(query)
        df_old = cur.fetchall()
        
        df_old_columns = [
            'Team_ID',
            'Date_ID',  
            'TOI',
            'CF', 
            'CA',
            'CF%', 
            'FF', 
            'FA',
            'FF%', 
            'SF', 
            'SA', 
            'SF%', 
            'GF', 
            'GA',
            'GF%', 
            'xGF',
            'xGA',
            'xGF%', 
            'SCF',
            'SCA', 
            'SCF%',
            'HDCF',
            'HDCA',
            'HDCF%', 
            'HDSF',
            'HDSA', 
            'HDSF%', 
            'HDGF', 
            'HDGA', 
            'HDGF%',
            'HDSH%',
            'HDSV%',
            'MDCF',
            'MDCA', 
            'MDCF%',
            'MDSF', 
            'MDSA', 
            'MDSF%', 
            'MDGF',
            'MDGA',
            'MDGF%', 
            'MDSH%',
            'MDSV%', 
            'LDCF', 
            'LDCA', 
            'LDCF%', 
            'LDSF',
            'LDSA',
            'LDSF%',
            'LDGF', 
            'LDGA',
            'LDGF%',
            'LDSH%',
            'LDSV%',
            'SH%',
            'SV%', 
            'PDO',
            'Attendance'
        ]


        # Convert SQL query to pandas data frame
        df_old = pd.DataFrame(df_old, columns = df_old_columns)
        
    except:
        pass
    
    try:
        df = pd.concat([df, df_old], axis = 0)
        df = df.drop_duplicates(keep = 'first').reset_index(drop = True)
    
    except:
        pass

    # Create ids
    df['Team_ID'] = df.groupby(['Team']).ngroup()
    df['Date_ID'] = df.groupby(['Date']).ngroup()

    
    # Get opponent teams
    ####################################################
    pos1 = 0
    pos2 = 1

    df1 = df.copy()
    df1 = df1[['Team_ID','Team']]

    try:
        for i in range(0, df.shape[0]):
            df1.iloc[pos1,:], df1.iloc[pos2,:] = df[['Team_ID','Team']].iloc[pos2,:], df[['Team_ID','Team']].iloc[pos1,:]
            pos1 += 2
            pos2 += 2

    except:
        pass

    df1 = df1.reset_index(drop = True)
    df1 = df1[['Team_ID','Team']]

    df1 = df1.rename(columns = {
        'Team': 'Opponent', 
        'Team_ID': 'Opponent_ID'
    })
    
    
    # Concat data frames
    ####################################################
    df = pd.concat([df, df1], axis = 1)

    # shift column Date to third position
    column = df.pop('Date')
    # insert column using insert(position, column_name, first_column) function
    df.insert(0, 'Date', column)

    # shift column Team to third position
    column = df.pop('Team')
    # insert column using insert(position, column_name, first_column) function
    df.insert(1, 'Team', column)

    # shift column Opponent to third position
    column = df.pop('Opponent')
    # insert column using insert(position, column_name, first_column) function
    df.insert(2, 'Opponent', column)

    # shift column Date_ID to third position
    column = df.pop('Date_ID')
    # insert column using insert(position, column_name, first_column) function
    df.insert(0, 'Date_ID', column)

    # shift column Team_ID to third position
    column = df.pop('Team_ID')
    # insert column using insert(position, column_name, first_column) function
    df.insert(2, 'Team_ID', column)

    # shift column Opponent_ID to third position
    column = df.pop('Opponent_ID')
    # insert column using insert(position, column_name, first_column) function
    df.insert(4, 'Opponent_ID', column)
    
    # Split data frames and remove duplicates
    # Teams
    teams_df = df[['Team_ID','Team']]
    teams_df = teams_df.drop_duplicates(keep = 'first').reset_index(drop = True)
    
    # Opponents
    opponents_df = df[['Opponent_ID','Opponent']]
    opponents_df = opponents_df.drop_duplicates(keep = 'first').reset_index(drop = True)
    
    # Dates
    dates_df = df[['Date_ID','Date']]
    dates_df = dates_df.drop_duplicates(keep = 'first').reset_index(drop = True)
    
    # Game Facts table
    game_facts_df = df[[
        'Date_ID', 
        'Team_ID', 
        'Opponent_ID', 
        'TOI',
        'CF', 
        'CA',
        'CF%', 
        'FF', 
        'FA',
        'FF%', 
        'SF', 
        'SA', 
        'SF%', 
        'GF', 
        'GA',
        'GF%', 
        'xGF',
        'xGA',
        'xGF%', 
        'SCF',
        'SCA', 
        'SCF%',
        'HDCF',
        'HDCA',
        'HDCF%', 
        'HDSF',
        'HDSA', 
        'HDSF%', 
        'HDGF', 
        'HDGA', 
        'HDGF%',
        'HDSH%',
        'HDSV%',
        'MDCF',
        'MDCA', 
        'MDCF%',
        'MDSF', 
        'MDSA', 
        'MDSF%', 
        'MDGF',
        'MDGA',
        'MDGF%', 
        'MDSH%',
        'MDSV%', 
        'LDCF', 
        'LDCA', 
        'LDCF%', 
        'LDSF',
        'LDSA',
        'LDSF%',
        'LDGF', 
        'LDGA',
        'LDGF%',
        'LDSH%',
        'LDSV%',
        'SH%',
        'SV%', 
        'PDO',
        'Attendance'
    ]] 
    
    return teams_df, opponents_df, dates_df, game_facts_df


######################################################################################################################
# NEW BLOCK - Insert data into nhlgamesdb
######################################################################################################################

# Insert teams data
def insert_teams_data(teams_df, conn, cur):
    '''
    - Inserts data into the teams table in nhlgamesdb
    - This is a bulk import maximizing speed and memory to adjust for the large number of rows
    '''
    # Drop all date in current table
    cur.execute(teams_table_drop_rows)  
    conn.commit()
    
    # Stream cleaned data in bulk to database
    try:
        sio = StringIO()
        
        sio.write(teams_df.to_csv(
            index = None,
            header = None,
            sep = '|'
        ))
        
        sio.seek(0)

        cur.copy_from(
            file = sio,
            table = 'teams',
            columns = teams_df.columns,
            sep = '|'
            )
            
        conn.commit()

        print('Teams data inserted in bulk to nhlgamesdb successfully 1.')
        print(' '.join(['Rows inserted:', str(teams_df.shape[0])]))

    except ps.Error as e:
        print('Insert Teams data error:')
        print(e)
        

# Insert teams data
def insert_opponents_data(opponents_df, conn, cur):
    '''
    - Inserts data into the teams table in nhlgamesdb
    - This is a bulk import maximizing speed and memory to adjust for the large number of rows
    '''
    # Drop all date in current table
    cur.execute(opponents_table_drop_rows)  
    conn.commit()
    
    # Stream cleaned data in bulk to database
    try:
        sio = StringIO()
        
        sio.write(opponents_df.to_csv(
            index = None,
            header = None,
            sep = '|'
        ))
        
        sio.seek(0)

        cur.copy_from(
            file = sio,
            table = 'opponents',
            columns = opponents_df.columns,
            sep = '|'
            )
            
        conn.commit()

        print('Opponents data inserted in bulk to nhlgamesdb successfully 1.')
        print(' '.join(['Rows inserted:', str(opponents_df.shape[0])]))

    except ps.Error as e:
        print('Insert Opponents data error:')
        print(e)
        

# Insert dates data
def insert_dates_data(dates_df, conn, cur):
    '''
    - Inserts data into the dates table in nhlgamesdb
    - This is a bulk import maximizing speed and memory to adjust for the large number of rows
    '''
    # Drop all date in current table
    cur.execute(dates_table_drop_rows)  
    conn.commit()
    
    # Stream cleaned data in bulk to database
    try:
        sio = StringIO()
        
        sio.write(dates_df.to_csv(
            index = None,
            header = None,
            sep = '|'
        ))
        
        sio.seek(0)

        cur.copy_from(
            file = sio,
            table = 'dates',
            columns = dates_df.columns,
            sep = '|'
            )
            
        conn.commit()

        print('Dates data inserted in bulk to nhlgamesdb successfully 1.')
        print(' '.join(['Rows inserted:', str(dates_df.shape[0])]))

    except ps.Error as e:
        print('Insert Dates data error:')
        print(e)
        

# Insert game facts data
def insert_game_facts_data(game_facts_df, conn, cur):
    '''
    - Inserts data into the game_facts table in nhlgamesdb
    - This is a bulk import maximizing speed and memory to adjust for the large number of rows
    '''
    # Drop all date in current table
    cur.execute(game_facts_table_drop_rows)  
    conn.commit()
    
    # Stream cleaned data in bulk to database
    try:
        sio = StringIO()
        
        sio.write(game_facts_df.to_csv(
            index = None,
            header = None,
            sep = '|'
        ))
        
        sio.seek(0)

        cur.copy_from(
            file = sio,
            table = 'game_facts',
            columns = game_facts_df.columns,
            sep = '|'
            )
            
        conn.commit()

        print('Game Facts data inserted in bulk to nhlgamesdb successfully 1.')
        print(' '.join(['Rows inserted:', str(game_facts_df.shape[0])]))

    except ps.Error as e:
        print('Insert Game Facts data error:')
        print(e)
   
    
######################################################################################################################
# NEW BLOCK - Run etl pipeline
######################################################################################################################

# Runs etl pipeline
def etl():
    '''
    - Runs ETL pipeline
    - Transforms data
    - Inserts data into propper nhlgamesdb tables
    - Relocates processed CSV to storage folder
    '''
    # Import full data from https://www.naturalstattrick.com/
    ######################################################################### 
    try:
        print('Pulling CSV')
        print('Loading...')
        # Get path
        import_path = r'C:\**\NHL_Monte_Carlo_Analysis\NHL_Game_ETL_Pipeline\Data'
        import_path = glob.glob(import_path, recursive = True)
        import_path = import_path[0]

        nhl_games = []
        for i in os.listdir(import_path):
            if os.path.isfile(os.path.join(import_path,i)) and 'Natural Stat TrickTeam Season Totals' in i:
                nhl_games.append(i)

        df = pd.read_csv(import_path + '//' + nhl_games[0])
        
    except:
        print('No data found to process')
        pass    
    
    
    # Connect to nhlgamesdb
    #########################################################################
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


    # Preprocess and transforms data
    #########################################################################
    teams_df, opponents_df, dates_df, game_facts_df = process_full_data(df = df, cur = cur)


    # Insert data to nhlgamesdb
    #########################################################################
    # Insert team data
    insert_teams_data(
        teams_df = teams_df, 
        conn = conn, 
        cur = cur
    )

    # Insert opponents data
    insert_opponents_data(
        opponents_df = opponents_df, 
        conn = conn, 
        cur = cur
    )

    # insert dates_df data
    insert_dates_data(
        dates_df = dates_df, 
        conn = conn, 
        cur = cur
    )

    # Insert financial statements data
    insert_game_facts_data(
        game_facts_df = game_facts_df, 
        conn = conn, 
        cur = cur
    )


    # Create export directory for all combined files
    #########################################################################
    try:
        # Get path
        print('Loading...')
        import_path = r'C:\**\NHL_Monte_Carlo_Analysis\NHL_Game_ETL_Pipeline\Data'
        import_path = glob.glob(import_path, recursive = True)
        import_path = import_path[0]
        
        # Get date
        date = datetime.datetime.now().strftime("%Y-%m-%d")

        try:
            # Create folder with date as name
            os.makedirs(import_path + '\\' + date)
            
        except:
            pass
      
        # Get all files from import path that are CSV
        allfiles = os.listdir(import_path + '\\')
        allfiles = [file_name for file_name in allfiles if file_name.endswith('.csv')]

        for file in allfiles:
            shutil.move(import_path + '\\' + file, import_path + '\\' + date + '\\' + file)
            print(' '.join(['Stored data', str(file), 'to folder', str(date)]))
    
    except:
        pass
    
    
    print('Program complete')
    
    
if __name__ == '__main__':
    etl()


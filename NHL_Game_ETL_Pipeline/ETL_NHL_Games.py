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

    # Get real scores
    ####################################################
    df1 = df['Game'].str.split(', ', expand = True)
    df1 = df1.drop_duplicates(keep = 'first')
    df1[0] = df1[0].str[-1:]
    df1[1] = df1[1].str[-1:]
    df1 = df1.stack().reset_index(level = [0,1], drop = True)

    df1 = pd.DataFrame(df1)
    df1.columns = ['Opponent_Real_Score']

    df = pd.concat([df, df1], axis = 1)

    # Get opponent real scores
    ####################################################
    pos1 = 0
    pos2 = 1

    df1 = df.copy()
    df1 = df1[['Opponent_Real_Score']]

    try:
        for i in range(0, df.shape[0]):
            df1.iloc[pos1,:], df1.iloc[pos2,:] = df[['Opponent_Real_Score']].iloc[pos2,:], df[['Opponent_Real_Score']].iloc[pos1,:]
            pos1 += 2
            pos2 += 2

    except:
        pass


    df = df.rename(columns = {'Opponent_Real_Score':'Real_Score'})
    df = pd.concat([df, df1], axis = 1)

    # Create date column and remove excess columns
    df = df.drop(
        ['Game','Unnamed: 2'],
        axis = 1,
        errors = 'ignore'
    )

    # shift column Date to first position
    column = df.pop('Date')
    # insert column using insert(position, column_name, first_column) function
    df.insert(1, 'Date', column)
    
    # Rename columns
    df = df.rename(columns = {
        'CF%':'CF_Percentage',
        'FF%':'FF_Percentage',
        'SF%':'SF_Percentage',
        'GF%':'GF_Percentage',
        'xGF%':'xGF_Percentage',
        'SCF%':'SCF_Percentage',
        'HDCF%':'HDCF_Percentage',
        'HDSF%':'HDSF_Percentage',
        'HDGF%':'HDGF_Percentage',
        'HDSH%':'HDSH_Percentage',
        'HDSV%':'HDSV_Percentage',
        'MDCF%':'MDCF_Percentage',
        'MDSF%':'MDSF_Percentage',
        'MDGF%':'MDGF_Percentage',
        'MDSH%':'MDSH_Percentage',
        'MDSV%':'MDSV_Percentage',
        'LDCF%':'LDCF_Percentage',
        'LDSF%':'LDSF_Percentage',
        'LDGF%':'LDGF_Percentage',
        'LDSH%':'LDSH_Percentage',
        'LDSV%':'LDSV_Percentage',
        'SH%':'SH_Percentage',
        'SV%':'SV_Percentage',
    })
    
    
    # Set all columns to lower case
    col_names = []
    
    for col in list(df.columns):
        lower_col = col.lower()
        col_names.append(lower_col)
        
    df.columns = col_names
    

    # Concat old old data with new
    query = '''
        SELECT 
            t.team,
            d.date,
            f.toi,
            f.cf,
            f.ca,
            f.cf_percentage,
            f.ff,
            f.fa,
            f.ff_percentage,
            f.sf,
            f.sa,
            f.sf_percentage,
            f.gf,
            f.ga,
            f.gf_percentage,
            f.real_score,
            f.opponent_real_score,
            f.xgf,
            f.xga,
            f.xgf_percentage,
            f.scf,
            f.sca,
            f.scf_percentage,
            f.hdcf,
            f.hdca,
            f.hdcf_percentage,
            f.hdsf,
            f.hdsa,
            f.hdsf_percentage,
            f.hdgf,
            f.hdga,
            f.hdgf_percentage,
            f.hdsh_percentage,
            f.hdsv_percentage,
            f.mdcf,
            f.mdca,
            f.mdcf_percentage,
            f.mdsf,
            f.mdsa,
            f.mdsf_percentage,
            f.mdgf,
            f.mdga,
            f.mdgf_percentage,
            f.mdsh_percentage,
            f.mdsv_percentage,
            f.ldcf,
            f.ldca,
            f.ldcf_percentage,
            f.ldsf,
            f.ldsa,
            f.ldsf_percentage,
            f.ldgf,
            f.ldga,
            f.ldgf_percentage,
            f.ldsh_percentage,
            f.ldsv_percentage,
            f.sh_percentage,
            f.sv_percentage,
            f.pdo,
            f.attendance
        FROM raw.game_facts AS f LEFT JOIN raw.teams AS t
        ON f.team_id = t.team_id
        LEFT JOIN raw.dates AS d
        ON f.date_id = d.date_id;
    '''

    cur.execute(query)
    df_old = cur.fetchall()

    df_old_columns = [
        'team',
        'date',
        'toi',
        'cf',
        'ca',
        'cf_percentage',
        'ff',
        'fa',
        'ff_percentage',
        'sf',
        'sa',
        'sf_percentage',
        'gf',
        'ga',
        'gf_percentage',
        'real_score',
        'opponent_real_score',
        'xgf',
        'xga',
        'xgf_percentage',
        'scf',
        'sca',
        'scf_percentage',
        'hdcf',
        'hdca',
        'hdcf_percentage',
        'hdsf',
        'hdsa',
        'hdsf_percentage',
        'hdgf',
        'hdga',
        'hdgf_percentage',
        'hdsh_percentage',
        'hdsv_percentage',
        'mdcf',
        'mdca',
        'mdcf_percentage',
        'mdsf',
        'mdsa',
        'mdsf_percentage',
        'mdgf',
        'mdga',
        'mdgf_percentage',
        'mdsh_percentage',
        'mdsv_percentage',
        'ldcf',
        'ldca',
        'ldcf_percentage',
        'ldsf',
        'ldsa',
        'ldsf_percentage',
        'ldgf',
        'ldga',
        'ldgf_percentage',
        'ldsh_percentage',
        'ldsv_percentage',
        'sh_percentage',
        'sv_percentage',
        'pdo',
        'attendance'
    ]

    # Convert SQL query to pandas data frame
    df_old = pd.DataFrame(df_old, columns = df_old_columns)
    df_old['id'] = df_old['team'] + df_old['date']
    df['id'] = df['team'] + df['date']

    df_old = df_old.astype('object')
    df = df.astype('object')

    df = pd.concat([df, df_old], axis = 0)
    df = df.drop_duplicates(subset = ['id'], keep = 'first').reset_index(drop = True)
    
    df = df.drop(
        'id',
        axis = 1,
        errors = 'ignore'
    )

    # Create ids
    df['team_id'] = df.groupby(['team']).ngroup()
    df['date_id'] = df.groupby(['date']).ngroup()

    
    # Get opponent teams
    ####################################################
    pos1 = 0
    pos2 = 1

    df1 = df.copy()
    df1 = df1[['team_id','team']]

    try:
        for i in range(0, df.shape[0]):
            df1.iloc[pos1,:], df1.iloc[pos2,:] = df[['team_id','team']].iloc[pos2,:], df[['team_id','team']].iloc[pos1,:]
            pos1 += 2
            pos2 += 2

    except:
        pass

    df1 = df1.reset_index(drop = True)
    df1 = df1[['team_id','team']]

    df1 = df1.rename(columns = {
        'team': 'opponent', 
        'team_id': 'opponent_id'
    })
    
    
    # Concat data frames
    ####################################################
    df = pd.concat([df, df1], axis = 1)

    # shift column Date to first position
    column = df.pop('date')
    # insert column using insert(position, column_name, first_column) function
    df.insert(0, 'date', column)

    # shift column Team to second position
    column = df.pop('team')
    # insert column using insert(position, column_name, first_column) function
    df.insert(1, 'team', column)

    # shift column Opponent to third position
    column = df.pop('opponent')
    # insert column using insert(position, column_name, first_column) function
    df.insert(2, 'opponent', column)

    # shift column Date_ID to first position
    column = df.pop('date_id')
    # insert column using insert(position, column_name, first_column) function
    df.insert(0, 'date_id', column)

    # shift column Team_ID to third position
    column = df.pop('team_id')
    # insert column using insert(position, column_name, first_column) function
    df.insert(2, 'team_id', column)

    # shift column Opponent_ID to fifth position
    column = df.pop('opponent_id')
    # insert column using insert(position, column_name, first_column) function
    df.insert(4, 'opponent_id', column)

    # shift column Real_Score to twentieth position
    column = df.pop('real_score')
    # insert column using insert(position, column_name, first_column) function
    df.insert(19, 'real_score', column)

    # shift column Opponent_Real_Score to twenty first position
    column = df.pop('opponent_real_score')
    # insert column using insert(position, column_name, first_column) function
    df.insert(20, 'opponent_real_score', column)
    
    # Split data frames and remove duplicates
    # Teams
    teams_df = df[['team_id','team']]
    teams_df = teams_df.drop_duplicates(keep = 'first').reset_index(drop = True)
    
    # Opponents
    opponents_df = df[['opponent_id','opponent']]
    opponents_df = opponents_df.drop_duplicates(keep = 'first').reset_index(drop = True)
    
    # Dates
    dates_df = df[['date_id','date']]
    dates_df = dates_df.drop_duplicates(keep = 'first').reset_index(drop = True)
    
    # Game Facts table
    game_facts_df = df[[
        'date_id',
        'team_id',
        'opponent_id',
        'toi',
        'cf',
        'ca',
        'cf_percentage',
        'ff',
        'fa',
        'ff_percentage',
        'sf',
        'sa',
        'sf_percentage',
        'gf',
        'ga',
        'gf_percentage',
        'real_score',
        'opponent_real_score',
        'xgf',
        'xga',
        'xgf_percentage',
        'scf',
        'sca',
        'scf_percentage',
        'hdcf',
        'hdca',
        'hdcf_percentage',
        'hdsf',
        'hdsa',
        'hdsf_percentage',
        'hdgf',
        'hdga',
        'hdgf_percentage',
        'hdsh_percentage',
        'hdsv_percentage',
        'mdcf',
        'mdca',
        'mdcf_percentage',
        'mdsf',
        'mdsa',
        'mdsf_percentage',
        'mdgf',
        'mdga',
        'mdgf_percentage',
        'mdsh_percentage',
        'mdsv_percentage',
        'ldcf',
        'ldca',
        'ldcf_percentage',
        'ldsf',
        'ldsa',
        'ldsf_percentage',
        'ldgf',
        'ldga',
        'ldgf_percentage',
        'ldsh_percentage',
        'ldsv_percentage',
        'sh_percentage',
        'sv_percentage',
        'pdo',
        'attendance'
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
    # Set path for schema
    cur.execute(f'SET search_path TO raw')
    
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
    # Set path for schema
    cur.execute(f'SET search_path TO raw')
    
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
    # Set path for schema
    cur.execute(f'SET search_path TO raw')
    
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
    
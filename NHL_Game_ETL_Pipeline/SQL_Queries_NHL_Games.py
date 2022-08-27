###########################################################################################################################################
# NEW CODE BLOCK - Drop all tables
###########################################################################################################################################

# DROP TABLES
teams_table_drop = "DROP TABLE IF EXISTS raw.teams;"
opponents_table_drop = "DROP TABLE IF EXISTS raw.opponents;"
dates_table_drop = "DROP TABLE IF EXISTS raw.dates;"
game_facts_table_drop = "DROP TABLE IF EXISTS raw.game_facts;"


###########################################################################################################################################
# NEW CODE BLOCK - Delete rows
###########################################################################################################################################

teams_table_drop_rows = "DELETE FROM raw.teams;"
opponents_table_drop_rows = "DELETE FROM raw.opponents;"
dates_table_drop_rows = "DELETE FROM raw.dates;"
game_facts_table_drop_rows = "DELETE FROM raw.game_facts;"


###########################################################################################################################################
# NEW CODE BLOCK - Create all tables
###########################################################################################################################################

# CREATE TABLES
# DIMENSION TABLES
teams_table_create = ("""
    CREATE TABLE IF NOT EXISTS raw.teams(
        "Team_ID" int NOT NULL PRIMARY KEY,
        "Team" varchar NOT NULL
    );
    
""")

opponents_table_create = ("""
    CREATE TABLE IF NOT EXISTS raw.opponents(
        "Opponent_ID" int NOT NULL PRIMARY KEY,
        "Opponent" varchar NOT NULL
    );
    
""")

dates_table_create = ("""
    CREATE TABLE IF NOT EXISTS raw.dates(
        "Date_ID" int NOT NULL PRIMARY KEY,
        "Date" varchar NOT NULL
    );
    
""")

game_facts_table_create = ("""
    CREATE TABLE IF NOT EXISTS raw.game_facts(
        "Game_Facts_ID" SERIAL NOT NULL PRIMARY KEY,
        "Date_ID" int NOT NULL,
        "Team_ID" int NOT NULL,
        "Opponent_ID" int NOT NULL,
        "TOI" float NOT NULL,
        "CF" float NOT NULL,
        "CA" float NOT NULL,
        "CF%" float NOT NULL,
        "FF" float NOT NULL,
        "FA" float NOT NULL,
        "FF%" float NOT NULL,
        "SF" float NOT NULL,
        "SA" float NOT NULL,
        "SF%" float NOT NULL,
        "GF" float NOT NULL,
        "GA" float NOT NULL,
        "GF%" float NOT NULL,
        "xGF" float NOT NULL,
        "xGA" float NOT NULL,
        "xGF%" float NOT NULL,
        "SCF" float NOT NULL,
        "SCA" float NOT NULL,
        "SCF%" float NOT NULL,
        "HDCF" float NOT NULL,
        "HDCA" float NOT NULL,
        "HDCF%" float NOT NULL,
        "HDSF" float NOT NULL,
        "HDSA" float NOT NULL,
        "HDSF%" float NOT NULL,
        "HDGF" float NOT NULL,
        "HDGA" float NOT NULL,
        "HDGF%" float NOT NULL,
        "HDSH%" float NOT NULL,
        "HDSV%" float NOT NULL,
        "MDCF" float NOT NULL,
        "MDCA" float NOT NULL,
        "MDCF%" float NOT NULL,
        "MDSF" float NOT NULL,
        "MDSA" float NOT NULL,
        "MDSF%" float NOT NULL,
        "MDGF" float NOT NULL,
        "MDGA" float NOT NULL,
        "MDGF%" float NOT NULL,
        "MDSH%" float NOT NULL,
        "MDSV%" float NOT NULL,
        "LDCF" float NOT NULL,
        "LDCA" float NOT NULL,
        "LDCF%" float NOT NULL,
        "LDSF" float NOT NULL,
        "LDSA" float NOT NULL,
        "LDSF%" float NOT NULL,
        "LDGF" float NOT NULL,
        "LDGA" float NOT NULL,
        "LDGF%" float NOT NULL,
        "LDSH%" float NOT NULL,
        "LDSV%" float NOT NULL,
        "SH%" float NOT NULL,
        "SV%" float NOT NULL,
        "PDO" float NOT NULL,
        "Attendance" float NOT NULL
    );
    
""")


###########################################################################################################################################
# NEW CODE BLOCK - Query lists
###########################################################################################################################################

# QUERY LISTS
create_table_queries = [
    teams_table_create, 
    opponents_table_create,
    dates_table_create,
    game_facts_table_create
]

drop_table_queries = [
    teams_table_drop, 
    opponents_table_drop,
    dates_table_drop,
    game_facts_table_drop
]

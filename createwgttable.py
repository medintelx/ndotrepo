import sqlite3
from datetime import datetime

# SQLite database setup
DB_NAME = 'NDOTDATA.db'

# Function to initialize the weightageconfig table in the database
def init_config_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Create weightageconfig table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weightageconfig (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            AnchorWgt REAL NOT NULL,
            NonAnchorWgt REAL NOT NULL,
            MiscWgt REAL NOT NULL,
            AnchorMaxPoints INTEGER NOT NULL,
            NonAnchorMaxPoints INTEGER NOT NULL,
            EpicMinEffortPoints INTEGER NOT NULL,
            createdtime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            modifiedtime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()

# Call the function to initialize the table
init_config_db()
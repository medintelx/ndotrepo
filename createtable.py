import sqlite3

# SQLite database setup
DB_NAME = 'NDOTDATA.db'

# Function to initialize the user table in the database
def init_user_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Create users table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            role TEXT NOT NULL,
            active_status BOOLEAN NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()

# Call the function to initialize the table
#init_user_db()


def init_holidays_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Create holidays table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS holidays (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            holiday_name TEXT NOT NULL,
            holiday_date DATE NOT NULL UNIQUE
        )
    ''')

    conn.commit()
    conn.close()

# Initialize the holidays table
init_holidays_db()
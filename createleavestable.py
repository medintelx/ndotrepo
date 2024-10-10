import sqlite3

# SQLite database setup
DB_NAME = 'NDOTDATA.db'

# Function to initialize the user table in the database
def init_user_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
     # Create leaves table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS leaves (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            leave_from DATE NOT NULL,
            leave_to DATE NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    ''')
      
    conn.commit()
    conn.close()

# Call the function to initialize the table
init_user_db()
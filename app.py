import streamlit as st
import pyodbc

# Database connection details
server = 'prodbase.database.windows.net'
database = 'Analytics'
username = 'pdbadmin@prodbase'
password = 'Sandb123'
driver = '{ODBC Driver 17 for SQL Server}'

# Function to connect to the database and execute the stored procedure
def fetch_db_data():
    try:
        with pyodbc.connect(f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}') as conn:
            with conn.cursor() as cursor:
                cursor.execute("EXEC SP_EPIC_EFFORT")  # Replace with your stored procedure name
                result = cursor.fetchall()
                columns = [column[0] for column in cursor.description]
                data = [dict(zip(columns, row)) for row in result]
                return data
    except Exception as e:
        st.error(f"Error: {e}")
        return None

# Streamlit app
def main():
    st.title("Forecast App")

    menu = ["Home", "DB Data"]
    choice = st.sidebar.selectbox("Menu", menu)

    if choice == "Home":
        st.subheader("Home")
        st.write("Welcome to the Streamlit Azure SQL Database App!")

    elif choice == "DB Data":
        st.subheader("Database Data")
        data = fetch_db_data()
        if data:
            st.write(data)

if __name__ == '__main__':
    main()

import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os
import plotly.graph_objects as go
import pandas as pd
import calendar


# Set the page layout to wide mode
st.set_page_config(layout="wide")

st.markdown("""
    <style>
        .reportview-container {
            margin-top: -2em;
        }
        #MainMenu {visibility: hidden;}
        .stDeployButton {display:none;}
        footer {visibility: hidden;}
        #stDecoration {display:none;}
    </style>
""", unsafe_allow_html=True) 



# Create two columns, left takes 40% of the width and right takes 60%
left, right = st.columns([6, 4], gap="small")

# Custom CSS for full-screen login container
login_css = """
    <style>

    [data-testid="stForm"] {
    border : 0
    }
 /* Style the left column with blue background matching the neveda.png image */
    [data-testid="stHorizontalBlock"] > div:first-child {
        background-color: #0253A4 !important;  /* Replace this with the exact blue color code from the image */
        padding: 0px !important;  /* Add padding inside the column */
        height: 100vh;  /* Full viewport height */
        display: flex;  /* Use flexbox to center content */
        justify-content: center;
        align-items: center;
        box-sizing: content-box !important;
        gap: 0;
    }
     [data-testid="stHorizontalBlock"] > div:third  -child {
        vertical-align: middle;
        pa
    }

    /* Right column style for login form */
    .login-container {
        vertical-align: middle;
        background-color: white;
        border: 0;
        padding: 0px;
        border-radius: 10px;
        box-shadow: 0px 4px 6px rgba(0, 0, 0, 0.1);
        text-align: center;
        width: 100%; /* Take full width */
        margin: 0; /* Remove any margin */
        justify-content: center;
        align-items: center;
    }

    /* Custom styling for login button and input */
    .login-btn, .login-input {
        width: 100%;
        padding: 10px;
        border-radius: 10px;
        border: 1px solid #ccc;
        margin-bottom: 10px;
        font-size: 16px;
    }

    /* Login button styling */
    .login-btn {
        background-color: #0253A4;  /* Blue color */
        color: white;  /* White text */
        border: none;
        cursor: pointer;
        text-align: center;
    }

    .login-btn:hover {
        background-color: #187bcd;  /* Darker blue on hover */
    }

    /* Align the text */
    h1 {
        color: black;
        font-family: 'Arial', sans-serif;
    }

    h2 {
        color: gray;
        font-family: 'Arial', sans-serif;
        font-weight: 300;
    }
    /* Remove padding and margin from the main container */
    [data-testid="stAppViewContainer"] {
        padding: 0 !important;  /* Remove padding from the app container */
        margin: 0 !important;   /* Remove margin from the app container */
    }
    </style>
"""

# Inject custom CSS
st.markdown(login_css, unsafe_allow_html=True)

# Custom CSS to change the sidebar background color
sidebar_style = """
    <style>
    [data-testid="stSidebar"] {
        background-color: #1560BD  /* Blue color */
    }
      /* Change main content area background color to ash */
    [data-testid="stAppViewContainer"] {
        background-color: #F5F5F5;  /* Ash color */
    }
    </style>
"""
# Injecting the custom CSS into the Streamlit app
st.markdown(sidebar_style, unsafe_allow_html=True)
 
# Load environment variables from a .env file
load_dotenv()

DB_NAME = os.getenv('DB_NAME')

# Path to the text file containing usernames (update with actual path)
USER_FILE = 'usernames.txt'

# Function to check if the username exists in the file
def check_username(username):
    if os.path.exists(USER_FILE):
        with open(USER_FILE, 'r') as file:
            valid_usernames = [line.strip() for line in file.readlines()]
            return username in valid_usernames
    else:
        return False


# # Main function for the login screen
# def login_screen():
#     st.title("Login")

#     # Input field for the username
#     username = st.text_input("Enter your username")

#     # Button for logging in
#     if st.button("Login"):
#         if check_username(username):
#             st.success(f"Welcome, {username}!")
#             st.session_state['logged_in'] = True  # Set session state to indicate user is logged in
#         else:
#             st.error("Invalid username. Please try again.")


# Main function for the login screen
def login_screen():
   # Left column for the logo
    with left:
        st.image("neveda.png", width=300)

    
    # Right column for the login form
    with right:
        st.markdown(
            """
            <div class="login-container">
                <h4>Hello Again!</h4>
                <h5>Welcome Back</h5>
            </div>
            """, unsafe_allow_html=True
        )
        
        # Form for username input and login button
        with st.form(key="login_form"):
            username = st.text_input("Username", key="username", placeholder="Enter your username", label_visibility="collapsed")
            login_button = st.form_submit_button("Login")


        
        # Button for logging in
        if login_button:
            if check_username(username):
                st.success(f"Welcome, {username}!")
                st.session_state['logged_in'] = True  # Set session state to indicate user is logged in
            else:
                st.error("Invalid username. Please try again.")


# Function to initialize the user table in the database
def init_user_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Create users table if it doesn't exist
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

# Call the function to initialize the user table
init_user_db()

# Function to add a new user to the database
def add_user_to_db(name, email, role, active_status):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO users (name, email, role, active_status)
            VALUES (?, ?, ?, ?)
        ''', (name, email, role, active_status))
        conn.commit()
        st.success("User added successfully!")
    except sqlite3.IntegrityError:
        st.error("This email is already registered. Please use a different email.")
    
    conn.close()

def add_leave_to_db(user_id, leave_from, leave_to):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    print(user_id)
    cursor.execute('''
        INSERT INTO leaves (user_id, leave_from, leave_to)
        VALUES (?, ?, ?)
    ''', (int(user_id), leave_from, leave_to))
    
    conn.commit()
    conn.close()
    st.success(f"Leave recorded for User ID {user_id} from {leave_from} to {leave_to}.")

# Function to fetch all leaves from the database and handle user IDs correctly
def fetch_leaves_from_db():
    conn = sqlite3.connect(DB_NAME)
    leaves_df = pd.read_sql('''
        SELECT l.id, u.name, l.leave_from, l.leave_to 
        FROM leaves l 
        JOIN users u ON l.user_id = u.id
    ''', conn)
    conn.close()
    return leaves_df

# Function to fetch all users from the database
def fetch_users_from_db():
    conn = sqlite3.connect(DB_NAME)
    users_df = pd.read_sql("SELECT id, name FROM users", conn)
    conn.close()
    return users_df

def add_or_update_holiday(holiday_name, holiday_date):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    try:
        cursor.execute('''
            INSERT OR REPLACE INTO holidays (holiday_name, holiday_date)
            VALUES (?, ?)
        ''', (holiday_name, holiday_date))

        conn.commit()
        st.success(f"Holiday '{holiday_name}' on {holiday_date} has been added/updated.")
    except sqlite3.IntegrityError:
        st.error("Holiday date conflict. Please choose a different date.")

    conn.close()

def fetch_holidays_from_db():
    conn = sqlite3.connect(DB_NAME)
    holidays_df = pd.read_sql("SELECT * FROM holidays", conn)
    conn.close()
    return holidays_df

# Function to delete a user from the database
def delete_user_from_db(user_id):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute('DELETE FROM users WHERE id = ?', (user_id,))
    conn.commit()
    conn.close()
    st.success(f"User with ID {user_id} has been deleted.")


# Function to add a new configuration to the database
def add_config_to_db(AnchorWgt, NonAnchorWgt, MiscWgt, AnchorMaxPoints, NonAnchorMaxPoints, EpicMinEffortPoints):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO weightageconfig (AnchorWgt, NonAnchorWgt, MiscWgt, AnchorMaxPoints, NonAnchorMaxPoints, EpicMinEffortPoints, modifiedtime)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (AnchorWgt, NonAnchorWgt, MiscWgt, AnchorMaxPoints, NonAnchorMaxPoints, EpicMinEffortPoints, datetime.now()))
    
    conn.commit()
    conn.close()
    st.success("Configuration saved successfully!")

# Function to fetch data from the database based on work item type
def fetch_data_from_db(work_item_type):
    conn = sqlite3.connect(DB_NAME)
    query = ""
    
    if work_item_type == "Projects":
        query = "SELECT * FROM projects"
    elif work_item_type == "Epics":
        query = "SELECT * FROM epics"
    elif work_item_type == "Features":
        query = "SELECT * FROM features"
    elif work_item_type == "Product Backlog Items":
        query = "SELECT * FROM productbacklogitems"
    
    df = pd.read_sql(query, conn)
    conn.close()
    return df


# Function to read tables into DataFrames and perform the joins
def load_and_join_data():
    conn = sqlite3.connect(DB_NAME)
    
    # SQL query to join productbacklogitems, features, epics, and projects
    # SQL query with all fields manually selected and aliased to avoid conflicts
# SQL query with all fields manually selected and aliased to avoid conflicts
    query = """
    SELECT 
    -- Fields from projects table
    pr.Work_Item_ID AS project_Work_Item_ID, 
    pr.Area_ID AS project_Area_ID,
    pr.Area_Path AS project_Area_Path,
    pr.Team_Project AS project_Team_Project,
    pr.Node_Name AS project_Node_Name,
    pr.Area_Level_1 AS project_Area_Level_1,
    pr.Revision AS project_Revision,
    pr.Authorized_Date AS project_Authorized_Date,
    pr.Revised_Date AS project_Revised_Date,
    pr.Iteration_ID AS project_Iteration_ID,
    pr.Iteration_Path AS project_Iteration_Path,
    pr.Iteration_Level_1 AS project_Iteration_Level_1,
    pr.Work_Item_Type AS project_Work_Item_Type,
    pr.State AS project_State,
    pr.Reason_for_State_Change AS project_Reason_for_State_Change,
    pr.Assigned_To AS project_Assigned_To,
    pr.Person_ID AS project_Person_ID,
    pr.Watermark AS project_Watermark,
    pr.Comment_Count AS project_Comment_Count,
    pr.Title AS project_Title,
    pr.Board_Column AS project_Board_Column,
    pr.Is_Board_Column_Done AS project_Is_Board_Column_Done,
    pr.State_Change_Date AS project_State_Change_Date,
    pr.Business_Value AS project_Business_Value,
    pr.Backlog_Priority AS project_Backlog_Priority,
    pr.Health AS project_Health,
    pr.Scoping_30_Percent AS project_Scoping_30_Percent,
    pr.SeventyFivePercentComplete AS project_SeventyFivePercentComplete,
    pr.Intermediate_Date AS project_Intermediate_Date,
    pr.QAQC_Submittal_Date AS project_QAQC_Submittal_Date,
    pr.Document_Submittal_Date AS project_Document_Submittal_Date,
    pr.Extension_Marker AS project_Extension_Marker,
    pr.Kanban_Column AS project_Kanban_Column,
    pr.Kanban_Column_Done AS project_Kanban_Column_Done,
    pr.EA_Number AS project_EA_Number,
    pr.Priority_Traffic_Ops AS project_Priority_Traffic_Ops,
    pr.Fiscal_Year AS project_Fiscal_Year,
    pr.Funding_Source AS project_Funding_Source,
    pr.Route_Type AS project_Route_Type,
    pr.Construction_EA_Number AS project_Construction_EA_Number,
    pr.Official_DOC_Date AS project_Official_DOC_Date,
    pr.Official_Advertise_Date AS project_Official_Advertise_Date,
    pr.Anchor_Project AS project_Anchor_Project,
    pr.Complexity_Signals AS project_Complexity_Signals,
    pr.Complexity_Lighting AS project_Complexity_Lighting,
    pr.Complexity_ITS AS project_Complexity_ITS,
    pr.Complexity_Power_Design AS project_Complexity_Power_Design,
    pr.Complexity_RoW_Coordination AS project_Complexity_RoW_Coordination,
    pr.Complexity_SLI_Project_Lead AS project_Complexity_SLI_Project_Lead,
    pr.Complexity_Solar_Design AS project_Complexity_Solar_Design,
    pr.Complexity_Trunkline AS project_Complexity_Trunkline,

    -- Fields from epics table
    e.System_Id AS epic_System_Id, 
    e.System_Title AS epic_Title, 
    e.System_AreaPath AS epic_AreaPath,
    e.System_TeamProject AS epic_TeamProject,
    e.System_IterationPath AS epic_IterationPath,
    e.System_WorkItemType AS epic_Work_ItemType,
    e.System_State AS epic_State,
    e.System_Reason AS epic_Reason,
    e.System_CreatedDate AS epic_CreatedDate,
    e.System_ChangedDate AS epic_ChangedDate,

    -- Fields from features table
    f.system_Id AS feature_System_Id, 
    f.System_Title AS feature_Title, 
    f.System_AreaPath AS feature_AreaPath,
    f.System_TeamProject AS feature_TeamProject,
    f.System_IterationPath AS feature_IterationPath,
    f.System_WorkItemType AS feature_WorkItemType,
    f.System_State AS feature_State,
    f.System_Reason AS feature_Reason,
    f.System_CreatedDate AS feature_CreatedDate,
    f.System_ChangedDate AS feature_ChangedDate,

    -- Fields from productbacklogitems table
    pbi.System_Id AS pbi_System_Id, 
    pbi.System_Parent AS pbi_System_Parent, 
    pbi.System_Title AS pbi_Title, 
    pbi.System_AreaPath AS pbi_AreaPath,
    pbi.System_TeamProject AS pbi_TeamProject,
    pbi.System_IterationPath AS pbi_IterationPath,
    pbi.System_WorkItemType AS pbi_WorkItemType,
    pbi.System_State AS pbi_State,
    pbi.System_Reason AS pbi_Reason,
    pbi.System_CreatedDate AS pbi_CreatedDate,
    pbi.System_ChangedDate AS pbi_ChangedDate,
    pbi.Microsoft_VSTS_Common_Priority AS pbi_Priority,
    pbi.Microsoft_VSTS_Scheduling_Effort AS pbi_Effort

FROM projects pr
LEFT JOIN epics e ON e.System_Parent = pr.Work_Item_ID
LEFT JOIN features f ON f.System_Parent = e.System_Id
LEFT JOIN productbacklogitems pbi ON pbi.System_Parent = f.system_Id;
"""

    # Execute the query and load the result into a DataFrame
    joined_df = pd.read_sql(query, conn)

    
    return joined_df

# Function to display a styled calendar for a specific month and year
def display_styled_calendar(month, year, holidays_df):
    cal = calendar.HTMLCalendar()

    # Convert the holiday dates to list of datetime.date
    holiday_dates = pd.to_datetime(holidays_df['holiday_date']).dt.date.tolist()

    # Generate the calendar HTML for the specified month and year
    month_calendar = cal.formatmonth(year, month)

    # Highlight holidays by modifying the HTML of the calendar
    for holiday in holiday_dates:
        if holiday.year == year and holiday.month == month:
            # Highlight the holiday date
            month_calendar = month_calendar.replace(f">{holiday.day}<", f' style="background-color: #FFDDC1;">{holiday.day}<')  # Orange for holidays

    # Highlight weekends (Saturday and Sunday)
    for day in range(1, 32):
        try:
            date = datetime(year, month, day)
            if date.weekday() == 5 or date.weekday() == 6:  # 5=Saturday, 6=Sunday
                month_calendar = month_calendar.replace(f">{day}<", f' style="background-color: #D3F8E2;">{day}<')  # Light green for weekends
        except ValueError:
            pass  # Ignore invalid dates

    # Display the styled calendar using Streamlit's Markdown component
    st.markdown(month_calendar, unsafe_allow_html=True)

# Initialize year and month to the current date
today = datetime.now()
current_year = today.year
current_month = today.month

# Main application after login
def main_application():
    # Top Menu using buttons for navigation
    col1, col2, col3, col4, col5, col6 = st.columns([1, 1, 1, 1, 1, 1])

    with col1:
        if st.button("Work Items"):
            st.session_state.menu = "Work Items"
    with col2:
        if st.button("Users"):
            st.session_state.menu = "Users"
    with col3:
        if st.button("Config"):
            st.session_state.menu = "Config"
    with col4:
        if st.button("Leaves"):
            st.session_state.menu = "Leaves"
    with col5:
        if st.button("Holidays"):
            st.session_state.menu = "Holidays"
    with col6:
        if st.button("Forecast"):
            st.session_state.menu = "Forecast"

    # Show selected menu content
    if st.session_state.get("menu") == "Work Items":
        st.title("Azure DevOps Work Items")
        work_item_type = st.selectbox("Select Work Item Type", ["Projects", "Epics", "Features", "Product Backlog Items"])
        if work_item_type:
            data = fetch_data_from_db(work_item_type)
            st.write(f"Displaying {work_item_type} data:")
            st.dataframe(data)

    elif st.session_state.get("menu") == "Users":
        st.title("User Registration")
        with st.form(key='user_form'):
            name = st.text_input("Name", max_chars=50)
            email = st.text_input("Email", max_chars=100)
            role = st.selectbox("Role", ["Admin", "Viewer", "Editor"])
            active_status = st.checkbox("Active Status", value=True)
            submit_button = st.form_submit_button(label="Create User")
            if submit_button:
                add_user_to_db(name, email, role, active_status)
        users_df = fetch_users_from_db()
        if not users_df.empty:
            st.dataframe(users_df)
        else:
            st.write("No users found.")

    elif st.session_state.get("menu") == "Config":
        st.title("Weightage Configurations")
        with st.form(key='config_form'):
            AnchorWgt = st.number_input("Anchor Weight", min_value=0.0, format="%.2f")
            NonAnchorWgt = st.number_input("Non-Anchor Weight", min_value=0.0, format="%.2f")
            MiscWgt = st.number_input("Miscellaneous Weight", min_value=0.0, format="%.2f")
            AnchorMaxPoints = st.number_input("Anchor Max Points", min_value=0)
            NonAnchorMaxPoints = st.number_input("Non-Anchor Max Points", min_value=0)
            EpicMinEffortPoints = st.number_input("Epic Min Effort Points", min_value=0)
            submit_button = st.form_submit_button(label="Save Configuration")
            if submit_button:
                add_config_to_db(AnchorWgt, NonAnchorWgt, MiscWgt, AnchorMaxPoints, NonAnchorMaxPoints, EpicMinEffortPoints)

    elif st.session_state.get("menu") == "Leaves":
        st.title("Leave Management")
        tab1, tab2 = st.tabs(["Add Leave", "Total Leaves"])
        with tab1:
            st.write("### Add Leave")
            users_df = fetch_users_from_db()
            if not users_df.empty:
                with st.form(key='leave_form'):
                    user = st.selectbox("Select User", users_df['name'])
                    leave_from = st.date_input("Leave From")
                    leave_to = st.date_input("Leave To")
                    submit_button = st.form_submit_button(label="Submit Leave")
                    if submit_button:
                        user_id = users_df[users_df['name'] == user]['id'].values[0]
                        add_leave_to_db(user_id, leave_from, leave_to)
                        st.experimental_rerun()
            else:
                st.write("No users available.")
        with tab2:
            st.write("### Total Leaves")
            leaves_df = fetch_leaves_from_db()
            if not leaves_df.empty:
                st.dataframe(leaves_df)
            else:
                st.write("No leave records found.")

    elif st.session_state.get("menu") == "Holidays":
        st.title("Holiday Management")
        tab1, tab2, tab3 = st.tabs(["Calendar", "Add/Modify Holiday", "Holiday List"])
        with tab1:
            st.write("### Holiday Calendar")
            display_styled_calendar(st.session_state.month, st.session_state.year, fetch_holidays_from_db())
        with tab2:
            st.write("### Add or Modify Holidays")
            with st.form(key='holiday_form'):
                holiday_name = st.text_input("Holiday Name", max_chars=100)
                holiday_date = st.date_input("Holiday Date")
                submit_button = st.form_submit_button(label="Add/Update Holiday")
                if submit_button:
                    add_or_update_holiday(holiday_name, holiday_date)
                    st.experimental_rerun()
        with tab3:
            st.write("### List of All Holidays")
            holidays_df = fetch_holidays_from_db()
            if not holidays_df.empty:
                st.dataframe(holidays_df)
            else:
                st.write("No holidays found.")

    elif st.session_state.get("menu") == "Forecast":
        st.title("Forecast")
        joined_df = load_and_join_data()
        complexity_columns = [
            'project_Complexity_Signals', 'project_Complexity_Lighting',
            'project_Complexity_ITS', 'project_Complexity_Power_Design',
            'project_Complexity_RoW_Coordination', 'project_Complexity_SLI_Project_Lead',
            'project_Complexity_Solar_Design', 'project_Complexity_Trunkline'
        ]
        joined_df[complexity_columns] = joined_df[complexity_columns].fillna(0)
        joined_df['project_Complexity'] = joined_df[complexity_columns].sum(axis=1)
        filtered_df = joined_df[joined_df['project_State'].isin(['Actively Working', 'Approved', 'On-Hold'])]
        st.dataframe(filtered_df)

# Check if the user is logged in
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False

# Display login screen or main application
if not st.session_state['logged_in']:
    login_screen()
else:
    main_application()
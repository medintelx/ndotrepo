import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os
import plotly.graph_objects as go
import pandas as pd
import calendar

# st.markdown("""
#     <style>
#         .reportview-container {
#             margin-top: -2em;
#         }
#         #MainMenu {visibility: hidden;}
#         .stDeployButton {display:none;}
#         footer {visibility: hidden;}
#         #stDecoration {display:none;}
#         .stAppHeader {visibility: hidden;}
#     </style>
# """, unsafe_allow_html=True)

login_css= """
<style>
.appview-container {
background-color: #0253A4 !important; 
text-color:white;
}
</style>
"""

# Custom CSS to change the sidebar background color
sidebar_style = """
    <style>
    [data-testid="stSidebar"] {
        background-color: #0253A4  /* Blue color */
      /* Change main content area background color to ash */
    [data-testid="stAppViewContainer"] {
        background-color: white;  /* white color */
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


# Main function for the login screen
def login_screen():

    st.markdown(login_css, unsafe_allow_html=True)
    st.image("neveda.png", width=200)
      # Custom CSS to style the title
    st.markdown(
        """
        <h3 style='text-align: left; color: white;'>Login</h3>
        """, 
        unsafe_allow_html=True
    )

    # Inject custom CSS for text input styling
    st.markdown("""
    <style>
    .st-key-logintext  p {
    color: white;
    }
    </style>
    """, unsafe_allow_html=True)
    # Input field for the username
    username = st.text_input("Enter your username", key="logintext")
 
    # Button for logging in
    if st.button("Login"):
        if check_username(username):
            st.session_state['logged_in'] = True  # Set session state to indicate user is logged in
            st.rerun()  # Force the app to rerun immediately to reflect the state change
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
   
    # Initialize session state to handle page navigation
    if 'page' not in st.session_state:
        st.session_state.page = 'Home'

    # Sidebar styling
    st.sidebar.markdown(
        """
        <style>
         /* Adjust the sidebar width */
        [data-testid="stSidebar"] {
            background-color: #ADD8E6;
            min-width: 140px;  /* Set the minimum width for the sidebar */
            max-width: 140px;  /* Set the maximum width for the sidebar */
        }
        [data-testid="stButton"] {
            background-color: #ADD8E6;
            border-size: none;
            min-width: 130px;  /* Set the minimum width for the sidebar */
            max-width: 130px;  /* Set the maximum width for the sidebar */
        }
        [data-testid="stButton"]  button {
        background-color: #ADD8E6;
         border: none;
        }
        .menu-button {
            background-color: #0253A4;
            color: white;
            text-align: left;
            padding: 10px;
            margin: 5px 0;
            width: 100%;
            font-size: 16px;
            border-radius: 10px;
            font-weight: bold;
        }
        .menu-button:hover {
            background-color: #023C6D;
            color: white;
            cursor: pointer;
        }
        </style>
        """, unsafe_allow_html=True
    )

    # Sidebar with interactive buttons
    if st.sidebar.button('🏠 Home', key='home'):
        st.session_state.page = 'Home'
    if st.sidebar.button('📊 Dashboard', key='dashboard'):
        st.session_state.page = 'Dashboard'
    if st.sidebar.button('📅 Leaves', key='leaves'):
        st.session_state.page = 'Leaves'
    if st.sidebar.button('👤 Users', key='users'):
        st.session_state.page = 'Users'
    if st.sidebar.button('🏦 Account', key='account'):
        st.session_state.page = 'Account'
    if st.sidebar.button('⚙️ Settings', key='settings'):
        st.session_state.page = 'Settings'

    # Based on the selected page, display the corresponding content
    if st.session_state.page == 'Home':
        st.title("Home Page")
        st.write("Welcome to the Home Page!")

    elif st.session_state.page == 'Dashboard':
        st.title("Dashboard")
        st.write("Welcome to the Dashboard!")

    elif st.session_state.page == 'Leaves':

#         st.markdown("""
#     <style>
#     /* Main Container */
#                     /* Full-width section */
#     .leave-section {
#         width: 100%;  /* Make the section take full width */
#         background-color: transparent;  /* Remove the background color */
#         padding: 0;  /* Remove padding */
#         box-shadow: none;  /* Remove box shadow */
#         margin: 0;  /* Remove margin */
#         border-radius: 0;  /* Remove border radius */
#     }
#     .leave-section {
#         background-color: #f4f7f9;
#         border-radius: 10px;
#         padding: 20px;
#         box-shadow: 0px 4px 10px rgba(0, 0, 0, 0.1);
#         margin: 20px 0;
#     }

#     /* Form Elements */
#     .leave-section input[type="text"], .leave-section input[type="date"], .leave-section select {
#         width: 100%;
#         padding: 12px;
#         margin: 8px 0;
#         display: inline-block;
#         border: 1px solid #ccc;
#         border-radius: 4px;
#         box-sizing: border-box;
#         font-size: 16px;
#         background-color: #fff;
#         color: #333;
#     }

#     /* Form Labels */
#     .leave-section label {
#         font-weight: bold;
#         margin-bottom: 5px;
#         display: block;
#         color: #0253A4;
#     }

#     /* Submit Button */
#     .leave-section .submit-btn {
#         width: 100%;
#         background-color: #0253A4;
#         color: white;
#         padding: 14px 20px;
#         margin-top: 10px;
#         border: none;
#         border-radius: 4px;
#         cursor: pointer;
#         font-size: 18px;
#         font-weight: bold;
#         transition: background-color 0.3s ease;
#     }

#     .leave-section .submit-btn:hover {
#         background-color: #023C6D;
#     }

#     /* Titles and Headings */
#     .leave-section h2 {
#         font-size: 24px;
#         font-weight: bold;
#         color: #0253A4;
#         margin-bottom: 20px;
#     }

#     .leave-section h3 {
#         font-size: 20px;
#         color: #333;
#         margin-bottom: 10px;
#     }

#     /* Table Styling */
#     .leave-section table {
#         width: 100%;
#         border-collapse: collapse;
#         margin-top: 20px;
#     }

#     .leave-section table, th, td {
#         border: 1px solid #ccc;
#         padding: 12px;
#         text-align: left;
#     }

#     .leave-section table th {
#         background-color: #0253A4;
#         color: white;
#         font-weight: bold;
#     }

#     .leave-section table tr:nth-child(even) {
#         background-color: #f9f9f9;
#     }

#     .leave-section table tr:hover {
#         background-color: #f1f1f1;
#     }

#     </style>
# """, unsafe_allow_html=True)
        # st.markdown('<div class="leave-section">', unsafe_allow_html=True)
        st.title("Leave Management")

        # Create tabs for the Leaves menu
        tab1, tab2 = st.tabs(["Add Leave", "Total Leaves"])

        # Tab 1: Add Leave
        with tab1:
            st.write("### Add Leave")

            # Fetch all users from the database
            users_df = fetch_users_from_db()

            if not users_df.empty:
                # Form for adding a leave
                with st.form(key='leave_form'):
                    user = st.selectbox("Select User", users_df['name'])
                    leave_from = st.date_input("Leave From")
                    leave_to = st.date_input("Leave To")

                    submit_button = st.form_submit_button(label="Submit Leave")

                    if submit_button:
                        # Get the selected user's ID
                        user_id = users_df[users_df['name'] == user]['id'].values[0]
                    
                        # Add the leave to the database
                        add_leave_to_db(user_id, leave_from, leave_to)
                        st.rerun()

            else:
                st.write("No users available.")

        # Tab 2: Total Leaves
        with tab2:
            st.write("### Total Leaves")

            # Fetch and display all leave records
            leaves_df = fetch_leaves_from_db()

            if not leaves_df.empty:
                st.dataframe(leaves_df)
            else:
                st.write("No leave records found.")

    elif st.session_state.page == 'Users':
        st.title("User Management")
        st.write("Manage users here.")

    elif st.session_state.page == 'Account':
        st.title("Account Management")
        st.write("Manage your account settings.")

    elif st.session_state.page == 'Settings':
        st.title("Settings")
        st.write("Manage application settings.")

        
    #menu = st.sidebar.selectbox("Menu", ["Work Items", "Users", "Config", "Leaves", "Holidays", "Forecast"])

    # if st.session_state.page == 'Leaves':
    #     st.title("Azure DevOps Work Items")
        
    #     # Sidebar dropdown to select work item type
    #     work_item_type = st.sidebar.selectbox(
    #         "Select Work Item Type", 
    #         ["Projects", "Epics", "Features", "Product Backlog Items"]
    #     )

    #     # Fetch and display the corresponding data
    #     if work_item_type:
    #         data = fetch_data_from_db(work_item_type)
    #         st.write(f"Displaying {work_item_type} data:")
    #         st.dataframe(data)

    if st.session_state.page == "Leaves":
        st.title("User Registration")
        
        # Create a form for user creation
        with st.form(key='user_form'):
            name = st.text_input("Name", max_chars=50)
            email = st.text_input("Email", max_chars=100)
            role = st.selectbox("Role", ["Admin", "Viewer", "Editor"])
            active_status = st.checkbox("Active Status", value=True)
            
            # Submit button
            submit_button = st.form_submit_button(label="Create User")
            
            if submit_button:
                add_user_to_db(name, email, role, active_status)
        
        # Display existing users
        conn = sqlite3.connect(DB_NAME)
        users = pd.read_sql("SELECT * FROM users", conn)
        conn.close()
        
        st.subheader("Existing Users")
        
        if not users.empty:
            for index, row in users.iterrows():
                col1, col2, col3, col4, col5 = st.columns([2, 3, 3, 2, 1])
                
                # Display the user information
                col1.write(row["id"])
                col2.write(row["name"])
                col3.write(row["email"])
                col4.write(row["role"])
                
                delete_button = col5.button("Del", key=row["id"])
                if delete_button:
                    delete_user_from_db(row["id"])
                    st.experimental_rerun()  # Refresh the app after deletion

        else:
            st.write("No users found.")
    # elif menu == "Config":
    #     st.title("Weightage Configurations")

    #     # Create a form for entering weightage configuration
    #     with st.form(key='config_form'):
    #         AnchorWgt = st.number_input("Anchor Weight", min_value=0.0, format="%.2f")
    #         NonAnchorWgt = st.number_input("Non-Anchor Weight", min_value=0.0, format="%.2f")
    #         MiscWgt = st.number_input("Miscellaneous Weight", min_value=0.0, format="%.2f")
    #         AnchorMaxPoints = st.number_input("Anchor Max Points", min_value=0)
    #         NonAnchorMaxPoints = st.number_input("Non-Anchor Max Points", min_value=0)
    #         EpicMinEffortPoints = st.number_input("Epic Min Effort Points", min_value=0)
            
    #         # Submit button
    #         submit_button = st.form_submit_button(label="Save Configuration")
            
    #         if submit_button:
    #             add_config_to_db(AnchorWgt, NonAnchorWgt, MiscWgt, AnchorMaxPoints, NonAnchorMaxPoints, EpicMinEffortPoints)
    # elif menu == "Leaves":
    #     st.title("Leave Management")

    #     # Create tabs for the Leaves menu
    #     tab1, tab2 = st.tabs(["Add Leave", "Total Leaves"])

    #     # Tab 1: Add Leave
    #     with tab1:
    #         st.write("### Add Leave")

    #         # Fetch all users from the database
    #         users_df = fetch_users_from_db()

    #         if not users_df.empty:
    #             # Form for adding a leave
    #             with st.form(key='leave_form'):
    #                 user = st.selectbox("Select User", users_df['name'])
    #                 leave_from = st.date_input("Leave From")
    #                 leave_to = st.date_input("Leave To")

    #                 submit_button = st.form_submit_button(label="Submit Leave")

    #                 if submit_button:
    #                     # Get the selected user's ID
    #                     user_id = users_df[users_df['name'] == user]['id'].values[0]
                    
    #                     # Add the leave to the database
    #                     add_leave_to_db(user_id, leave_from, leave_to)
    #                     st.rerun()

    #         else:
    #             st.write("No users available.")

    #     # Tab 2: Total Leaves
    #     with tab2:
    #         st.write("### Total Leaves")

    #         # Fetch and display all leave records
    #         leaves_df = fetch_leaves_from_db()

    #         if not leaves_df.empty:
    #             st.dataframe(leaves_df)
    #         else:
    #             st.write("No leave records found.")
    # elif menu == "Holidays":
    #     st.title("Holiday Management")

    #     # Fetch all holidays from the database
    #     holidays_df = fetch_holidays_from_db()

    #     # Adding previous and next buttons to navigate between months
    #     if "month" not in st.session_state:
    #         st.session_state.month = current_month
    #     if "year" not in st.session_state:
    #         st.session_state.year = current_year

    #     # Create tabs for the Holidays menu
    #     tab1, tab2, tab3 = st.tabs(["Calendar", "Add/Modify Holiday", "Holiday List"])

    #     # Tab 1: Calendar
    #     with tab1:
    #         st.write("### Holiday Calendar")
    #         # Display navigation buttons for the calendar
    #         col1, col2, col3 = st.columns([1, 2, 1])

    #         with col1:
    #             if st.button("Previous Month"):
    #                 if st.session_state.month == 1:
    #                     st.session_state.month = 12
    #                     st.session_state.year -= 1
    #                 else:
    #                     st.session_state.month -= 1

    #         with col2:
    #             st.write(f"### {calendar.month_name[st.session_state.month]} {st.session_state.year}")

    #         with col3:
    #             if st.button("Next Month"):
    #                 if st.session_state.month == 12:
    #                     st.session_state.month = 1
    #                     st.session_state.year += 1
    #                 else:
    #                     st.session_state.month += 1

    #         # Display the styled calendar for the selected month and year
    #         display_styled_calendar(st.session_state.month, st.session_state.year, holidays_df)

    #     # Tab 2: Add/Modify Holiday
    #     with tab2:
    #         st.write("### Add or Modify Holidays")
    #         # Form for adding/updating holidays
    #         with st.form(key='holiday_form'):
    #             holiday_name = st.text_input("Holiday Name", max_chars=100)
    #             holiday_date = st.date_input("Holiday Date")

    #             submit_button = st.form_submit_button(label="Add/Update Holiday")

    #             if submit_button:
    #                 add_or_update_holiday(holiday_name, holiday_date)
    #                 st.experimental_rerun()

    #     # Tab 3: Holiday List
    #     with tab3:
    #         st.write("### List of All Holidays")
    #         # Fetch and display all holidays
    #         if not holidays_df.empty:
    #             st.dataframe(holidays_df)
    #         else:
    #             st.write("No holidays found.")
    # elif menu == "Forecast":
    #     st.title("Forecast")
        
    #     # Load and join data
    #     joined_df = load_and_join_data()
        
    #     # Fill missing values in the complexity-related columns with 0
    #     complexity_columns = [
    #         'project_Complexity_Signals', 'project_Complexity_Lighting',
    #         'project_Complexity_ITS', 'project_Complexity_Power_Design',
    #         'project_Complexity_RoW_Coordination', 'project_Complexity_SLI_Project_Lead',
    #         'project_Complexity_Solar_Design', 'project_Complexity_Trunkline'
    #     ]
        
    #     # Ensure that missing values (None) are replaced with 0
    #     joined_df[complexity_columns] = joined_df[complexity_columns].fillna(0)

    #     # Create the new 'project_Complexity' column as the sum of all the complexity columns
    #     joined_df['project_Complexity'] = joined_df[complexity_columns].sum(axis=1)
        

    #     # Filter the DataFrame based on the project status
    #     filtered_df = joined_df[joined_df['project_State'].isin(['Actively Working', 'Approved', 'On-Hold'])]

    #     # Display the updated DataFrame with the new column
    #     st.write("Product Backlog Items joined with Features, Epics, and Projects:")
    #     st.dataframe(filtered_df)


# Check if the user is logged in
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False

# Display login screen or main screen based on login status
if not st.session_state['logged_in']:
    login_screen()
else:
    main_application()
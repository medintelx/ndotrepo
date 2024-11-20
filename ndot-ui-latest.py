import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os
import calendar
from streamlit_modal import Modal
from st_aggrid import AgGrid, GridOptionsBuilder, AgGridTheme
import datautility as du
from st_aggrid.shared import GridUpdateMode
load_dotenv()
#import logging

#DB_PATH = os.getenv('DB_PATH')
DB_PATH = 'NDOTDATA.db'
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
st.set_page_config(layout="wide")
login_css= """
<style>
.appview-container {
background-color: #0253A4 !important; 
text-color:white;
}
</style>
"""
import base64
# Function to load a local image and convert it to base64
def get_image_as_base64(image_file):
    with open(image_file, "rb") as f:
        data = f.read()
    return base64.b64encode(data).decode()

# Sidebar logo setup
def sidebar_with_logo():
    img_base64 = get_image_as_base64(os.path.join(os.path.dirname(__file__),"neveda.png"))
    # CSS to inject the logo into the sidebar header using the data-testid attribute
    st.markdown(
        """
        <style>
        [data-testid="stSidebarHeader"] {
            display: flex;
            align-items: center;
            justify-content: center;
        }

        [data-testid="stSidebarHeader"]::before {
            content: '';
            background-image: url(data:image/png;base64,{img_base64});
            background-size: contain;
            background-repeat: no-repeat;
            width: 120px;
            height: 120px;
            display: block;
            margin: 0 auto;
        }
        </style>
        """,
        unsafe_allow_html=True
    )


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
    div.st-ah {
                width: 30%;
    }
    .custom-error {
        color: #FF4C4C;  /* Use a visible color like orange */
        font-size: 16px;
        font-weight: bold;
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
            # st.error("Invalid username. Please try again.")
            st.markdown("<p class='custom-error'>Invalid username. Please try again.</p>", unsafe_allow_html=True)


# Function to initialize the user table in the database
def init_user_db():
    conn = sqlite3.connect(DB_PATH)
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

# Call the function to initialize the user table
init_user_db()

# Function to add a new user to the database
def add_user_to_db(name, email, role):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO users (name, email, role, user_type, start_date)
            VALUES (?, ?, ?, ?, ?)
        ''', (name, email, role, 1, datetime.now()))
        conn.commit()
        #st.success("User added successfully!")
    except sqlite3.IntegrityError:
        st.error("This email is already registered. Please use a different email.")
    
    conn.close()

def add_leave_to_db(user_id, leave_from, leave_to):
    conn = sqlite3.connect(DB_PATH)
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
    conn = sqlite3.connect(DB_PATH)
    today = datetime.today().date()
    
    leaves_df = pd.read_sql(f'''
        SELECT l.id, u.name, l.leave_from, l.leave_to 
        FROM leaves l 
        JOIN users u ON l.user_id = u.id
        WHERE l.leave_from >= '{today}'
    ''', conn)
    
    conn.close()
    return leaves_df

# Function to delete a leave entry from the database
def delete_leave_from_db(leave_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('DELETE FROM leaves WHERE id = ?', (leave_id,))
    conn.commit()
    conn.close()

# Function to fetch all users from the database
def fetch_users_from_db():
    conn = sqlite3.connect(DB_PATH)
    users_df = pd.read_sql("SELECT id, name FROM users", conn)
    conn.close()
    return users_df

def add_or_update_holiday(holiday_name, holiday_date):
    conn = sqlite3.connect(DB_PATH)
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
    conn = sqlite3.connect(DB_PATH)
    holidays_df = pd.read_sql("SELECT * FROM holidays ORDER BY holiday_date ASC", conn)
    conn.close()
    return holidays_df

# Function to delete a holiday entry from the database
def delete_holiday_from_db(holiday_date):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('DELETE FROM holidays WHERE holiday_date = ?', (holiday_date,))
    conn.commit()
    conn.close()

# Function to delete a user from the database
def delete_user_from_db(user_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('DELETE FROM users WHERE id = ?', (user_id,))
    conn.commit()
    conn.close()
    st.success(f"User with ID {user_id} has been deleted.")

#fetch latest config
def fetch_latest_config():
    conn = sqlite3.connect(DB_PATH)
    query = '''
        SELECT AnchorWgt, NonAnchorWgt, MiscWgt, AnchorMaxPoints, NonAnchorMaxPoints, EpicMinEffortPoints 
        FROM weightageconfig
        ORDER BY modifiedtime DESC LIMIT 1
    '''
    config = pd.read_sql(query, conn)
    conn.close()
    return config.iloc[0] if not config.empty else None

# Function to insert a new configuration if none exists, or update the existing one
def add_config_to_db(AnchorWgt,NonAnchorWgt, MiscWgt, AnchorMaxPoints, NonAnchorMaxPoints, EpicMinEffortPoints):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Calculate NonAnchorWgt as 100 - AnchorWgt
    NonAnchorWgt = 100 - AnchorWgt

    # Check if there is already a configuration in the table
    cursor.execute('SELECT COUNT(*) FROM weightageconfig')
    record_count = cursor.fetchone()[0]
    print(record_count)
    if record_count == 0:
        # If no record exists, insert a new one
        cursor.execute('''
            INSERT INTO weightageconfig (id, AnchorWgt, NonAnchorWgt, MiscWgt, AnchorMaxPoints, NonAnchorMaxPoints, EpicMinEffortPoints, modifiedtime)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (1, AnchorWgt, NonAnchorWgt, MiscWgt, AnchorMaxPoints, NonAnchorMaxPoints, EpicMinEffortPoints, datetime.now()))
        st.toast("Configuration inserted successfully!")
    else:
        # If a record already exists, update the first row
        cursor.execute('''
            UPDATE weightageconfig
            SET AnchorWgt = ?, NonAnchorWgt = ?, MiscWgt = ?, AnchorMaxPoints = ?, NonAnchorMaxPoints = ?, EpicMinEffortPoints = ?,modifiedtime=?
            WHERE rowid = 1  -- Assuming you always want to update the first row
        ''', (AnchorWgt, NonAnchorWgt, MiscWgt, AnchorMaxPoints, NonAnchorMaxPoints, EpicMinEffortPoints, datetime.now()))
        st.toast("Configuration updated successfully!")

    # Commit the transaction and close the connection
    conn.commit()
    conn.close()

# Function to fetch data from the database based on work item type
def fetch_data_from_db(work_item_type):
    conn = sqlite3.connect(DB_PATH)
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

def update_leave_in_db(leave_id, leave_from, leave_to):
    """
    Updates an existing leave record in the database.

    Parameters:
    - leave_id (int): The ID of the leave record to update.
    - leave_from (date): The new start date for the leave.
    - leave_to (date): The new end date for the leave.
    """
    conn = sqlite3.connect(DB_PATH)  # Use your database path
    cursor = conn.cursor()
    
    try:
        # Update the leave record with the provided dates
        cursor.execute('''
            UPDATE leaves
            SET leave_from = ?, leave_to = ?
            WHERE id = ?
        ''', (leave_from, leave_to, leave_id))
        
        conn.commit()
        st.success(f"Leave ID {leave_id} has been successfully updated.")
    except sqlite3.Error as e:
        st.error(f"An error occurred while updating the leave: {e}")
    finally:
        conn.close()

# Function to read tables into DataFrames and perform the joins
def load_and_join_data():
    conn = sqlite3.connect(DB_PATH)
    
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

# Custom HTMLCalendar subclass to start week on Wednesday
class CustomCalendar(calendar.HTMLCalendar):
    def __init__(self, firstweekday=2):  # 2 represents Wednesday
        super().__init__(firstweekday=firstweekday)


# Function to display styled calendar with holidays and weekends
def display_styled_calendar(month, year, holidays_df):
    cal = CustomCalendar()  # Use the custom calendar, starting on Wednesday

    # Convert the holiday dates to a list of datetime.date
    holiday_dates = pd.to_datetime(holidays_df['holiday_date']).dt.date.tolist()

    # Generate the calendar HTML for the specified month and year
    month_calendar = cal.formatmonth(year, month)

    # Highlight holidays and weekends by modifying the HTML of the calendar
    for day in range(1, 32):  # Loop through all possible days in a month
        try:
            date = datetime(year, month, day).date()
            
            # Check if the date is a holiday
            if date in holiday_dates:
                month_calendar = month_calendar.replace(f">{day}<", f' style="background-color: #ff9b9b;">{day}<')  # Orange for holidays

            # Check if the date is a Saturday or Sunday
            elif date.weekday() == 5:  # Saturday
                month_calendar = month_calendar.replace(f">{day}<", f' style="background-color: #D3D3D3;">{day}<')  # Orange for Saturday
            elif date.weekday() == 6:  # Sunday
                month_calendar = month_calendar.replace(f">{day}<", f' style="background-color: #D3D3D3;">{day}<')  # Orange for Sunday

        except ValueError:
            pass  # Ignore invalid dates (e.g., February 30)

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
    if 'updated_forecast_df' not in st.session_state:
        st.session_state.updated_forecast_df = pd.DataFrame()  # Initialize empty dataframe in session state
    if 'selected_project_details' not in st.session_state:
        st.session_state.selected_project_details = pd.DataFrame()  # Initialize empty dataframe in session state
        


    # Sidebar styling
    st.sidebar.markdown(
        """
        <style>
         /* Adjust the sidebar width */
        [data-testid="stSidebar"] {
            background-color: #0253A4; 
            min-width: 140px;  /* Set the minimum width for the sidebar */
            max-width: 140px;  /* Set the maximum width for the sidebar */
        }
        [data-testid="stButton"] {
           background-color: #0253A4; 
              border-radius: 50px; 
             width: 100%; 
            border-size: none;
            min-width: 130px;  /* Set the minimum width for the sidebar */
            max-width: 130px;  /* Set the maximum width for the sidebar */
            padding-top: 0px;
        }
       [data-testid="stButton"]  button {
        background-color:#0253A4;
        border-radius: 50px; 
         border: none;
        color: white;
        }


        [data-testid="stLogo"] {
         height: 5.5rem;
         width: 100%;
         margin: 0px 0px 0px 0px;
         padding: 0px;
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
        div.st-emotion-cache-1gwvy71.eczjsme12{
        margin-top: -36px;
        }
        .Apptitle {
        font-size: 14px;
        
        text-decoration-color: white;
        font-weight: 800;
        color: white;
        margin-top: -10px;
        width:100%;
        }
        </style>
        """, unsafe_allow_html=True
    )
    st.logo("neveda.png")
    st.sidebar.image("slingshot.png")
    htmlstr = """
<div class='Apptitle'>
<span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;SLIngshots!</span>
</div>
"""

    #st.sidebar.write(htmlstr)
    st.sidebar.html(htmlstr)
   # st.sidebar.write("SLI Forecasting Tool", key="ToolName")
    # Sidebar with interactive buttons
   # st.sidebar.divider()
    if st.sidebar.button('🏠 Home', key='home'):
        st.session_state.page = 'Home'
    # if st.sidebar.button('📊 Dashboard', key='dashboard'):
    #     st.session_state.page = 'Dashboard'
    if st.sidebar.button('👤 Resources', key='resources'):
        st.session_state.page = 'Resources'
    if st.sidebar.button('📅 Leaves', key='leaves'):
        st.session_state.page = 'Leaves'
    if st.sidebar.button('🎄 Holidays', key='holiday'):
        st.session_state.page = 'Holidays'
    if st.sidebar.button('⚙️ Settings', key='settings'):
        st.session_state.page = 'Settings'
        # Logout button
    if st.sidebar.button("🔓 Logout", key='logout'):
        st.session_state['logged_in'] = False  # Log out the user
        st.rerun() 
        

    # Based on the selected page, display the corresponding content
    if st.session_state.page == 'Home':
        
        st.markdown("""
    <style>
        div.stMainBlockContainer.block-container.st-emotion-cache-1jicfl2.ea3mdgi5 {
                    padding-top: 1rem !important;
                    padding-left: 2rem !important;
                    font-size: 12px;
        }
        div.stVerticalBlock.st-emotion-cache-2ajiip.e1f1d6gn2{
                    gap:0;
        }
        dev.stForm.st-emotion-cache-4uzi61.e10yg2by1{
                    background-color:  #ADD8E6 !important;
        }
    </style>
""", unsafe_allow_html=True)
        st.markdown(""" <style>
                    h1#forecast {
                    color: #ADD8E6;
                    font-size: 24px;
                    }
                    [data-testid="stVerticalBlock"] {
                    font-size: 10px;
                    }
                    button.st-emotion-cache-1vt4y43.ef3psqc16{
                    width:100%;
                    }
                    .custom-form {
        border: 2px solid #4CAF50;
                     background-color: #f0f8ff;
        padding: 0px;
        border-radius: 10px;
        margin-top: 20px;
    }
                    [data-testid="stForm"]{
                    background-color: #ADD8E6;
                    
                    }
             
                    </style>
                    """, unsafe_allow_html=True)
        with st.spinner('Loading Forecast...'):
            st.title("Forecast")
            mcol1, mcol2 = st.columns([1, 5])
            # Fetch the latest configuration values
            latest_config = du.fetch_latest_config()
            
            # Default values if no configuration exists
            if latest_config is not None:
                AnchorWgt_default = latest_config['AnchorWgt']
                NonAnchorWgt_default = latest_config['NonAnchorWgt']
                MiscWgt_default = latest_config['MiscWgt']
                AnchorMaxPoints_default = latest_config['AnchorMaxPoints']
                NonAnchorMaxPoints_default = latest_config['NonAnchorMaxPoints']
                EpicMinEffortPoints_default = latest_config['EpicMinEffortPoints']
            else:
                AnchorWgt_default = 0
                NonAnchorWgt_default = 0
                MiscWgt_default = 0
                AnchorMaxPoints_default = 0
                NonAnchorMaxPoints_default = 0
                EpicMinEffortPoints_default = 0
            with mcol1:
                # Create the form inside a div with the custom class
                with st.form(key='config_form'):
                    st.markdown('<div class="custom-form">', unsafe_allow_html=True)
                    #st.write("Project Weightage")
                    with st.container():
                        with st.popover("Type Weights"):
                        # Input fields will be stacked vertically inside the container
                            MiscWgt = st.number_input("Miscellaneous Weight %", min_value=0, value=MiscWgt_default,step=1, format="%d")
                            AnchorWgt = st.number_input("Anchor Weight %", min_value=0, value=AnchorWgt_default, step=1, format="%d")
                            NonAnchorWgt = st.number_input("Non-Anchor Weight %", min_value=0, value=(100-AnchorWgt), step=1, format="%d", disabled=True)
                        st.divider()
                        with st.popover("Max Points "):                     
                            AnchorMaxPoints = st.number_input("Anchor Max Effort Points", min_value=0,  value=AnchorMaxPoints_default,  step=1, format="%d")
                            NonAnchorMaxPoints = st.number_input("Non-Anchor Effort Max Points", min_value=0, value=NonAnchorMaxPoints_default , step=1, format="%d")
                        st.divider()
                        with st.popover("Min Points "):   
                            EpicMinEffortPoints = st.number_input("Epic Min Effort Points", min_value=0,value=EpicMinEffortPoints_default, step=1, format="%d")
                        
                        # Submit button at the end
                        st.divider()
                        submit_button = st.form_submit_button(label="Forecast")
                        st.markdown('</div>', unsafe_allow_html=True)
                        if submit_button:
                            add_config_to_db(AnchorWgt, NonAnchorWgt, MiscWgt, AnchorMaxPoints, NonAnchorMaxPoints, EpicMinEffortPoints)
                        # Recompute the updated forecast data based on the new configuration
                            anchor_project_df, non_anchor_project_df = du.get_project_data()
                            
                            upcoming_sprint_data = du.get_upcoming_sprints_with_effortpoints_and_weightage()

                            # Update session state with new forecast data for projects
                            st.session_state.updated_forecast_df = du.distribute_epics_to_sprints(anchor_project_df, non_anchor_project_df, upcoming_sprint_data)
                            # st.rerun()
                            # st.toast("Forecast updated successfully!")
            # Create the form inside a div with the custom class
            with mcol2:
            #  st.write("Project Forecast")
                df = fetch_data_from_db("Projects")
                df = df[["Title", "State", "Anchor_Project", "Priority_Traffic_Ops","Fiscal_Year","Funding_Source","Route_Type","Scoping_30_Percent","SeventyFivePercentComplete","Intermediate_Date","QAQC_Submittal_Date","Document_Submittal_Date"]]
        



                tabs = st.tabs(["Projects"])
            
                with tabs[0]:

                    anchor_project_df, non_anchor_project_df = du.get_project_data()
                    upcoming_sprint_data = du.get_upcoming_sprints_with_effortpoints_and_weightage()
                    allocation,anchor_projects_df,non_anchor_projects_df = du.distribute_epics_to_sprints(anchor_project_df, non_anchor_project_df, upcoming_sprint_data)
                    #st.write(allocation)
                    df = st.session_state.updated_forecast_df = allocation
                    #st.dataframe(st.session_state.updated_forecast_df,    hide_index=True, on_select="rerun")
                    # Display the dataframe with single-column selection enabled
                    selection_event = st.dataframe(
                        df,
                        selection_mode="single-column",
                        on_select="rerun",
                        use_container_width=True,
                        hide_index=True
                    )

                # Prepare an empty list to collect project details
                    project_details = []

                    # Check if a column is selected
                    if selection_event and selection_event.selection.get('columns'):
                        selected_column = selection_event.selection['columns'][0]
                        
                        # Iterate over selected column values
                        for index, value in df[selected_column].items():
                            # Extract ID and type (A for Anchor, NA for Non-Anchor) from the column value
                            if isinstance(value, str):
                                # Parse `value` for project ID, A/NA designation, epic title, and effort points
                                try:
                                    project_id = int(value.split()[0])  # Extract project ID
                                    designation = value.split("(")[1].split(")")[0]  # Extract "A" or "NA"
                                    epic_title = value.split(" - ")[1].split(" (")[0]  # Extract epic title

                                    # Select the relevant DataFrame based on designation
                                    relevant_df = anchor_projects_df if designation == "A" else non_anchor_projects_df
                                    
                                    # Filter the relevant DataFrame for the selected project ID and Epic Title
                                    project_info = relevant_df[
                                        (relevant_df['projects_Work_Item_ID'] == project_id) &
                                        (relevant_df['epics_System_Title'] == epic_title)
                                    ]
                                    print(project_info)
                                    # Append project details to the list if found
                                    if not project_info.empty:
                                        project_details.append({
                                            "Project ID": project_info['projects_Work_Item_ID'].values[0],
                                            "Project Title": project_info['projects_Title'].values[0],
                                            "Epic ID": project_info['epics_System_Id'].values[0],
                                            "Epic Title": project_info['epics_System_Title'].values[0],
                                            "Total Effort Points": project_info['total_effort_from_pbis'].values[0],
                                            "Nearest Due Date": project_info['nearest_doc_date'].values[0],
                                            "Sprint": selected_column
                                        })
                                except (IndexError, ValueError):
                                    st.write("Error parsing value:", value)
                                    continue
                    # Convert the list to a DataFrame
                    if project_details:
                        details_df = pd.DataFrame(project_details)
                        st.session_state['selected_project_details'] = details_df 
                        
        
          
        
        
            st.write("Epic status")
            if not st.session_state['selected_project_details'].empty:
                print(st.session_state['selected_project_details'])
                # Convert 'nearest_due_date' column to datetime and format as MM/DD/YYYY
                st.session_state['selected_project_details']['Nearest Due Date'] = pd.to_datetime(st.session_state['selected_project_details']['Nearest Due Date'], errors='coerce')
                st.session_state['selected_project_details']['Nearest Due Date'] = st.session_state['selected_project_details']['Nearest Due Date'].dt.strftime('%m/%d/%Y')

      
                st.session_state['selected_project_details']['Project ID'] = st.session_state['selected_project_details']['Project ID'].astype(int)
                st.dataframe(st.session_state['selected_project_details'].style.format({"Project ID": "{:.0f}", 'Total Effort Points': "{}"}), hide_index=True) 
            # st.write(anchor_project_df)
            # st.write("non-anchor")
            # st.write(non_anchor_project_df)
        
            # st.write("Sprint Allocation")
            # st.write(allocation)
            st.write("Upcoming Sprint Data")

            # Convert 'Start_date' and 'End_date' to US date format
            upcoming_sprint_data['Start_date'] = pd.to_datetime(upcoming_sprint_data['Start_date'], errors='coerce').dt.strftime('%m/%d/%Y')
            upcoming_sprint_data['End_date'] = pd.to_datetime(upcoming_sprint_data['End_date'], errors='coerce').dt.strftime('%m/%d/%Y')

            st.dataframe(upcoming_sprint_data, hide_index=True) 
            st.write("Anchor")
            anchor_projects_df['projects_Fiscal_Year'] = pd.to_numeric(anchor_projects_df['projects_Fiscal_Year'], errors='coerce').fillna(0).astype(int)
            anchor_projects_df['projects_Priority_Traffic_Ops'] = pd.to_numeric(anchor_projects_df['projects_Priority_Traffic_Ops'], errors='coerce').fillna(0).astype(int)
            anchor_projects_df['projects_Construction_EA_Number'] = pd.to_numeric(anchor_projects_df['projects_Construction_EA_Number'], errors='coerce').fillna(0).astype(int)
            anchor_projects_df['projects_Work_Item_ID'] = pd.to_numeric(anchor_projects_df['projects_Work_Item_ID'], errors='coerce').fillna(0).astype(int)
            anchor_projects_df['epics_System_Id'] = pd.to_numeric(anchor_projects_df['epics_System_Id'], errors='coerce').fillna(0).astype(int)
            anchor_projects_df['projects_Anchor_Project'] = pd.to_numeric(anchor_projects_df['projects_Anchor_Project'], errors='coerce').fillna(0).astype(int)
            anchor_projects_df['projects_Complexity_Signals'] = pd.to_numeric(anchor_projects_df['projects_Complexity_Signals'], errors='coerce').fillna(0).astype(int)
            anchor_projects_df['projects_Complexity_Lighting'] = pd.to_numeric(anchor_projects_df['projects_Complexity_Lighting'], errors='coerce').fillna(0).astype(int)
            anchor_projects_df['projects_Complexity_ITS'] = pd.to_numeric(anchor_projects_df['projects_Complexity_ITS'], errors='coerce').fillna(0).astype(int)
            anchor_projects_df['projects_Complexity_Power_Design'] = pd.to_numeric(anchor_projects_df['projects_Complexity_Power_Design'], errors='coerce').fillna(0).astype(int)
            anchor_projects_df['projects_Complexity_RoW_Coordination'] = pd.to_numeric(anchor_projects_df['projects_Complexity_RoW_Coordination'], errors='coerce').fillna(0).astype(int)
            anchor_projects_df['projects_Complexity_SLI_Project_Lead'] = pd.to_numeric(anchor_projects_df['projects_Complexity_SLI_Project_Lead'], errors='coerce').fillna(0).astype(int)
            anchor_projects_df['projects_Complexity_Solar_Design'] = pd.to_numeric(anchor_projects_df['projects_Complexity_Solar_Design'], errors='coerce').fillna(0).astype(int)
            anchor_projects_df['projects_Complexity_Trunkline'] = pd.to_numeric(anchor_projects_df['projects_Complexity_Trunkline'], errors='coerce').fillna(0).astype(int)
            anchor_projects_df['projects_complexity'] = pd.to_numeric(anchor_projects_df['projects_complexity'], errors='coerce').fillna(0).astype(int)
            
            # Update the formatting dictionary with your actual column names
            format_dict = {
                'total_effort_from_pbis': '{}', 
                'nearest_doc_date': lambda x: pd.to_datetime(x).strftime('%m/%d/%Y') if pd.notnull(x) else '',
                'projects_Scoping_30_Percent': lambda x: pd.to_datetime(x).strftime('%m/%d/%Y') if pd.notnull(x) else '',
                'projects_SeventyFivePercentComplete': lambda x: pd.to_datetime(x).strftime('%m/%d/%Y') if pd.notnull(x) else '',
                'projects_Intermediate_Date': lambda x: pd.to_datetime(x).strftime('%m/%d/%Y') if pd.notnull(x) else '',
                'projects_QAQC_Submittal_Date': lambda x: pd.to_datetime(x).strftime('%m/%d/%Y') if pd.notnull(x) else '',
                'projects_Document_Submittal_Date': lambda x: pd.to_datetime(x).strftime('%m/%d/%Y') if pd.notnull(x) else '',
                'projects_Official_DOC_Date': lambda x: pd.to_datetime(x).strftime('%m/%d/%Y') if pd.notnull(x) else ''
            }
            
            # Apply styling to format columns
            styled_anchor_df = anchor_projects_df.style.format(format_dict)

            st.dataframe(styled_anchor_df, hide_index=True)
            st.write("Non Anchor")
            non_anchor_projects_df['projects_Fiscal_Year'] = pd.to_numeric(non_anchor_projects_df['projects_Fiscal_Year'], errors='coerce').fillna(0).astype(int)
            non_anchor_projects_df['projects_Priority_Traffic_Ops'] = pd.to_numeric(non_anchor_projects_df['projects_Priority_Traffic_Ops'], errors='coerce').fillna(0).astype(int)
            non_anchor_projects_df['projects_Construction_EA_Number'] = pd.to_numeric(non_anchor_projects_df['projects_Construction_EA_Number'], errors='coerce').fillna(0).astype(int)
            non_anchor_projects_df['projects_Work_Item_ID'] = pd.to_numeric(non_anchor_projects_df['projects_Work_Item_ID'], errors='coerce').fillna(0).astype(int)
            non_anchor_projects_df['epics_System_Id'] = pd.to_numeric(non_anchor_projects_df['epics_System_Id'], errors='coerce').fillna(0).astype(int)
            non_anchor_projects_df['projects_Anchor_Project'] = pd.to_numeric(non_anchor_projects_df['projects_Anchor_Project'], errors='coerce').fillna(0).astype(int)
            non_anchor_projects_df['projects_Complexity_Signals'] = pd.to_numeric(non_anchor_projects_df['projects_Complexity_Signals'], errors='coerce').fillna(0).astype(int)
            non_anchor_projects_df['projects_Complexity_Lighting'] = pd.to_numeric(non_anchor_projects_df['projects_Complexity_Lighting'], errors='coerce').fillna(0).astype(int)
            non_anchor_projects_df['projects_Complexity_ITS'] = pd.to_numeric(non_anchor_projects_df['projects_Complexity_ITS'], errors='coerce').fillna(0).astype(int)
            non_anchor_projects_df['projects_Complexity_Power_Design'] = pd.to_numeric(non_anchor_projects_df['projects_Complexity_Power_Design'], errors='coerce').fillna(0).astype(int)
            non_anchor_projects_df['projects_Complexity_RoW_Coordination'] = pd.to_numeric(non_anchor_projects_df['projects_Complexity_RoW_Coordination'], errors='coerce').fillna(0).astype(int)
            non_anchor_projects_df['projects_Complexity_SLI_Project_Lead'] = pd.to_numeric(non_anchor_projects_df['projects_Complexity_SLI_Project_Lead'], errors='coerce').fillna(0).astype(int)
            non_anchor_projects_df['projects_Complexity_Solar_Design'] = pd.to_numeric(non_anchor_projects_df['projects_Complexity_Solar_Design'], errors='coerce').fillna(0).astype(int)
            non_anchor_projects_df['projects_Complexity_Trunkline'] = pd.to_numeric(non_anchor_projects_df['projects_Complexity_Trunkline'], errors='coerce').fillna(0).astype(int)
            non_anchor_projects_df['projects_complexity'] = pd.to_numeric(non_anchor_projects_df['projects_complexity'], errors='coerce').fillna(0).astype(int)
            

                # Apply styling to format columns
            styled_non_anchor_df = non_anchor_project_df.style.format(format_dict)
            st.dataframe(styled_non_anchor_df, hide_index=True)

    elif st.session_state.page == 'Dashboard':
        st.title("Dashboard")
        st.write("Welcome to the Dashboard!")

#     elif st.session_state.page == 'Leaves':
#         st.markdown(""" <style>
#                     .LeaveManage {
#                     color: #ADD8E6;
#                     font-size: 20px;
#                     font-weight: bold;  
#                     }
                   
#                      div.stMainBlockContainer.block-container.st-emotion-cache-1jicfl2.ea3mdgi5 {
#                     padding-top: 2rem !important;
#                     padding-left: 2rem !important;
#                     font-size: 12px;
#         }
#         div.stVerticalBlock.st-emotion-cache-2ajiip.e1f1d6gn2{
#                     gap:0;
#         }
#         dev.stForm.st-emotion-cache-4uzi61.e10yg2by1{
#                     background-color:  #ADD8E6 !important;
#         }
#                     }
#                     </style>
#                     """, unsafe_allow_html=True)

#      # Title for Leave Management
#         htmlleavestr = """
# <div class='LeaveManage'>
# <span>Leave Management</span>
# </div>
# """ 
#         st.html(htmlleavestr)
#         #st.title("Leave Management")

#         # Create a modal instance
#         modal = Modal(key="add_leave_modal", title="Add Leave")

#         # Add Leave button in the top-right corner
#         add_leave_button = st.button("Add Leave", key="add_leave_button", help="Click to add leave")



#             # Fetch leaves data
#         leaves_df = fetch_leaves_from_db()

       
        
#         if not leaves_df.empty:
#             # Display the column headers using st.columns
#             header1, header2, header3, header5= st.columns([1, 1,1,1])
#             header1.write("**Name**")
#             header2.write("**Leave From**")
#             header3.write("**Leave To**")
#             #header4.write("*Edit Actions*")
#             header5.write("**Action**")

#             for i, row in leaves_df.iterrows():
#                 col1, col2, col3, col5 = st.columns([1, 1, 1, 1])
#                 col1.write(row["name"])
#                                 # Convert 'leave_from' and 'leave_to' to US date format
#                 leave_from_us = pd.to_datetime(row["leave_from"]).strftime('%m/%d/%Y')
#                 leave_to_us = pd.to_datetime(row["leave_to"]).strftime('%m/%d/%Y')

#                 # Write the formatted dates
#                 col2.write(leave_from_us)
#                 col3.write(leave_to_us)


#                 # Add Edit button
#                 # edit_button = col4.button("Edit", key=f"edit_{row['id']}")
#                 # if edit_button:
#                 #     st.session_state["edit_leave_id"] = row["id"]
#                 #     st.session_state["edit_leave_from"] = row["leave_from"]
#                 #     st.session_state["edit_leave_to"] = row["leave_to"]
#                 #     modal.open()  # Open edit modal

#                 # Add Delete button
#                 delete_button = col5.button("Delete", key=f"delete_{row['id']}")
#                 if delete_button:
#                     delete_leave_from_db(row["id"])
#                     st.success("Leave deleted successfully!")
#                     st.rerun()  # Refresh to reflect changes
#         else:
#             st.write("No leave records found.")

#         # Trigger modal when Add Leave button is clicked
#         if add_leave_button:
#             modal.open()
#         if modal.is_open() and "edit_leave_id" in st.session_state:
#             with modal.container():
#                 st.write("Edit Leave")

#                 with st.form(key='edit_leave_form'):
#                     leave_from = st.date_input("Leave From", value=st.session_state["edit_leave_from"])
#                     leave_to = st.date_input("Leave To", value=st.session_state["edit_leave_to"])
#                     submit_button = st.form_submit_button(label="Update Leave")

#                     if submit_button:
#                         update_leave_in_db(st.session_state["edit_leave_id"], leave_from, leave_to)
#                         st.success("Leave updated successfully!")
#                         st.rerun()  # Refresh to display updated leave
#         if modal.is_open():
#             with modal.container():
                

#                 # Fetch all users from the database
#                 users_df = fetch_users_from_db()

#                 if not users_df.empty:
#                     # Form for adding a leave

#                     with st.form(key='leave_form'):
#                         user = st.selectbox("Select Resource", users_df['name'])
#                         leave_from = st.date_input("Leave From")
#                         leave_to = st.date_input("Leave To")

#                         submit_button = st.form_submit_button(label="Submit Leave")

#                         if submit_button:
#                             # Get the selected user's ID
#                             user_id = users_df[users_df['name'] == user]['id'].values[0]
                            
#                             # Add the leave to the database
#                             add_leave_to_db(user_id, leave_from, leave_to)
#                             st.rerun()

#                 else:
#                     st.write("No Resources available.")
    elif st.session_state.page == 'Leaves':
        st.markdown(""" <style>
                    .LeaveManage {
                    color: #ADD8E6;
                    font-size: 20px;
                    font-weight: bold;  
                    }
                    
                    div.stMainBlockContainer.block-container.st-emotion-cache-1jicfl2.ea3mdgi5 {
                    padding-top: 2rem !important;
                    padding-left: 2rem !important;
                    font-size: 12px;
                    }
                    div.stVerticalBlock.st-emotion-cache-2ajiip.e1f1d6gn2{
                        gap:0;
                    }
                    dev.stForm.st-emotion-cache-4uzi61.e10yg2by1{
                        background-color:  #ADD8E6 !important;
                    }

                    /* Styling for clickable icon buttons */
                    .icon-button {
                        font-size: 20px;
                        padding: 0;
                        margin: 0;
                        border: none;
                        background: none;
                        cursor: pointer;
                    }
                    </style>
                    """, unsafe_allow_html=True)
    
        # Title for Leave Management
        htmlleavestr = """
    <div class='LeaveManage'>
    <span>Leave Management</span>
    </div>
    """ 
        st.html(htmlleavestr)

        # Create a modal instance for adding or editing leave
        modal = Modal(key="add_edit_leave_modal", title="Add/Edit Leave")

        # Add Leave button in the top-right corner
        add_leave_button = st.button("Add Leave", key="add_leave_button", help="Click to add leave")

        # Fetch leaves data
        leaves_df = fetch_leaves_from_db()

        if not leaves_df.empty:
            # Display the column headers using st.columns
            header1, header2, header3, header4, header5 = st.columns([1, 1, 1, 1, 1])
            header1.write("**Name**")
            header2.write("**Leave From**")
            header3.write("**Leave To**")
            header4.write("**Edit**")
            header5.write("**Delete**")
            
            for i, row in leaves_df.iterrows():
                col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 1])
                col1.write(row["name"])

                # Convert 'leave_from' and 'leave_to' to a user-friendly format
                leave_from_us = pd.to_datetime(row["leave_from"]).strftime('%m/%d/%Y')
                leave_to_us = pd.to_datetime(row["leave_to"]).strftime('%m/%d/%Y')
                col2.write(leave_from_us)
                col3.write(leave_to_us)

                # Edit icon as a clickable button
                edit_clicked = col4.button("Edit", key=f"edit_{row['id']}", help="Edit Leave")

                # Delete icon as a clickable button
                delete_clicked = col5.button("Delete", key=f"delete_{row['id']}", help="Delete Leave")

                # Check if edit icon was clicked
                if edit_clicked:
                    st.session_state["edit_leave_id"] = row["id"]
                    st.session_state["edit_leave_from"] = pd.to_datetime(row["leave_from"])
                    st.session_state["edit_leave_to"] = pd.to_datetime(row["leave_to"])
                    st.session_state["edit_user_name"] = row["name"]
                    modal.open()  # Open edit modal

                # Check if delete icon was clicked
                if delete_clicked:
                    delete_leave_from_db(row["id"])
                    st.success("Leave deleted successfully!")
                    st.rerun()  # Refresh to reflect changes
        else:
            st.write("No leave records found.")

        # Trigger modal when Add Leave button is clicked
        if add_leave_button:
            st.session_state.pop("edit_leave_id", None)  # Clear any existing edit ID
            modal.open()

        if modal.is_open():
            with modal.container():
                # Fetch all users from the database
                users_df = fetch_users_from_db()

                if not users_df.empty:
                    # Start the form inside the modal
                    with st.form(key='leave_form'):
                        if "edit_leave_id" in st.session_state:
                            # Editing existing leave
                            st.write("Edit Leave")
                            try:
                                user_name = st.session_state["edit_user_name"]
                                user_options = users_df['name'].tolist()  # Convert to a list of strings
                                
                                # Find the index of the selected user
                                user_index = user_options.index(user_name) if user_name in user_options else 0
                                
                                user_name = st.selectbox("Select User", user_options, index=user_index)
                            except Exception as e:
                                st.error(f"Error: {e}")
                                user_name = st.selectbox("Select User", users_df['name'].tolist())  # Fallback

                            # Convert leave_from to datetime.date for compatibility
                            leave_from = st.session_state["edit_leave_from"].date() if isinstance(st.session_state["edit_leave_from"], pd.Timestamp) else st.session_state["edit_leave_from"]
                            leave_to = st.session_state["edit_leave_to"].date() if isinstance(st.session_state["edit_leave_to"], pd.Timestamp) else st.session_state["edit_leave_to"]

                            leave_from = st.date_input("Leave From", value=leave_from)

                            # Dynamically adjust the default value of leave_to
                            leave_to_default = max(leave_to, leave_from)
                            leave_to = st.date_input(
                                "Leave To", 
                                value=leave_to_default,
                                min_value=leave_from  # Restrict minimum date to "From Date"
                            )
                            submit_label = "Update Leave"
                        else:
                            # Adding new leave
                            st.write("Add Leave")
                            user_name = st.selectbox("Select User", users_df['name'].tolist())
                            leave_from = st.date_input("Leave From")
                            leave_to = st.date_input(
                                "Leave To", 
                                min_value=leave_from,  # Restrict minimum date to "From Date"
                                value=leave_from  # Default value is the same as "From Date"
                            )
                            submit_label = "Submit Leave"

                        # Add the submit button inside the form
                        submit_button = st.form_submit_button(label=submit_label)

                        if submit_button:
                            # Edit existing leave
                            if "edit_leave_id" in st.session_state:
                                update_leave_in_db(st.session_state["edit_leave_id"], leave_from, leave_to)
                                st.success("Leave updated successfully!")
                                st.session_state.pop("edit_leave_id")  # Clear edit session state
                                modal.close()
                                st.rerun()
                            else:
                                # Add new leave
                                user_id = users_df[users_df['name'] == user_name]['id'].values[0]
                                add_leave_to_db(user_id, leave_from, leave_to)
                                st.success("Leave added successfully!")
                                modal.close()
                                st.rerun()

                            st.rerun()  # Refresh to display the updated list
                else:
                    st.write("No users available.")
    elif st.session_state.page == 'Resources':
        st.markdown(""" <style>
                    .UserManage {
                    color: #ADD8E6;
                    font-size: 20px;
                    font-weight: bold;  
                    }
                   
                     div.stMainBlockContainer.block-container.st-emotion-cache-1jicfl2.ea3mdgi5 {
                    padding-top: 2rem !important;
                    padding-left: 2rem !important;
                    font-size: 12px;
        }
        div.stVerticalBlock.st-emotion-cache-2ajiip.e1f1d6gn2{
                    gap:0;
        }
        dev.stForm.st-emotion-cache-4uzi61.e10yg2by1{
                    background-color:  #ADD8E6 !important;
        }
                    }
                    </style>
                    """, unsafe_allow_html=True)
        htmluserstr = """
<div class='UserManage'>
<span>Resource Management</span>
</div>
""" 
        st.html(htmluserstr)
        tab2,tab1  = st.tabs(["All Resources", "Add Resources (Slingshots only)"])

        with tab1:
            # Create a form for user creation
            with st.form(key='user_form',  enter_to_submit=False):
                name = st.text_input("Name", max_chars=50)
                email = st.text_input("Email", max_chars=100)
                #role = st.selectbox("Role", ["Admin", "Viewer", "Editor"])
                #active_status = st.checkbox("Active Status", value=True)
                
                # Submit button
                submit_button = st.form_submit_button(label="Add Resource")
 
                
                if submit_button:
                    # Validation: Check if name or email is empty
                    if not name.strip():
                        st.error("Please enter a name.")
                    elif not email.strip():
                        st.error("Please enter an email.")
                    else:
                        # Proceed to add the user to the database if both fields are filled
                        add_user_to_db(name, email, "Viewer")
                        st.success("Resource added successfully!")
        with tab2:   
        # Display existing users
            conn = sqlite3.connect(DB_PATH)
            users = pd.read_sql("SELECT name, email FROM users where start_date <= datetime('now') and user_type=0", conn)
            dummyusers = pd.read_sql("SELECT name, email FROM users where user_type=1", conn)
            conn.close()    
            
            st.subheader("Existing Resources")
            
            if not users.empty:
                usersdf = pd.DataFrame(users)
                # Rename columns in the DataFrame
                usersdf = usersdf.rename(columns={"name": "Name", "email": "Email"})
                st.dataframe(usersdf,  use_container_width=True,  hide_index=True)
            else:
                st.write("No resources found.")
            # Display dummyusers table with delete functionality
            

                    # Check if dummyusers DataFrame is not empty
            if not dummyusers.empty:
                dummyusers = dummyusers.rename(columns={"name": "Name", "email": "Email"})
                st.subheader("Newly Added Resources")
                st.markdown(
    "<p style='font-size:12px;'>(These are dummy entries, intended only for testing effort point allocations.)</p>",
    unsafe_allow_html=True
)
                # Create GridOptions with checkbox selection mode

                row_height = 35  # Approximate row height in pixels
                grid_height = min(400, (len(dummyusers) * row_height) + 35) 
                gb = GridOptionsBuilder.from_dataframe(dummyusers)
                gb.configure_selection(selection_mode="single", use_checkbox=True)
                gb.configure_default_column(resizable=True)
                gb.configure_grid_options(domLayout='autoHeight')  # Sets auto height for the grid
                for col in dummyusers.columns:
                    gb.configure_column(col, flex=1) 
                 # Additional grid options to fit the screen width
                gb.configure_grid_options(domLayout='autoHeight', suppressHorizontalScroll=True)
    
                # Set columns to auto fit to the grid width
                gb.configure_grid_options(fit_columns_on_grid_load=True)
                            # Build the grid options
                grid_options = gb.build()

                # Display the AgGrid with selection mode
                grid_response = AgGrid(
                    dummyusers,
                    gridOptions=grid_options,
                    update_mode=GridUpdateMode.SELECTION_CHANGED,
               
                    fit_columns_on_grid_load=True,
                    allow_unsafe_jscode=True ,
                    enable_enterprise_modules=False,
                    theme = AgGridTheme.STREAMLIT,
                    height = grid_height
                     
                )

                # Extract selected rows
                selected_rows = grid_response['selected_rows']

                # Check if selected_rows is a non-empty DataFrame
                if selected_rows is not None and  not selected_rows.empty:
                    # Extract the email value from the selected row
                    email_to_delete = selected_rows.iloc[0]['Email']
                    
                    # Display a delete button for confirmation
                    if st.button("Delete"):
                        # Delete the selected user from the database
                        conn = sqlite3.connect(DB_PATH)
                        conn.execute("DELETE FROM users WHERE email = ?", (email_to_delete,))
                        conn.commit()
                        conn.close()
                        st.success(f"Deleted resource with email: {email_to_delete}")
                        st.rerun()  # Refresh to show updated data
         
               
    elif st.session_state.page == 'Holidays':
        st.markdown(""" <style>
        div.stMainBlockContainer.block-container.st-emotion-cache-1jicfl2.ea3mdgi5 {
                    padding-top: 2rem !important;
                    padding-left: 2rem !important;
                    font-size: 12px;
        }
        div.stVerticalBlock.st-emotion-cache-2ajiip.e1f1d6gn2{
                    gap:0;
        }
        dev.stForm.st-emotion-cache-4uzi61.e10yg2by1{
                    background-color:  #ADD8E6 !important;
        }
    </style>
""", unsafe_allow_html=True)
        st.markdown(""" <style>
                    .HolidayManage {
                    color: #ADD8E6;
                    font-size: 20px;
                    font-weight: bold;  
                    }
                   
    
                    </style>
                    """, unsafe_allow_html=True)
        htmlholidaystr = """
<div class='HolidayManage'>
<span>Holiday Management</span>
</div>
""" 
        st.html(htmlholidaystr)
        #st.title("Holiday Management")

        # Fetch all holidays from the database
        holidays_df = fetch_holidays_from_db()

        # Adding previous and next buttons to navigate between months
        if "month" not in st.session_state:
            st.session_state.month = current_month
        if "year" not in st.session_state:
            st.session_state.year = current_year

        # Create tabs for the Holidays menu
        tab1, tab2, tab3 = st.tabs([ "Holiday List","Add/Modify Holiday", "Calendar"])

        # Tab 1: Calendar
        with tab3:
            st.write("### Holiday Calendar")
            # Display navigation buttons for the calendar
            col1, col2, col3 = st.columns([0.5, 1, 1], gap="small",vertical_alignment="center")

            with col1:
                if st.button("Previous Month"):
                    if st.session_state.month == 1:
                        st.session_state.month = 12
                        st.session_state.year -= 1
                        st.rerun()
                    else:
                        st.session_state.month -= 1
                        st.rerun()

            with col2:
                st.write(f"### {calendar.month_name[st.session_state.month]} {st.session_state.year}")
                # Display the styled calendar for the selected month and year
                display_styled_calendar(st.session_state.month, st.session_state.year, holidays_df)
                
                # Create the legend with colored blocks
                st.markdown("""
                <style>
                    .legend-container {
                        display: flex;
                        flex-direction: row;
                        align-items: center;
                        margin-bottom: 10px;
                    }
                    .legend-box {
                        width: 20px;
                        height: 20px;
                        margin-right: 10px;
                        display: inline-block;
                        border-radius: 5px;
                    }
                    .holiday { background-color: #ff9b9b; } /* Color for holidays */
                    .weekend { background-color: #D3D3D3; } /* Color for weekends */
                    .workday { background-color: #FFFFFF; border: 1px solid #DDD; } /* Color for regular workdays */
                </style>
                """, unsafe_allow_html=True)

                # Display each legend item with a description
                st.markdown("""
                <div class="legend-container">
                    <div class="legend-box holiday"></div>
                    <span>Holiday</span>
                    &nbsp; &nbsp; 
                    <div class="legend-box weekend"></div>
                    <span>Weekend</span>
                </div>
              
                """, unsafe_allow_html=True)

            with col3:
                if st.button("Next Month"):
                    if st.session_state.month == 12:
                        st.session_state.month = 1
                        st.session_state.year += 1
                        st.rerun()
                    else:
                        st.session_state.month += 1
                        st.rerun()

            

        # Tab 2: Add/Modify Holiday
        with tab2:
            
            st.write("### Add or Modify Holidays")
            
            # Form for adding/updating holidays
            with st.form(key='holiday_form'):
                holiday_name = st.text_input("Holiday Name", max_chars=100)
                holiday_date = st.date_input("Holiday Date")

                submit_button = st.form_submit_button(label="Add/Update Holiday")

                if submit_button:
                    add_or_update_holiday(holiday_name, holiday_date)
                    st.rerun()

        # Tab 3: Holiday List
        with tab1:
            holidays_df = fetch_holidays_from_db()  # Fetch all holidays

     
            holidays_df = holidays_df[["holiday_name", "holiday_date"]]

            if not holidays_df.empty:
                # Display column headers
                col1, col2, col3 = st.columns([2, 2, 1])
                col1.write("**Holiday Name**")
                col2.write("**Holiday Date**")
                col3.write("**Action**")

                # Display each row with a delete button
                for i, row in holidays_df.iterrows():
                    col1, col2, col3 = st.columns([2, 2, 1])
                    col1.write(row["holiday_name"])
                    us_formatted_date = pd.to_datetime(row["holiday_date"]).strftime('%m/%d/%Y')
                    col2.write(us_formatted_date)   
                 

                    # Add delete button for each holiday
                    delete_button = col3.button("Delete", key=f"delete_{row['holiday_date']}")
                    
                    if delete_button:
                        delete_holiday_from_db(row["holiday_date"])
                        st.success(f"Holiday '{row['holiday_name']}' on {row['holiday_date']} deleted.")
                        st.rerun()  # Refresh the page to reflect changes
            else:
                st.write("No holidays found.")

    elif st.session_state.page == 'Settings':
        st.markdown(""" <style>
                    .weightManage {
                    color: #ADD8E6;
                    font-size: 20px;
                    font-weight: bold;  
                    }
                   
                     div.stMainBlockContainer.block-container.st-emotion-cache-1jicfl2.ea3mdgi5 {
                    padding-top: 2rem !important;
                    padding-left: 2rem !important;
                    font-size: 12px;
        }
        div.stVerticalBlock.st-emotion-cache-2ajiip.e1f1d6gn2{
                    gap:0;
        }
        dev.stForm.st-emotion-cache-4uzi61.e10yg2by1{
                    background-color:  #ADD8E6 !important;
        }
                    }
                    </style>
                    """, unsafe_allow_html=True)

     # Title for Leave Management
        htmlleavestr = """
<div class='weightManage'>
<span>Settings</span>
</div>
""" 
        st.html(htmlleavestr)
        st.title("Weightage Configurations")

            # Reset the configuration data in session state if not already set
        if "config_data" not in st.session_state:
            st.session_state.config_data = fetch_latest_config()
        
        config = st.session_state.config_data  # Retrieve configuration from session state

        # Set default values from session state if available, otherwise use fallback values
        AnchorWgt_default = config['AnchorWgt'] if config is not None else 0
        NonAnchorWgt_default = config['NonAnchorWgt'] if config is not None else 0
        MiscWgt_default = config['MiscWgt'] if config is not None else 0
        AnchorMaxPoints_default = config['AnchorMaxPoints'] if config is not None else 0
        NonAnchorMaxPoints_default = config['NonAnchorMaxPoints'] if config is not None else 0
        EpicMinEffortPoints_default = config['EpicMinEffortPoints'] if config is not None else 0

        # Display the form with prepopulated data
        with st.form(key='config_form'):
            AnchorWgt = st.number_input("Anchor Weight", min_value=0, value=AnchorWgt_default, step=1)
            NonAnchorWgt = st.number_input("Non-Anchor Weight", min_value=0, value=NonAnchorWgt_default, step=1, disabled=True)
            MiscWgt = st.number_input("Miscellaneous Weight", min_value=0, value=MiscWgt_default, step=1)
            AnchorMaxPoints = st.number_input("Anchor Max Points", min_value=0, value=AnchorMaxPoints_default, step=1)
            NonAnchorMaxPoints = st.number_input("Non-Anchor Max Points", min_value=0, value=NonAnchorMaxPoints_default, step=1)
            EpicMinEffortPoints = st.number_input("Epic Min Effort Points", min_value=0, value=EpicMinEffortPoints_default, step=1)
            
            submit_button = st.form_submit_button(label="Save Configuration")
            
            if submit_button:
                # Save data back to the database (replace this with your existing update logic)
                add_config_to_db(AnchorWgt, NonAnchorWgt, MiscWgt, AnchorMaxPoints, NonAnchorMaxPoints, EpicMinEffortPoints)
                # Update session state with new config to reflect changes without leaving the page
                st.session_state.config_data = fetch_latest_config()
                st.rerun()
   
# Check if the user is logged in
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False

# Display login screen or main screen based on login status
if not st.session_state['logged_in']:
    login_screen()
else:
    main_application()
   

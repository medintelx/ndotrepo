import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os
import plotly.graph_objects as go
import pandas as pd
from st_material_table import st_material_table
import calendar
from streamlit_modal import Modal
#from streamlit_timeline import st_timeline
#from streamlit_timeline import timeline
from st_aggrid import AgGrid, GridOptionsBuilder
import pandas as pd



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
    conn = sqlite3.connect('NDOTDATA.db')
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
    conn = sqlite3.connect('NDOTDATA.db')
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO users (name, email, role, active_status)
            VALUES (?, ?, ?, ?)
        ''', (name, email, role, 0))
        conn.commit()
        st.success("User added successfully!")
    except sqlite3.IntegrityError:
        st.error("This email is already registered. Please use a different email.")
    
    conn.close()

def add_leave_to_db(user_id, leave_from, leave_to):
    conn = sqlite3.connect('NDOTDATA.db')
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
    conn = sqlite3.connect('NDOTDATA.db')
    leaves_df = pd.read_sql('''
        SELECT l.id, u.name, l.leave_from, l.leave_to 
        FROM leaves l 
        JOIN users u ON l.user_id = u.id
    ''', conn)
    conn.close()
    return leaves_df

# Function to fetch all users from the database
def fetch_users_from_db():
    conn = sqlite3.connect('NDOTDATA.db')
    users_df = pd.read_sql("SELECT id, name FROM users", conn)
    conn.close()
    return users_df

def add_or_update_holiday(holiday_name, holiday_date):
    conn = sqlite3.connect('NDOTDATA.db')
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
    conn = sqlite3.connect('NDOTDATA.db')
    holidays_df = pd.read_sql("SELECT * FROM holidays", conn)
    conn.close()
    return holidays_df

# Function to delete a user from the database
def delete_user_from_db(user_id):
    conn = sqlite3.connect('NDOTDATA.db')
    cursor = conn.cursor()
    
    cursor.execute('DELETE FROM users WHERE id = ?', (user_id,))
    conn.commit()
    conn.close()
    st.success(f"User with ID {user_id} has been deleted.")


# Function to add a new configuration to the database
def add_config_to_db(AnchorWgt, NonAnchorWgt, MiscWgt, AnchorMaxPoints, NonAnchorMaxPoints, EpicMinEffortPoints):
    conn = sqlite3.connect('NDOTDATA.db')
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
    conn = sqlite3.connect('NDOTDATA.db')
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
    conn = sqlite3.connect('NDOTDATA.db')
    
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
                month_calendar = month_calendar.replace(f">{day}<", f' style="background-color: #FFDDC1;">{day}<')  # Light green for weekends
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
    st.logo("neveda.png", size="medium")
    htmlstr = """
<div class='Apptitle'>
<span>SLI FORECASTING TOOL</span>
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
    if st.sidebar.button('📅 Leaves', key='leaves'):
        st.session_state.page = 'Leaves'
    if st.sidebar.button('👤 Users', key='users'):
        st.session_state.page = 'Users'
    if st.sidebar.button('🎄 Holidays', key='holiday'):
        st.session_state.page = 'Holidays'
    if st.sidebar.button('⚙️ Settings', key='settings'):
        st.session_state.page = 'Settings'
    # if st.sidebar.button("🔓 Logout", key='logout'):
    #     st.session_state.page = 'Logout'
        

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
        st.title("Forecast")
        mcol1, mcol2 = st.columns([1, 5])
        with mcol1:
            # Create the form inside a div with the custom class
            with st.form(key='config_form'):
                st.markdown('<div class="custom-form">', unsafe_allow_html=True)
                #st.write("Project Weightage")
                with st.container():
                    with st.popover("Type Weights"):
                    # Input fields will be stacked vertically inside the container
                        MiscWgt = st.number_input("Miscellaneous Weight", min_value=0.0, format="%.2f")
                        AnchorWgt = st.number_input("Anchor Weight", min_value=0.0, format="%.2f")
                        NonAnchorWgt = st.number_input("Non-Anchor Weight", min_value=0.0, format="%.2f", disabled=True)
                    st.divider()
                    with st.popover("Max Points "):                     
                        AnchorMaxPoints = st.number_input("Anchor Max Effort Points", min_value=0)
                        NonAnchorMaxPoints = st.number_input("Non-Anchor Effort Max Points", min_value=0)
                    st.divider()
                    with st.popover("Min Points "):   
                        EpicMinEffortPoints = st.number_input("Epic Min Effort Points", min_value=0)
                    
                # Submit button at the end
                st.divider()
                submit_button = st.form_submit_button(label="Forecast")
                st.markdown('</div>', unsafe_allow_html=True)
                if submit_button:
                    add_config_to_db(AnchorWgt, NonAnchorWgt, MiscWgt, AnchorMaxPoints, NonAnchorMaxPoints, EpicMinEffortPoints)
        # Create the form inside a div with the custom class
        with mcol2:
          #  st.write("Project Forecast")
            df = fetch_data_from_db("Projects")
            df = df[["Title", "State", "Anchor_Project", "Priority_Traffic_Ops","Fiscal_Year","Funding_Source","Route_Type","Scoping_30_Percent","SeventyFivePercentComplete","Intermediate_Date","QAQC_Submittal_Date","Document_Submittal_Date"]]
    



            tabs = st.tabs(["Projects"])
        
            with tabs[0]:
                # Create sample data with numbers and design descriptions
                data = {
                    "Sprint 17 2024": ["38241 (QAQC Design)", "62238 (Intermediate Design)", "46368 (QAQC Design)","62238 (Intermediate Design)", "46368 (QAQC Design)"],
                    "Sprint 18 2024": ["45020 (QAQC Design)", "45020 (Intermediate Design)", "45827 (Post-Doc Design)", "53336 (Post-Doc Design)", "48704 (Doc Design)"],
                    "Sprint 19 2024": ["62169 (Intermediate Design)", "19358 (Doc Design)", "55804 (Post-Doc Design)", "7647 (Intermediate Design)", "62239 (QAQC Design)"],
                    "Sprint 20 2024": ["19358 (Doc Design)", "62169 (QAQC Design)", "68958 (Post-Doc Design)","62238 (Intermediate Design)", "46368 (QAQC Design)"],
                    "Sprint 21 2024": ["62169 (QAQC Design)", "56358 (Doc Design)", "53336 (Post-Doc Design)", "7647 (Intermediate Design)", "62239 (QAQC Design)"],
                    "Sprint 22 2024": ["19358 (QAQC Design)", "53336 (Post-Doc Design)", "48704 (Doc Design)","62238 (Intermediate Design)", "46368 (QAQC Design)"],
                    "Sprint 23 2024": ["62169 (QAQC Design)", "56458 (Intermediate Design)", "7647 (Doc Design)", "7647 (Intermediate Design)", "62239 (QAQC Design)"],
                    "Sprint 24 2024": ["56358 (Doc Design)", "7647 (Intermediate Design)", "62239 (QAQC Design)","62238 (Intermediate Design)", "46368 (QAQC Design)"]
                }

                # Convert the data into a pandas DataFrame
                df = pd.DataFrame(data)

                df = df.reset_index(drop=True)

                # Function to apply colors based on conditions
                def highlight_cells(val):
                    if 'QAQC' in val:
                        color = '#dc6c55'  # Cells containing "QAQC" will be red
                    elif 'Intermediate' in val:
                        color = '#dbc53a'  # Cells containing "Intermediate" will be yellow
                    else:
                        color = ''  # Default color (no highlight)
                    return f'background-color: {color}'

                # Apply the styling
              #  styled_df = df.style.applymap(highlight_cells).hide(axis="index")

                # Display the styled dataframe in Streamlit
                st.dataframe(df)
                

                #st.write(grid_return)
        st.markdown(""" <style>
                    
                  .st-emotion-cache-1rsyhoq.e1nzilvr5 P {
                    color:#ADD8E6;
                  }
                    </style>
                    """, unsafe_allow_html=True)
                
        st.write("Epic Status")
        #data = fetch_data_from_db("Epics")     
        data = {
        "epic": ["Epic-001"],
        "projectid": ["Project-123"],
        "Effort points": [40],
        "DueDate": ["2024-10-30"],
        "Sprint date": ["2024-10-01"]
       } 
        df = pd.DataFrame(data)
        #grid_return = AgGrid(st.dataframe(data) ,enable_enterprise_modules=False) 
        st.dataframe(df)
    elif st.session_state.page == 'Dashboard':
        st.title("Dashboard")
        st.write("Welcome to the Dashboard!")

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
        #st.title("Leave Management")

        # Create a modal instance
        modal = Modal(key="add_leave_modal", title="Add Leave")

        # Add Leave button in the top-right corner
        add_leave_button = st.button("Add Leave", key="add_leave_button", help="Click to add leave")

        # Show the Total Leaves section
        st.write("### Total Leaves")

        # Fetch and display all leave records
        leaves_df = fetch_leaves_from_db()
        leaves_df = leaves_df[["name","leave_from","leave_to"]]
        if not leaves_df.empty:
            grid_return = AgGrid(leaves_df,fit_columns_on_grid_load=True )

        else:
            st.write("No leave records found.")

        # Trigger modal when Add Leave button is clicked
        if add_leave_button:
            modal.open()

        if modal.is_open():
            with modal.container():
                

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
    elif st.session_state.page == 'Users':
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
<span>User Management</span>
</div>
""" 
        st.html(htmluserstr)
        #st.title("User Management")
        tab1, tab2 = st.tabs(["Create User", "All Users"])

        with tab1:
            st.form
            # Create a form for user creation
            with st.form(key='user_form'):
                name = st.text_input("Name", max_chars=50)
                email = st.text_input("Email", max_chars=100)
                role = st.selectbox("Role", ["Admin", "Viewer", "Editor"])
                #active_status = st.checkbox("Active Status", value=True)
                
                # Submit button
                submit_button = st.form_submit_button(label="Create User")
                
                if submit_button:
                    add_user_to_db(name, email, role)
        with tab2:   
        # Display existing users
            conn = sqlite3.connect('NDOTDATA.db')
            users = pd.read_sql("SELECT * FROM users", conn)
            conn.close()
            
            st.subheader("Existing Users")
            
            if not users.empty:
                for index, row in users.iterrows():
                    col1, col2, col3, col4, col5 = st.columns([2, 3, 3, 2, 1])
                    
                    # # Display the user information
                    # col1.write(row["id"])
                    col2.write(row["name"])
                    col3.write(row["email"])
                    col4.write(row["role"])
                    
                    delete_button = col5.button("Del", key=row["id"])
                    if delete_button:
                        delete_user_from_db(row["id"])
                        st.rerun()  # Refresh the app after deletion

            else:
                st.write("No users found.")

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
                    #st.rerun()

        # Tab 3: Holiday List
        with tab1:
            st.write("### List of All Holidays")
            holidays_df = holidays_df[["holiday_name","holiday_date"]]
            # Fetch and display all holidays
            if not holidays_df.empty:
                grid_return = AgGrid(holidays_df,fit_columns_on_grid_load=True)
                
            else:
                st.write("No holidays found.")

    elif st.session_state.page == 'Settings':
        st.title("Settings")
        st.write("Manage application settings.")
    # elif st.session_state.page == 'Logout':
    #     st.session_state['logged_in'] = False
        
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

    # if st.session_state.page == "Leaves":
    #     st.title("User Registration")
        
    #     # Create a form for user creation
    #     with st.form(key='user_form'):
    #         name = st.text_input("Name", max_chars=50)
    #         email = st.text_input("Email", max_chars=100)
    #         role = st.selectbox("Role", ["Admin", "Viewer", "Editor"])
    #         active_status = st.checkbox("Active Status", value=True)
            
    #         # Submit button
    #         submit_button = st.form_submit_button(label="Create User")
            
    #         if submit_button:
    #             add_user_to_db(name, email, role, active_status)
        
    #     # Display existing users
    #     conn = sqlite3.connect('NDOTDATA.db')
    #     users = pd.read_sql("SELECT * FROM users", conn)
    #     conn.close()
        
    #     st.subheader("Existing Users")
        
    #     if not users.empty:
    #         for index, row in users.iterrows():
    #             col1, col2, col3, col4, col5 = st.columns([2, 3, 3, 2, 1])
                
    #             # Display the user information
    #             col1.write(row["id"])
    #             col2.write(row["name"])
    #             col3.write(row["email"])
    #             col4.write(row["role"])
                
    #             delete_button = col5.button("Del", key=row["id"])
    #             if delete_button:
    #                 delete_user_from_db(row["id"])
    #                 st.experimental_rerun()  # Refresh the app after deletion

    #     else:
    #         st.write("No users found.")
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
    #     st.write("Product Backlog Items joined with Features, Epics, and Projects:"n
    #     st.dataframe(filtered_df)


# Check if the user is logged in
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False

# Display login screen or main screen based on login status
if not st.session_state['logged_in']:
    login_screen()
else:
    main_application()

import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, timedelta

# Function to read data from the SQLite database
def load_data():
    conn = sqlite3.connect(':memory:')
    sheet1_data.to_sql('Sheet1', conn, index=False, if_exists='replace')
    sprint_dates_data.to_sql('upcoming_sprint_dates', conn, index=False, if_exists='replace')
    
    # Add Active field to resource leaves data
    resource_leaves_data['Active'] = 'Active'
    resource_leaves_data.to_sql('resource_leaves', conn, index=False, if_exists='replace')
    
    holidays_data.to_sql('holidays', conn, index=False, if_exists='replace')
    return conn

# Load the data
file_path = 'Updated_Forecast_19_Random.xlsx'
sheet1_data = pd.read_excel(file_path, sheet_name='Sheet1')
sprint_dates_data = pd.read_excel(file_path, sheet_name='upcoming sprint dates')
resource_leaves_data = pd.read_excel(file_path, sheet_name='Resource leaves')
holidays_data = pd.read_excel(file_path, sheet_name='Holidays')

conn = load_data()

# Initialize configuration values
if 'config' not in st.session_state:
    st.session_state.config = {
        'total_effort_points': 55,
        'miscellaneous_tasks_percentage': 0.05,
        'anchor_percentage': 0.85,
        'non_anchor_percentage': 0.15,
        'max_effort_per_sprint': 20
    }

# Streamlit app
st.title('Project Data Overview')

# Sidebar for navigation
menu = ["Full Data", "Default View", "Anchor Projects", "Non-Anchor Projects", "Resource Leaves", "Holidays", "Forecast", "Configuration"]
choice = st.sidebar.selectbox("Menu", menu)

if choice == "Full Data":
    st.subheader("Full Data")
    data = pd.read_sql_query('SELECT * FROM Sheet1', conn)
    st.write(f"Number of records: {len(data)}")
    st.dataframe(data)
    
elif choice == "Default View":
    st.subheader("Default View with Filters")
    data = pd.read_sql_query('SELECT * FROM Sheet1', conn)
    
    # Create filters
    project_state = st.sidebar.multiselect("Select Project State", options=data['Project_state'].unique())
    project_title = st.sidebar.multiselect("Select Project Title", options=data['Project_title'].unique())
    epic_title = st.sidebar.multiselect("Select Epic Title", options=data['Epic_Title'].unique())
    project_assigned_to = st.sidebar.multiselect("Select Project Assigned To", options=data['Project_Assigned_To'].unique())
    iteration_path = st.sidebar.multiselect("Select Iteration Path", options=data['Iteration Path'].unique())
    pbi_state = st.sidebar.multiselect("Select PBI State", options=data['PBI_State'].unique())
    pbi_assigned_to = st.sidebar.multiselect("Select PBI Assigned To", options=data['PBI_Assigned_To'].unique())
    
    # Apply filters
    filtered_data = data.copy()
    if project_state:
        filtered_data = filtered_data[filtered_data['Project_state'].isin(project_state)]
    if project_title:
        filtered_data = filtered_data[filtered_data['Project_title'].isin(project_title)]
    if epic_title:
        filtered_data = filtered_data[filtered_data['Epic_Title'].isin(epic_title)]
    if project_assigned_to:
        filtered_data = filtered_data[filtered_data['Project_Assigned_To'].isin(project_assigned_to)]
    if iteration_path:
        filtered_data = filtered_data[filtered_data['Iteration Path'].isin(iteration_path)]
    if pbi_state:
        filtered_data = filtered_data[filtered_data['PBI_State'].isin(pbi_state)]
    if pbi_assigned_to:
        filtered_data = filtered_data[filtered_data['PBI_Assigned_To'].isin(pbi_assigned_to)]
    
    st.write(f"Number of records: {len(filtered_data)}")
    st.dataframe(filtered_data)
    
elif choice == "Anchor Projects":
    st.subheader("Anchor Projects")
    data = pd.read_sql_query('SELECT * FROM Sheet1 WHERE [Anchor Project] = 1', conn)
    
    # Filter for due dates greater than today's date
    today = datetime.today().strftime('%Y-%m-%d')
    data = data[data['Due_Date'] > today]
    
    # Sort by due date
    data = data.sort_values(by='Due_Date')
    
    st.write(f"Number of records: {len(data)}")
    st.dataframe(data)
    
elif choice == "Non-Anchor Projects":
    st.subheader("Non-Anchor Projects")
    data = pd.read_sql_query('SELECT * FROM Sheet1 WHERE [Anchor Project] = 0', conn)
    
    # Filter for due dates greater than today's date
    today = datetime.today().strftime('%Y-%m-%d')
    data = data[data['Due_Date'] > today]
    
    # Sort by due date
    data = data.sort_values(by='Due_Date')
    
    st.write(f"Number of records: {len(data)}")
    st.dataframe(data)
    
elif choice == "Resource Leaves":
    st.subheader("Resource Leaves")
    data = pd.read_sql_query('SELECT * FROM resource_leaves', conn)
    
    # Allow user to edit data
    edited_data = st.experimental_data_editor(data, num_rows='dynamic')
    
    # Save edited data back to the SQLite database
    if st.button("Save Changes"):
        edited_data.to_sql('resource_leaves', conn, index=False, if_exists='replace')
        st.success("Changes saved successfully.")
    
elif choice == "Holidays":
    st.subheader("Holidays")
    data = pd.read_sql_query('SELECT * FROM holidays', conn)
    
    # Allow user to edit data
    edited_data = st.experimental_data_editor(data, num_rows='dynamic')
    
    # Save edited data back to the SQLite database
    if st.button("Save Changes"):
        edited_data.to_sql('holidays', conn, index=False, if_exists='replace')
        st.success("Changes saved successfully.")

elif choice == "Forecast":
    st.subheader("Forecast")

    # Step 1: Load data and preprocess
    projects = pd.read_sql_query('SELECT * FROM Sheet1', conn)
    sprints = pd.read_sql_query('SELECT * FROM upcoming_sprint_dates', conn)
    holidays = pd.read_sql_query('SELECT * FROM holidays', conn)
    resource_leaves = pd.read_sql_query('SELECT * FROM resource_leaves', conn)

    # Filter out projects with empty due dates
    projects = projects[projects['Due_Date'].notna()]

    # Step 2: Implement priority calculation
    projects['Priority'] = projects['Due_Date'].apply(lambda x: (pd.to_datetime(x) - datetime.now()).days)
    projects['Priority'] = projects.apply(lambda x: x['Priority'] if x['Anchor Project'] == 1 else x['Priority'] + 1000, axis=1)
    projects = projects.sort_values(by='Priority')

    # Step 3: Effort point allocation and sprint planning
    config = st.session_state.config

    total_effort_points = config['total_effort_points']
    miscellaneous_tasks_percentage = config['miscellaneous_tasks_percentage']
    anchor_percentage = config['anchor_percentage']
    non_anchor_percentage = config['non_anchor_percentage']
    max_effort_per_sprint = config['max_effort_per_sprint']

    miscellaneous_tasks_points = total_effort_points * miscellaneous_tasks_percentage
    remaining_points = total_effort_points - miscellaneous_tasks_points

    anchor_points = remaining_points * anchor_percentage
    non_anchor_points = remaining_points * non_anchor_percentage

    # Calculate effort points required per sprint
    def calculate_effort_per_sprint(row):
        due_date = pd.to_datetime(row['Due_Date'])
        days_left = (due_date - datetime.now()).days
        sprints_left = days_left // 14  # Assuming 2-week sprints
        if sprints_left > 0:
            return row['Epic_TotalEfforts'] / sprints_left
        else:
            return row['Epic_TotalEfforts']

    projects['Effort_Per_Sprint'] = projects.apply(calculate_effort_per_sprint, axis=1)
    
    # Allocate effort points
    forecast = []
    for sprint in sprints['Sprint']:
        sprint_data = {'Sprint': sprint, 'Miscellaneous': miscellaneous_tasks_points, 'Anchor': 0, 'Non-Anchor': 0}
        
        for _, project in projects.iterrows():
            if project['Anchor Project'] == 1:
                allocated_points = min(anchor_points, project['Effort_Per_Sprint'])
                sprint_data['Anchor'] += allocated_points
                anchor_points -= allocated_points
            else:
                allocated_points = min(non_anchor_points, project['Effort_Per_Sprint'])
                sprint_data['Non-Anchor'] += allocated_points
                non_anchor_points -= allocated_points
        
        forecast.append(sprint_data)
    
    forecast_df = pd.DataFrame(forecast)
    st.dataframe(forecast_df)

elif choice == "Configuration":
    st.subheader("Configuration")
    config = st.session_state.config

    # Configurable parameters
    config['total_effort_points'] = st.number_input("Total Effort Points per Sprint", value=config['total_effort_points'])
   # config['miscellaneous_tasks_percentage'] = st.number_input("Miscellaneous Tasks Percentage", value=config['miscellaneous_tasks_percentage'])

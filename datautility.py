import pandas as pd
import sqlite3
from datetime import datetime, timedelta


# Connect to the SQLite database
db_path = 'NDOTDATA.db'

def calculate_days_overlap_exclude_weekends(start1, end1, start2, end2):
    """Calculate the number of overlapping weekdays (excluding Saturdays and Sundays) between two date ranges."""
    start_date = max(start1, start2)
    end_date = min(end1, end2)
    
    overlap_days = 0

    # Loop through each day in the overlap period
    current_date = start_date
    while current_date <= end_date:
        # Check if the day is not Saturday (5) or Sunday (6)
        if current_date.weekday() < 5:  # 0 = Monday, ..., 4 = Friday
            overlap_days += 1
        current_date += timedelta(days=1)

    return overlap_days

def add_nearest_date_column(df):
    # Ensure all date columns are properly formatted as datetime objects
    date_columns = ['projects_Scoping_30_Percent', 
                    'projects_SeventyFivePercentComplete', 
                    'projects_Intermediate_Date', 
                    'projects_QAQC_Submittal_Date', 
                    'projects_Document_Submittal_Date']

    # Convert these columns to datetime, ignoring errors to prevent issues with invalid dates
    df[date_columns] = df[date_columns].apply(pd.to_datetime, errors='coerce')

    # Create a new column 'projects_Nearest_Date' which is the earliest date from the specified columns
    df['projects_Nearest_Date'] = df[date_columns].min(axis=1)
    print(df)
    return df

def sort_projects_dataframe(df):
    # Define the order for the 'projects_State' column (Actively working and Approved will be higher priority)
    state_priority = pd.CategoricalDtype(categories=["Actively working", "Approved", "On Hold"], ordered=True)
    df['projects_State'] = df['projects_State'].astype(state_priority)

    # Define the order for the 'projects_Funding_Source' column
    funding_source_priority = pd.CategoricalDtype(categories=["Federal", "Other", "State"], ordered=True)
    df['projects_Funding_Source'] = df['projects_Funding_Source'].astype(funding_source_priority)

    # Define the order for the 'projects_Route_Type' column
    route_type_priority = pd.CategoricalDtype(categories=["Interstate", "Highway", "State route", "Arterial / local", "Other"], ordered=True)
    df['projects_Route_Type'] = df['projects_Route_Type'].astype(route_type_priority)

    # Ensure the specified columns are numeric (in case there are any non-numeric values)
    df[['projects_Complexity_Signals', 'projects_Complexity_Lighting', 'projects_Complexity_ITS', 
        'projects_Complexity_Power_Design', 'projects_Complexity_RoW_Coordination', 
        'projects_Complexity_SLI_Project_Lead', 'projects_Complexity_Solar_Design', 
        'projects_Complexity_Trunkline']] = df[['projects_Complexity_Signals', 'projects_Complexity_Lighting', 'projects_Complexity_ITS', 
        'projects_Complexity_Power_Design', 'projects_Complexity_RoW_Coordination', 
        'projects_Complexity_SLI_Project_Lead', 'projects_Complexity_Solar_Design', 
        'projects_Complexity_Trunkline']].apply(
        pd.to_numeric, errors='coerce')

    # Create a new column 'projects_complexity' that is the sum of the specified columns
    df['projects_complexity'] = df[['projects_Complexity_Signals', 'projects_Complexity_Lighting', 
                                    'projects_Complexity_ITS', 'projects_Complexity_Power_Design', 
                                    'projects_Complexity_RoW_Coordination', 'projects_Complexity_SLI_Project_Lead', 
                                    'projects_Complexity_Solar_Design', 'projects_Complexity_Trunkline']].sum(axis=1)
    


    # Sort the dataframe based on multiple criteria including projects_complexity
    sorted_df = df.sort_values(by=[
        'projects_State',          # First priority: Project State
        'projects_Funding_Source', # Second priority: Funding Source
        'projects_Fiscal_Year',    # Third priority: Fiscal Year (ascending, soonest first)
        'projects_Nearest_Date',   # Fourth priority: Nearest Date (soonest first)
        'projects_Priority_Traffic_Ops',    # Fifth priority: Traffic Ops (descending, higher numbers first)
        'projects_Route_Type',     # Sixth priority: Route Type
        'projects_complexity'      # Seventh priority: Project Complexity (descending, higher complexity first)
    ], ascending=[True, True, True, True, False, True, False])  # Adjusting the sorting order for each column

    return sorted_df


def allocate_epics_to_sprints(sprint_data, anchor_projects, non_anchor_projects):
    # Group anchor and non-anchor projects by 'epics_System_Id' and sum the efforts for each group
    grouped_anchor_projects = anchor_projects.groupby(['epics_System_Id', 'projects_Work_Item_ID', 'features_System_Title']).agg({
        'pbis_Microsoft_VSTS_Scheduling_Effort': 'sum'
    }).reset_index()

    grouped_non_anchor_projects = non_anchor_projects.groupby(['epics_System_Id', 'projects_Work_Item_ID', 'features_System_Title']).agg({
        'pbis_Microsoft_VSTS_Scheduling_Effort': 'sum'
    }).reset_index()
    # Convert the 'projects_Work_Item_ID' to integer to avoid float issues
    grouped_anchor_projects['projects_Work_Item_ID'] = grouped_anchor_projects['projects_Work_Item_ID'].astype(int)
    grouped_non_anchor_projects['projects_Work_Item_ID'] = grouped_non_anchor_projects['projects_Work_Item_ID'].astype(int)

    # Prepare a dictionary to hold allocation data for wide format
    allocation_dict = {}

    for idx, sprint in sprint_data.iterrows():
        sprint_name = sprint['Iteration']
        anchor_effort_remaining = sprint['AnchorEffortPoints']
        non_anchor_effort_remaining = sprint['NonAnchorEffortPoints']

        # Allocate anchor epics
        allocated_projects = []
        for _, epic in grouped_anchor_projects.iterrows():
            effort = epic['pbis_Microsoft_VSTS_Scheduling_Effort']
            if effort <= anchor_effort_remaining:
                allocated_projects.append(f"{epic['projects_Work_Item_ID']}-{epic['features_System_Title']}-{effort}")
                anchor_effort_remaining -= effort
            else:
                # Partial allocation if effort exceeds remaining capacity
                allocated_projects.append(f"{epic['projects_Work_Item_ID']}-{epic['features_System_Title']}-{anchor_effort_remaining}")
                epic['pbis_Microsoft_VSTS_Scheduling_Effort'] = effort - anchor_effort_remaining
                anchor_effort_remaining = 0
                break

        # Allocate non-anchor epics
        for _, epic in grouped_non_anchor_projects.iterrows():
            effort = epic['pbis_Microsoft_VSTS_Scheduling_Effort']
            if effort <= non_anchor_effort_remaining:
                allocated_projects.append(f"{epic['projects_Work_Item_ID']}-{epic['features_System_Title']}-{effort}")
                non_anchor_effort_remaining -= effort
            else:
                # Partial allocation if effort exceeds remaining capacity
                allocated_projects.append(f"{epic['projects_Work_Item_ID']}-{epic['features_System_Title']}-{non_anchor_effort_remaining}")
                epic['pbis_Microsoft_VSTS_Scheduling_Effort'] = effort - non_anchor_effort_remaining
                non_anchor_effort_remaining = 0
                break

        # Add allocated projects to the sprint's column
        allocation_dict[sprint_name] = allocated_projects

    # Convert allocation dictionary to a DataFrame, aligning rows
    max_len = max([len(v) for v in allocation_dict.values()])  # Find the maximum number of projects allocated in any sprint
    for sprint in allocation_dict:
        # Extend lists to the max length to align columns
        allocation_dict[sprint] += [None] * (max_len - len(allocation_dict[sprint]))

    # Convert the dictionary to a DataFrame for display
    allocation_df = pd.DataFrame(allocation_dict)

    return allocation_df



def get_project_data():
    # Define the SQL queries to select only the required columns
    projects_query = """
    SELECT Work_Item_ID, Iteration_Path, Work_Item_Type, State, Scoping_30_Percent, SeventyFivePercentComplete, 
    Intermediate_Date, QAQC_Submittal_Date, Document_Submittal_Date, Priority_Traffic_Ops, Fiscal_Year, Funding_Source, 
    Route_Type, Construction_EA_Number, Official_DOC_Date, Anchor_Project, Complexity_Signals, Complexity_Lighting, 
    Complexity_ITS, Complexity_Power_Design, Complexity_RoW_Coordination, Complexity_SLI_Project_Lead, 
    Complexity_Solar_Design, Complexity_Trunkline FROM projects
    WHERE State IN ('Actively Working', 'Approved', 'On-Hold');
    """
    
    epics_query = """
    SELECT System_Id, System_Title, System_IterationPath, System_WorkItemType, System_Parent FROM epics;
    """

    features_query = """
    SELECT system_Id, System_Title, System_Parent FROM features;
    """

    productbacklogitems_query = """
    SELECT System_Id, System_IterationPath, System_WorkItemType, System_State, System_Parent, System_Title, 
    Microsoft_VSTS_Scheduling_Effort FROM productbacklogitems;
    """

      # Update with the actual path to your SQLite database
    conn = sqlite3.connect(db_path)

    # Read the data from the tables into DataFrames
    projects_df = pd.read_sql_query(projects_query, conn)
    projects_df.columns = 'projects_' + projects_df.columns.values
    epics_df = pd.read_sql_query(epics_query, conn)
    epics_df.columns = 'epics_' + epics_df.columns.values
    features_df = pd.read_sql_query(features_query, conn)
    features_df.columns = 'features_' + features_df.columns.values
    productbacklogitems_df = pd.read_sql_query(productbacklogitems_query, conn)
    productbacklogitems_df.columns = 'pbis_' + productbacklogitems_df.columns.values
   

    # Close the database connection
    conn.close()

    # Perform a LEFT JOIN of productbacklogitems with features, epics, and projects
    merged_df = productbacklogitems_df.merge(features_df, left_on='pbis_System_Parent', right_on='features_system_Id', how='left')  \
    .merge(epics_df, left_on='features_System_Parent', right_on='epics_System_Id', how='left') \
    .merge(projects_df, left_on='epics_System_Parent', right_on='projects_Work_Item_ID', how='left')
     # Ensure the specified columns are numeric (in case there are any non-numeric values)
    merged_df[['projects_Complexity_Signals', 'projects_Complexity_Lighting', 'projects_Complexity_ITS', 
            'projects_Complexity_Power_Design', 'projects_Complexity_RoW_Coordination', 
            'projects_Complexity_SLI_Project_Lead', 'projects_Complexity_Solar_Design', 
            'projects_Complexity_Trunkline']] = merged_df[['projects_Complexity_Signals', 'projects_Complexity_Lighting', 'projects_Complexity_ITS', 
            'projects_Complexity_Power_Design', 'projects_Complexity_RoW_Coordination', 
            'projects_Complexity_SLI_Project_Lead', 'projects_Complexity_Solar_Design', 
            'projects_Complexity_Trunkline']].apply(
        pd.to_numeric, errors='coerce')

    # Create a new column 'projects_complexity' that is the sum of the specified columns
    merged_df['projects_complexity'] = merged_df[['projects_Complexity_Signals', 'projects_Complexity_Lighting', 
                                                'projects_Complexity_ITS', 'projects_Complexity_Power_Design', 
                                                'projects_Complexity_RoW_Coordination', 'projects_Complexity_SLI_Project_Lead', 
                                                'projects_Complexity_Solar_Design', 'projects_Complexity_Trunkline']].sum(axis=1)
    merged_df = add_nearest_date_column(merged_df)
    anchor_project_df = merged_df[merged_df['projects_Anchor_Project'].notna()]

    # DataFrame with rows where Anchor_Project is null/empty
    non_anchor_project_df = merged_df[merged_df['projects_Anchor_Project'].isna()]

    print(get_upcoming_sprints_with_effortpoints_and_weightage())
    return sort_projects_dataframe(anchor_project_df), sort_projects_dataframe(non_anchor_project_df)


# def get_upcoming_sprint():
#      # Connect to the SQLite database
#     conn = sqlite3.connect(db_path)
#     cursor = conn.cursor()

#     # Get today's date in 'YYYY-MM-DD' format
#     today = datetime.now().strftime('%Y-%m-%d')

#     # Query to get all upcoming sprints (where start date is greater than today's date)
#     cursor.execute('''
#         SELECT Iteration, Start_date, End_date 
#         FROM iterations 
#         WHERE Start_date > ? 
#         ORDER BY Start_date ASC
#     ''', (today,))

#     # Fetch all results
#     upcoming_sprints = cursor.fetchall()

#     # Convert the fetched results into a pandas DataFrame
#     df = pd.DataFrame(upcoming_sprints, columns=['Iteration', 'Start_date', 'End_date'])

#     # Add a column for holidays count
#     df['Holidays_Count'] =  0

#      # Add a column for holidays count
#     Effort_points_per_user =  10

#     resource_count = get_usercount()


#     # For each sprint, calculate the number of holidays that fall within the start and end dates
#     for i, row in df.iterrows():
#         sprint_start = datetime.strptime(row['Start_date'], '%Y-%m-%d')
#         sprint_end = datetime.strptime(row['End_date'], '%Y-%m-%d')
#         cursor.execute('''
#             SELECT COUNT(*)
#             FROM holidays
#             WHERE Holiday_date BETWEEN ? AND ?
#         ''', (row['Start_date'], row['End_date']))
        
#         # Fetch the count of holidays and assign to the new column in the DataFrame
#         holidays_count = cursor.fetchone()[0]
#         df.at[i, 'Holidays_Count'] = holidays_count

#         df['resource_count']  = get_usercount()
#         cursor.execute('''
#             SELECT leave_from, leave_to
#             FROM leaves
#             WHERE leave_from <= ? AND leave_to >= ?
#         ''', (row['End_date'], row['Start_date']))

#         total_leave_days = 0
#         leave_periods = cursor.fetchall()

#         # Loop through each leave period and calculate the overlap with the sprint, excluding weekends
#         for leave_from, leave_to in leave_periods:
#             leave_from_date = datetime.strptime(leave_from, '%Y-%m-%d')
#             leave_to_date = datetime.strptime(leave_to, '%Y-%m-%d')

#             # Calculate the overlapping days between the leave period and the sprint period, excluding weekends
#             overlap_days = calculate_days_overlap_exclude_weekends(leave_from_date, leave_to_date, sprint_start, sprint_end)
#             total_leave_days += overlap_days

#         df.at[i, 'Leave_Days'] = total_leave_days

#     # Close the connection
#     conn.close()
#     df['Total_Effort_points'] = (resource_count* Effort_points_per_user)- (resource_count*df['Holidays_Count']) - df['Leave_Days']
#     print(df)
#     return df

def get_upcoming_sprints_with_effortpoints_and_weightage():
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get today's date in 'YYYY-MM-DD' format
    today = datetime.now().strftime('%Y-%m-%d')

    # Query to get all upcoming sprints (where start date is greater than today's date)
    cursor.execute('''
        SELECT Iteration, Start_date, End_date 
        FROM iterations 
        WHERE Start_date > ? 
        ORDER BY Start_date ASC
    ''', (today,))

    # Fetch all results
    upcoming_sprints = cursor.fetchall()

    # Convert the fetched results into a pandas DataFrame
    df = pd.DataFrame(upcoming_sprints, columns=['Iteration', 'Start_date', 'End_date'])

    # Add columns for holidays count, leave count, effort points per user, resource count, total effort points, and weightage points
    df['Holidays_Count'] = 0
    df['Effort_points_per_user'] = 10  # Each user gets 10 effort points per sprint
    df['resource_count'] = get_usercount()  # Assuming get_usercount() returns the number of users
    df['Leave_Days'] = 0  # Initialize Leave_Days
    df['TotalEffortpoints'] = 0  # Initialize TotalEffortpoints
    df['MiscEffortPoints'] = 0  # Initialize Misc Effort Points
    df['AnchorEffortPoints'] = 0  # Initialize Anchor Effort Points
    df['NonAnchorEffortPoints'] = 0  # Initialize Non-Anchor Effort Points

    # Fetch weightage configuration for MiscWgt, AnchorWgt, and NonAnchorWgt
    # Fetch all columns from weightageconfig table
    cursor.execute('SELECT * FROM weightageconfig LIMIT 1')
    weightageconfig = cursor.fetchone()
    miscwgt = weightageconfig[0]
    anchorwgt = weightageconfig[1]
    nonanchorwgt = weightageconfig[2]
    anchorMaxPoints = weightageconfig[3]
    nonAnchorMaxPoints = weightageconfig[4]
    epicMinEffortPoints = weightageconfig[5]
    

    # For each sprint, calculate the number of holidays and leaves that fall within the start and end dates
    for i, row in df.iterrows():
        sprint_start = datetime.strptime(row['Start_date'], '%Y-%m-%d')
        sprint_end = datetime.strptime(row['End_date'], '%Y-%m-%d')

        # Count holidays between sprint start and end date
        cursor.execute('''
            SELECT COUNT(*)
            FROM holidays
            WHERE Holiday_date BETWEEN ? AND ?
        ''', (row['Start_date'], row['End_date']))
        
        holidays_count = cursor.fetchone()[0]
        df.at[i, 'Holidays_Count'] = holidays_count

        # Calculate total leave days that overlap with the sprint, excluding weekends
        cursor.execute('''
            SELECT leave_from, leave_to
            FROM leaves
            WHERE leave_from <= ? AND leave_to >= ?
        ''', (row['End_date'], row['Start_date']))

        total_leave_days = 0
        leave_periods = cursor.fetchall()

        # Loop through each leave period and calculate the overlap with the sprint, excluding weekends
        for leave_from, leave_to in leave_periods:
            leave_from_date = datetime.strptime(leave_from, '%Y-%m-%d')
            leave_to_date = datetime.strptime(leave_to, '%Y-%m-%d')

            # Calculate the overlapping days between the leave period and the sprint period, excluding weekends
            overlap_days = calculate_days_overlap_exclude_weekends(leave_from_date, leave_to_date, sprint_start, sprint_end)
            total_leave_days += overlap_days

        df.at[i, 'Leave_Days'] = total_leave_days

        # Calculate TotalEffortpoints based on the formula
        resource_count = df.at[i, 'resource_count']
        effort_points_per_user = df.at[i, 'Effort_points_per_user']
        holidays_count = df.at[i, 'Holidays_Count']
        leave_days = df.at[i, 'Leave_Days']

        total_effort_points = (resource_count * effort_points_per_user) - (resource_count * holidays_count) - leave_days
        df.at[i, 'TotalEffortpoints'] = total_effort_points

        # Calculate Misc Effort Points
        misceffortpoints = round((miscwgt / 100) * total_effort_points)
        df.at[i, 'MiscEffortPoints'] = misceffortpoints

        # Calculate Remaining Effort Points after Misc Effort Points
        remaining_effort_points = total_effort_points - misceffortpoints

        # Calculate Anchor Effort Points and Non-Anchor Effort Points based on remaining effort points
        anchoreffortpoints = round((anchorwgt / 100) * remaining_effort_points)
        nonanchoreffortpoints = round((nonanchorwgt / 100) * remaining_effort_points)

        df.at[i, 'AnchorEffortPoints'] = anchoreffortpoints
        df.at[i, 'NonAnchorEffortPoints'] = nonanchoreffortpoints

    # Close the connection
    conn.close()

    return df

def get_usercount():
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    
    # Query to count the number of users in the 'users' table
    query_users_count = "SELECT COUNT(*) FROM users;"
    cursor = conn.cursor()
    cursor.execute(query_users_count)
    
    # Fetch the number of users
    number_of_users = cursor.fetchone()[0]
    # Close the database connection
    conn.close()
    return number_of_users
 

 # Function to fetch the latest weightage configuration from the database
def fetch_latest_config():
    conn = sqlite3.connect('NDOTDATA.db')
    query = '''
        SELECT AnchorWgt, NonAnchorWgt, MiscWgt, AnchorMaxPoints, NonAnchorMaxPoints, EpicMinEffortPoints 
        FROM weightageconfig
        ORDER BY modifiedtime DESC LIMIT 1
    '''
    config = pd.read_sql(query, conn)
    conn.close()
    return config.iloc[0] if not config.empty else None
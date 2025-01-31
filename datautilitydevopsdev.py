import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import numpy as np
import os
import streamlit as st
from dotenv import load_dotenv
load_dotenv()

db_path = os.getenv('DB_PATH')

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


def sort_projects_dataframe(df):
 
    # Convert columns to lowercase for case-insensitive sorting
    df['projects_State'] = df['projects_State'].str.lower()
    df['projects_Funding_Source'] = df['projects_Funding_Source'].str.lower()
    df['projects_Route_Type'] = df['projects_Route_Type'].str.lower()

    # Define the order for the 'projects_State' column
    state_priority = pd.CategoricalDtype(categories=["actively working", "approved", "on-hold"], ordered=True)
    df['projects_State'] = df['projects_State'].astype(state_priority)

    # Define the order for the 'projects_Funding_Source' column
    funding_source_priority = pd.CategoricalDtype(categories=["federal", "other", "state"], ordered=True)
    df['projects_Funding_Source'] = df['projects_Funding_Source'].astype(funding_source_priority)

    # Define the order for the 'projects_Route_Type' column
    route_type_priority = pd.CategoricalDtype(categories=["interstate", "highway", "state route", "arterial / local", "other"], ordered=True)
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
        'projects_Document_Submittal_Date',
        'nearest_doc_date',   # Fourth priority: Nearest Date (soonest first)
        'projects_Priority_Traffic_Ops',    # Fifth priority: Traffic Ops (descending, higher numbers first)
        'projects_Route_Type',     # Sixth priority: Route Type
        'projects_complexity'      # Seventh priority: Project Complexity (descending, higher complexity first)
    ], ascending=[True, True, True, True, True, False, True, False])  # Adjusting the sorting order for each column


    return sorted_df


def distribute_epics_to_sprints(anchor_projects_df, non_anchor_projects_df, upcoming_sprints_df):
    # Initialize dictionary to store sprint allocations
    sprint_allocations = {
        sprint: {'anchor': [], 'non_anchor': [], 'remaining_anchor_effort': 0, 'remaining_non_anchor_effort': 0}
        for sprint in upcoming_sprints_df['Iteration']
    }

    # Convert date columns to datetime
    anchor_projects_df['nearest_doc_date'] = pd.to_datetime(anchor_projects_df['nearest_doc_date'], errors='coerce').dt.tz_localize(None)
    non_anchor_projects_df['nearest_doc_date'] = pd.to_datetime(non_anchor_projects_df['nearest_doc_date'], errors='coerce').dt.tz_localize(None)
    upcoming_sprints_df['Start_date'] = pd.to_datetime(upcoming_sprints_df['Start_date'], errors='coerce').dt.tz_localize(None)
    upcoming_sprints_df['End_date'] = pd.to_datetime(upcoming_sprints_df['End_date'], errors='coerce').dt.tz_localize(None)

    # Initialize remaining capacity for each sprint
    for _, sprint in upcoming_sprints_df.iterrows():
        sprint_name = sprint['Iteration']
        sprint_allocations[sprint_name]['remaining_anchor_effort'] = sprint['AnchorEffortPoints']
        sprint_allocations[sprint_name]['remaining_non_anchor_effort'] = sprint['NonAnchorEffortPoints']

    def allocate_epics(projects_df, project_type):
        max_effort_column = 'MaxAnchorEffortPointspersprint' if project_type == 'anchor' else 'MaxNonAnchorEffortPointspersprint'
        last_sprint_end_date = upcoming_sprints_df['End_date'].max()
        today = datetime.today()
        six_months_later = today + timedelta(days=180)
        
        # Process each project
        for project_id, project_group in projects_df.groupby('projects_Work_Item_ID', sort=False):
            epics = project_group.to_dict('records')
            used_sprints = set()  # To track sprints used for this project

            # Allocate each epic
            for epic in epics:
                remaining_effort = epic['total_effort_from_pbis']
                epic_title = epic['epics_System_Title']
                total_effort = epic['total_effort_from_pbis']
                #Use nearest_doc_date or None
                nearest_due_date = pd.to_datetime(epic['nearest_doc_date']) if not pd.isnull(epic['nearest_doc_date']) else last_sprint_end_date
                # Find the first available sprint that can accommodate the epic
                # Allocate effort across sprints
                #is_far_nearest_date = nearest_due_date and nearest_due_date > six_months_later
                
                 
                # Filter sprints eligible for the epic
                eligible_sprints_df = upcoming_sprints_df[upcoming_sprints_df['End_date'] < nearest_due_date] if nearest_due_date else upcoming_sprints_df
                num_eligible_sprints = len(eligible_sprints_df)

                # Minimum Epic Points Check
                minimum_epic_points = upcoming_sprints_df['minimumEpicPoints'].iloc[0]
                if total_effort < minimum_epic_points:
                    # Allocate to the last eligible sprint near the nearest due date
                    if num_eligible_sprints > 0:
                        last_sprint = eligible_sprints_df.iloc[-1]
                        sprint_name = last_sprint['Iteration']

                        # Fetch the remaining capacity and max effort constraints for the sprint
                        remaining_sprint_capacity = sprint_allocations[sprint_name][f'remaining_{project_type}_effort']
                        max_effort_per_sprint = last_sprint[max_effort_column]

                        # Calculate the actual effort to allocate (respecting constraints)
                        allocated_effort = min(total_effort, remaining_sprint_capacity, max_effort_per_sprint)

                        if allocated_effort > 0:
                            # Allocate the allowed effort to the last sprint
                            sprint_allocations[sprint_name][project_type].append({
                                'project_effort': f"{int(epic['projects_Work_Item_ID'])} ({'A' if project_type == 'anchor' else 'NA'}) - {epic_title} ({allocated_effort}) [Below Min Points]"
                            })
                            sprint_allocations[sprint_name][f'remaining_{project_type}_effort'] -= allocated_effort

                            # Mark the project as allocated to this sprint
                            sprint_allocations[sprint_name].setdefault('allocated_projects', set()).add(project_id)

                    continue  # Skip further distribution logic for this epic

                # Adjust distribution logic based on minimum epic points
                if num_eligible_sprints > 0:
                    start_index = 0  # Default to the first eligible sprint
                    for idx in range(num_eligible_sprints):
                        remaining_sprints = eligible_sprints_df.iloc[idx:]
                        num_remaining_sprints = len(remaining_sprints)
                        average_distribution = total_effort / num_remaining_sprints

                        if average_distribution >= minimum_epic_points:
                            start_index = idx
                            break

                    # Filter sprints starting from the calculated index
                    eligible_sprints_df = eligible_sprints_df.iloc[start_index:]
                for _, sprint in upcoming_sprints_df.iterrows():
                    sprint_name = sprint['Iteration']
                    sprint_start_date = sprint['Start_date']
                    remaining_sprint_capacity = sprint_allocations[sprint_name][f'remaining_{project_type}_effort']

                    # Fetch max effort constraint from sprint-level data
                    max_effort_per_sprint = sprint[max_effort_column]

                    # Skip if this sprint is already used for the project
                    if sprint_name in used_sprints:
                        continue

                    # Allocate effort with sprint and project constraints
                    allocated_effort = min(remaining_effort, remaining_sprint_capacity, max_effort_per_sprint)

                    if allocated_effort > 0:
                        overdue = nearest_due_date and sprint_start_date > nearest_due_date
                        effort_text = f"{int(epic['projects_Work_Item_ID'])} ({'A' if project_type == 'anchor' else 'NA'}) - {epic_title} ({allocated_effort}) {'[Overdue]' if overdue else ''}"
                        sprint_allocations[sprint_name][project_type].append({'project_effort': effort_text, 'overdue': overdue})
                        sprint_allocations[sprint_name][f'remaining_{project_type}_effort'] -= allocated_effort
                        remaining_effort -= allocated_effort
                        used_sprints.add(sprint_name)

                    if remaining_effort <= 0:
                        break

    # Allocate anchor and non-anchor projects
    allocate_epics(anchor_projects_df, 'anchor')
    allocate_epics(non_anchor_projects_df, 'non_anchor')

    # Prepare a list of sprints with buffer capacity remaining
    sprints_with_buffer = [
        {
            'Sprint': sprint_name,
            'Remaining_Anchor_Effort': allocations['remaining_anchor_effort'],
            'Remaining_Non_Anchor_Effort': allocations['remaining_non_anchor_effort']
        }
        for sprint_name, allocations in sprint_allocations.items()
        if allocations['remaining_anchor_effort'] > 0 or allocations['remaining_non_anchor_effort'] > 0
    ]

    # Prepare final allocations
    allocation_results = []
    for sprint_name, allocations in sprint_allocations.items():
        for project_type, items in {'anchor': allocations['anchor'], 'non_anchor': allocations['non_anchor']}.items():
            for item in items:
                allocation_results.append({
                    'Sprint': sprint_name,
                    'Effort': item['project_effort']
                })

    allocations_df = pd.DataFrame(allocation_results)
    sprint_order = upcoming_sprints_df['Iteration'].tolist()
    pivot_df = allocations_df.pivot(columns='Sprint', values='Effort').reindex(columns=sprint_order).reset_index(drop=True)
    df_uniform = pivot_df.apply(lambda x: pd.Series(x.dropna().values), axis=0)
    print(sprints_with_buffer)
    return df_uniform, anchor_projects_df, non_anchor_projects_df

@st.cache_data
def get_project_data():
    # Define the SQL queries to select only the required columns
    projects_query = """
    SELECT Work_Item_ID, title, EA_Number, Iteration_Path, Work_Item_Type, State, Scoping_30_Percent, SeventyFivePercentComplete, 
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
    Microsoft_VSTS_Scheduling_Effort FROM productbacklogitems where [System_State] IN ('New', 'Approved', 'Committed');
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
    # Merge to create project-epic-feature hierarchy with PBIs
    merged_df = productbacklogitems_df.merge(features_df, left_on='pbis_System_Parent', right_on='features_system_Id', how='left') \
                                      .merge(epics_df, left_on='features_System_Parent', right_on='epics_System_Id', how='left') \
                                      .merge(projects_df, left_on='epics_System_Parent', right_on='projects_Work_Item_ID', how='left')

    
    # Sum efforts from product backlog items grouped by Project, Epic, and Feature, including Epic title
    aggregated_df = merged_df.groupby(
        ['projects_Work_Item_ID', 'epics_System_Id', 'epics_System_Title', 'projects_Title','projects_EA_Number'], as_index=False
    ).agg({
        'pbis_Microsoft_VSTS_Scheduling_Effort': 'sum',  # Summing effort from PBIs
        **{col: 'first' for col in projects_df.columns}  # Keep all project columns
    })

    # Rename effort column for clarity
    aggregated_df = aggregated_df.rename(columns={'pbis_Microsoft_VSTS_Scheduling_Effort': 'total_effort_from_pbis'})
    
      # Filter rows containing "Post" or "post" (case-insensitive)
    aggregated_df = aggregated_df[~aggregated_df['epics_System_Title'].str.contains('Post', case=False, na=False)]

     # Define conditions and corresponding choices for nearest_doc_date
    conditions = [
        aggregated_df['epics_System_Title'].str.contains('QAQC|QA/QC|PS&E', case=False, na=False),
        aggregated_df['epics_System_Title'].str.contains('30%|Preliminary', case=False, na=False),
        aggregated_df['epics_System_Title'].str.contains('Intermediate', case=False, na=False),
        aggregated_df['epics_System_Title'].str.contains('75%', case=False, na=False),
        aggregated_df['epics_System_Title'].str.contains('Doc Design', case=False, na=False)  &
        ~aggregated_df['epics_System_Title'].str.contains('post', case=False, na=False)
    ]
    choices = [
        aggregated_df['projects_QAQC_Submittal_Date'],
        aggregated_df['projects_Scoping_30_Percent'],
        aggregated_df['projects_Intermediate_Date'],
        aggregated_df['projects_SeventyFivePercentComplete'],
        aggregated_df['projects_Document_Submittal_Date']
    ]

    # Apply conditions using np.select
    aggregated_df['nearest_doc_date'] = np.select(conditions, choices, default=np.nan)

    # Combine all conditions to create a filter mask
    filter_mask = conditions[0]
    for condition in conditions[1:]:
        filter_mask |= condition

    # Filter out rows where none of the conditions are met
    aggregated_df = aggregated_df[filter_mask].copy()

    # Reorder columns with specified order first
    ordered_columns = ['projects_Work_Item_ID', 'epics_System_Id', 'epics_System_Title', 'total_effort_from_pbis'] + \
                      [col for col in aggregated_df.columns if col not in ['projects_Work_Item_ID', 'epics_System_Id', 'epics_System_Title', 'total_effort_from_pbis']]
    aggregated_df = aggregated_df[ordered_columns]


    # Create a new column 'projects_complexity' that is the sum of the specified columns
    aggregated_df['projects_complexity'] = aggregated_df[['projects_Complexity_Signals', 'projects_Complexity_Lighting', 
                                                'projects_Complexity_ITS', 'projects_Complexity_Power_Design', 
                                                'projects_Complexity_RoW_Coordination', 'projects_Complexity_SLI_Project_Lead', 
                                                'projects_Complexity_Solar_Design', 'projects_Complexity_Trunkline']].sum(axis=1)
   
    
    # Split into anchor and non-anchor project DataFrames based on `projects_Anchor_Project`
    anchor_project_df = aggregated_df[aggregated_df['projects_Anchor_Project'] == 1].copy()
    non_anchor_project_df = aggregated_df[aggregated_df['projects_Anchor_Project'] == 0].copy()


    anchor_project_df = sort_projects_dataframe(anchor_project_df)
    non_anchor_project_df = sort_projects_dataframe(non_anchor_project_df)
    projects_display_df = pd.read_sql_query(projects_query, conn)
    # Close the database connection
    conn.close()
    return anchor_project_df, non_anchor_project_df, projects_display_df



def get_upcoming_sprints_with_effortpoints_and_weightage():
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get today's date in 'YYYY-MM-DD' format
    today = datetime.now().strftime('%Y-%m-%d')

    # Query to get all upcoming sprints (where start date is greater than today's date)
    # cursor.execute('''
    #     SELECT Iteration, Start_date, End_date 
    #     FROM iterations 
    #     WHERE Start_date > ? 
    #     ORDER BY Start_date ASC
    # ''', (today,))
    cursor.execute('''
    SELECT Iteration, Start_date, End_date 
    FROM iterations 
    WHERE DATE(End_date, '+2 day') > ? 
    ORDER BY Start_date ASC
    ''', (today,))

    # Fetch all results
    upcoming_sprints = cursor.fetchall()

    # Convert the fetched results into a pandas DataFrame
    df = pd.DataFrame(upcoming_sprints, columns=['Iteration', 'Start_date', 'End_date'])

    # Add columns for holidays count, leave count, effort points per user, resource count, total effort points, and weightage points
    df['Holidays_Count'] = 0
    df['Effort_points_per_user'] = 10  # Each user gets 10 effort points per sprint
    df['resource_count'] = 0  # Initialize resource count to be calculated per sprint
    df['Leave_Days'] = 0  # Initialize Leave_Days
    df['TotalEffortpoints'] = 0  # Initialize TotalEffortpoints
    df['MiscEffortPoints'] = 0  # Initialize Misc Effort Points
    df['AnchorEffortPoints'] = 0  # Initialize Anchor Effort Points
    df['NonAnchorEffortPoints'] = 0  # Initialize Non-Anchor Effort Points

    # Fetch weightage configuration for MiscWgt, AnchorWgt, and NonAnchorWgt
    cursor.execute('SELECT * FROM weightageconfig LIMIT 1')
    weightageconfig = cursor.fetchone() 
    if weightageconfig:
        miscwgt = weightageconfig[3]
        anchorwgt = weightageconfig[1]
        nonanchorwgt = weightageconfig[2]
        anchorMaxPoints = weightageconfig[4]
        nonAnchorMaxPoints = weightageconfig[5]
        epicMinEffortPoints = weightageconfig[6]

    # For each sprint, calculate the number of holidays, leaves, and resource count
    for i, row in df.iterrows():
        sprint_start = datetime.strptime(row['Start_date'], '%Y-%m-%d')
        sprint_end = datetime.strptime(row['End_date'], '%Y-%m-%d')

        # Get resource count for the specific sprint start date
        resource_count = get_usercount(row['Start_date'])
        df.at[i, 'resource_count'] = resource_count

        # Fetch holidays within the sprint start and end dates
        cursor.execute('''
            SELECT Holiday_date
            FROM holidays
            WHERE Holiday_date BETWEEN ? AND ?
        ''', (row['Start_date'], row['End_date']))
        
        holiday_dates = [datetime.strptime(h[0], '%Y-%m-%d') for h in cursor.fetchall()]
        df.at[i, 'Holidays_Count'] = len(holiday_dates)

        # Calculate total leave days that overlap with the sprint, excluding weekends and holidays
        cursor.execute('''
            SELECT leave_from, leave_to
            FROM leaves
            WHERE leave_from <= ? AND leave_to >= ?
        ''', (row['End_date'], row['Start_date']))

        total_leave_days = 0
        leave_periods = cursor.fetchall()

        # Loop through each leave period and calculate the overlap with the sprint, excluding weekends and holidays
        for leave_from, leave_to in leave_periods:
            leave_from_date = datetime.strptime(leave_from, '%Y-%m-%d')
            leave_to_date = datetime.strptime(leave_to, '%Y-%m-%d')

            # Calculate the overlapping days between the leave period and the sprint period, excluding weekends and holidays
            overlap_days = calculate_days_overlap_exclude_weekends_and_holidays(leave_from_date, leave_to_date, sprint_start, sprint_end, holiday_dates)
            total_leave_days += overlap_days

        df.at[i, 'Leave_Days'] = total_leave_days

        # Calculate TotalEffortpoints based on the formula
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
        df.at[i, 'MaxAnchorEffortPointspersprint'] = anchorMaxPoints
        df.at[i, 'MaxNonAnchorEffortPointspersprint'] = nonAnchorMaxPoints
        df.at[i, 'minimumEpicPoints'] = epicMinEffortPoints

    # Close the connection
    #print(df.to_excel("sample.xlsx"))
    conn.close()
    
    return df

def get_usercount(start_date):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    
    # Query to count the number of users in the 'users' table whose start date matches the sprint start date
    query_users_count = "SELECT COUNT(*) FROM users WHERE start_date <= ?"
    cursor = conn.cursor()
    cursor.execute(query_users_count, (start_date,))
    
    # Fetch the number of users
    number_of_users = cursor.fetchone()[0]
    
    # Close the database connection
    conn.close()
    
    return number_of_users

def calculate_days_overlap_exclude_weekends_and_holidays(start_date, end_date, sprint_start, sprint_end, holiday_dates):
    overlap_start = max(start_date, sprint_start)
    overlap_end = min(end_date, sprint_end)

    # Calculate total days between overlap period, excluding weekends and holidays
    total_days = 0
    current_day = overlap_start
    while current_day <= overlap_end:
        # Exclude weekends (Saturday and Sunday) and holidays
        if current_day.weekday() < 5 and current_day not in holiday_dates:
            total_days += 1
        current_day += timedelta(days=1)

    return total_days


 # Function to fetch the latest weightage configuration from the database
def fetch_latest_config():
    conn = sqlite3.connect(db_path)
    query = '''
        SELECT AnchorWgt, NonAnchorWgt, MiscWgt, AnchorMaxPoints, NonAnchorMaxPoints, EpicMinEffortPoints 
        FROM weightageconfig
        ORDER BY modifiedtime DESC LIMIT 1
    '''
    config = pd.read_sql(query, conn)
    conn.close()
    return config.iloc[0] if not config.empty else None

def fetch_sprint_trends_data_to_df():
    """
    Fetch data from sprint_trends_data table joined with productbacklogitems, features, epics, 
    and projects tables, including the anchor_project field.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        pd.DataFrame: DataFrame containing the joined data.
    """
    # Query to join tables and include anchor_project
    query = """
    SELECT 
        s.id AS sprint_id,
        s.effort AS efforts,
        s.sprint_name AS sprint_name,
        pbi.System_Id AS product_backlog_item_id,
        pbi.System_Parent AS feature_parent_id,
        f.System_Id AS feature_id,
        f.System_Parent AS epic_parent_id,
        e.System_Title AS epic_title,
        e.System_Parent AS project_work_item_id,
        pr.Work_Item_Id AS project_work_item_id_final,
        pr.Anchor_Project AS anchor_project
    FROM 
        sprint_trends_data AS s
    LEFT JOIN 
        productbacklogitems AS pbi ON s.id = pbi.System_Id
    LEFT JOIN 
        features AS f ON pbi.System_Parent = f.System_Id
    LEFT JOIN 
        epics AS e ON f.System_Parent = e.System_Id
    LEFT JOIN 
        projects AS pr ON e.System_Parent = pr.Work_Item_Id
    """

    # Connect to the database and execute the query
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()

    return df

def classify_epic_titles(df):
    """
    Classify epic titles based on predefined conditions and add a new column to the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame containing the epic titles.

    Returns:
        pd.DataFrame: Updated DataFrame with a new column 'epic_type'.
    """
    # Define the conditions for classifying epic titles
    conditions = [
        df['epic_title'].str.contains('QAQC|QA/QC|PS&E', case=False, na=False),
        df['epic_title'].str.contains('30%|Preliminary', case=False, na=False),
        df['epic_title'].str.contains('Intermediate', case=False, na=False),
        df['epic_title'].str.contains('75%', case=False, na=False),
        df['epic_title'].str.contains('Doc Design', case=False, na=False) &
        ~df['epic_title'].str.contains('post', case=False, na=False)
    ]

    # Define the corresponding labels for each condition
    labels = [
        'QAQC/PS&E',
        'Preliminary Design',
        'Intermediate Design',
        '75% Design',
        'Document Design'
    ]

    # Add a new column 'epic_type' based on the conditions
    df['epic_type'] = np.select(conditions, labels, default='Other')

    return df

 
def analyze_sprint_efforts(df):
    """
    Analyze sprint efforts to calculate anchor, non-anchor, and miscellaneous percentages for each sprint,
    including maximum effort points in a project for both anchor and non-anchor projects.

    Args:
        df (pd.DataFrame): Input DataFrame containing sprint data.

    Returns:
        pd.DataFrame: Analysis DataFrame with anchor, non-anchor, and misc percentages for each sprint,
                      and maximum effort points for anchor and non-anchor projects.
    """
    specified_epic_types = [
        'QAQC/PS&E',
        'Preliminary Design',
        'Intermediate Design',
        '75% Design',
        'Document Design'
    ]

    # Ensure 'efforts' and 'anchor_project' columns are numeric
    df['efforts'] = pd.to_numeric(df['efforts'], errors='coerce')
    df['anchor_project'] = pd.to_numeric(df['anchor_project'], errors='coerce')

    # Drop rows with invalid 'efforts' or 'anchor_project'
    df = df.dropna(subset=['efforts', 'anchor_project'])

    # Categorize rows based on specified epic types
    df['category'] = df['epic_type'].apply(
        lambda x: 'Specified' if x in specified_epic_types else 'Misc'
    )

    # Group data by sprint_name and category
    grouped = df.groupby(['sprint_name', 'category'])

    # Aggregate efforts for Specified and Misc categories
    effort_summary = grouped.agg(
        total_effort=('efforts', 'sum'),
        anchor_effort=('efforts', lambda x: x[df.loc[x.index, 'anchor_project'] == 1].sum()),
        non_anchor_effort=('efforts', lambda x: x[df.loc[x.index, 'anchor_project'] == 0].sum()),
        max_anchor_effort=('efforts', lambda x: x[df.loc[x.index, 'anchor_project'] == 1].max()),
        max_non_anchor_effort=('efforts', lambda x: x[df.loc[x.index, 'anchor_project'] == 0].max())
    ).reset_index()

    # Pivot the data for easier computation
    pivot_summary = effort_summary.pivot(index='sprint_name', columns='category', values=[
        'total_effort', 'anchor_effort', 'non_anchor_effort', 'max_anchor_effort', 'max_non_anchor_effort']).fillna(0)

    # Flatten the pivot table columns
    pivot_summary.columns = ['_'.join(col).strip() for col in pivot_summary.columns.values]

    # Calculate total effort across Specified and Misc
    pivot_summary['total_effort_all'] = pivot_summary['total_effort_Specified'] + pivot_summary['total_effort_Misc']

    # Calculate percentages
    pivot_summary['anchor_percentage_specified'] = (pivot_summary['anchor_effort_Specified'] / pivot_summary['total_effort_Specified']) * 100
    pivot_summary['non_anchor_percentage_specified'] = (pivot_summary['non_anchor_effort_Specified'] / pivot_summary['total_effort_Specified']) * 100
    pivot_summary['misc_percentage'] = (pivot_summary['total_effort_Misc'] / pivot_summary['total_effort_all']) * 100

    # Reset index for final output
    return pivot_summary.reset_index()

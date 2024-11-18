import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import numpy as np
import os
#from dotenv import load_dotenv
# load_dotenv()

# DB_PATH = os.getenv('DB_PATH')
db_path='NDOTDATA.db'
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
        'nearest_doc_date',   # Fourth priority: Nearest Date (soonest first)
        'projects_Priority_Traffic_Ops',    # Fifth priority: Traffic Ops (descending, higher numbers first)
        'projects_Route_Type',     # Sixth priority: Route Type
        'projects_complexity'      # Seventh priority: Project Complexity (descending, higher complexity first)
    ], ascending=[True, True, True, True, False, True, False])  # Adjusting the sorting order for each column

    return sorted_df

# working one

# def distribute_epics_to_sprints(anchor_projects_df, non_anchor_projects_df, upcoming_sprints_df):
#     # Initialize dictionary to store sprint allocations
#     sprint_allocations = {sprint: {'anchor': [], 'non_anchor': [], 'remaining_anchor_effort': 0, 'remaining_non_anchor_effort': 0}
#                           for sprint in upcoming_sprints_df['Iteration']}
    
#     # Initialize remaining capacity for each sprint based on anchor and non-anchor max values
#     for _, sprint in upcoming_sprints_df.iterrows():
#         sprint_name = sprint['Iteration']
#         sprint_allocations[sprint_name]['remaining_anchor_effort'] = sprint['AnchorEffortPoints']
#         sprint_allocations[sprint_name]['remaining_non_anchor_effort'] = sprint['NonAnchorEffortPoints']

#     # Function to allocate effort for a set of projects (either anchor or non-anchor)
#     def allocate_projects(projects_df, effort_col, project_type):
#         for _, epic in projects_df.iterrows():
#             remaining_effort = epic['total_effort_from_pbis']
#             nearest_due_date = pd.to_datetime(epic['nearest_doc_date']).tz_localize(None) if not pd.isnull(epic['nearest_doc_date']) else None

#             # Distribute epic effort across sprints, respecting sprint capacity limits
#             for _, sprint in upcoming_sprints_df.iterrows():
#                 sprint_name = sprint['Iteration']
#                 min_epic_effort = sprint['Effort_points_per_user']  # Minimum required effort for the epic in this sprint
#                 sprint_start_date = pd.to_datetime(sprint['Start_date']).tz_localize(None)
#                 sprint_end_date = pd.to_datetime(sprint['End_date']).tz_localize(None)
                
#                 # Calculate average effort per remaining sprints to ensure balanced distribution
#                 sprints_remaining = len(upcoming_sprints_df[upcoming_sprints_df['Iteration'] >= sprint_name])
#                 avg_effort_per_sprint = remaining_effort / sprints_remaining

#                 # Ensure average meets minimum required or that epic is due soon
#                 if avg_effort_per_sprint < min_epic_effort and (not nearest_due_date or sprint_start_date > nearest_due_date):
#                     continue
                
#                 # Determine the remaining capacity in this sprint for anchor or non-anchor projects
#                 remaining_sprint_capacity = sprint_allocations[sprint_name][f'remaining_{project_type}_effort']
#                 allocated_effort = min(remaining_effort, remaining_sprint_capacity)

#                 # Allocate effort to this sprint without exceeding its capacity
#                 if allocated_effort > 0:
#                     sprint_allocations[sprint_name][project_type].append({
#                         'project_id': epic['projects_Work_Item_ID'],
#                         'epic_id': epic['epics_System_Id'],
#                         'epic_title': epic['epics_System_Title'],
#                         'allocated_effort': allocated_effort,
#                         'overdue': nearest_due_date and sprint_end_date > nearest_due_date
#                     })
                    
#                     # Update remaining capacity and remaining effort
#                     sprint_allocations[sprint_name][f'remaining_{project_type}_effort'] -= allocated_effort
#                     remaining_effort -= allocated_effort

#                     # If effort has been fully allocated, move to next epic
#                     if remaining_effort <= 0:
#                         break

#     # Allocate efforts for anchor and non-anchor projects separately
#     allocate_projects(anchor_projects_df, 'AnchorEffortPoints', 'anchor')
#     allocate_projects(non_anchor_projects_df, 'NonAnchorEffortPoints', 'non_anchor')

#     # Convert allocations dictionary to DataFrame for easy viewing
#     allocation_results = []
#     for sprint_name, allocations in sprint_allocations.items():
#         for project_type, items in {'anchor': allocations['anchor'], 'non_anchor': allocations['non_anchor']}.items():
#             for item in items:
#                 item['sprint'] = sprint_name
#                 item['sprint_type'] = project_type
#                 allocation_results.append(item)

#     allocations_df = pd.DataFrame(allocation_results)
#     return allocations_df

import pandas as pd
import numpy as np

import pandas as pd
import numpy as np


# def distribute_epics_to_sprints(anchor_projects_df, non_anchor_projects_df, upcoming_sprints_df):
#     # Initialize dictionary to store sprint allocations
#     sprint_allocations = {sprint: {'anchor': [], 'non_anchor': [], 'remaining_anchor_effort': 0, 'remaining_non_anchor_effort': 0}
#                           for sprint in upcoming_sprints_df['Iteration']}
#     anchor_projects_df['nearest_doc_date'] = pd.to_datetime(anchor_projects_df['nearest_doc_date'], errors='coerce').dt.tz_localize(None)
#     non_anchor_projects_df['nearest_doc_date'] = pd.to_datetime(non_anchor_projects_df['nearest_doc_date'], errors='coerce').dt.tz_localize(None)
   
#     upcoming_sprints_df['Start_date'] = pd.to_datetime(upcoming_sprints_df['Start_date'], errors='coerce').dt.tz_localize(None)

#     # Initialize remaining capacity for each sprint based on total allowed capacity
#     for _, sprint in upcoming_sprints_df.iterrows():
#         sprint_name = sprint['Iteration']
#         sprint_allocations[sprint_name]['remaining_anchor_effort'] = sprint['AnchorEffortPoints']
#         sprint_allocations[sprint_name]['remaining_non_anchor_effort'] = sprint['NonAnchorEffortPoints']

#     # Function to allocate effort for a set of projects (either anchor or non-anchor)
#     def allocate_projects(projects_df, project_type):
#         max_effort_per_sprint_column = 'MaxAnchorEffortPointspersprint' if project_type == 'anchor' else 'MaxNonAnchorEffortPointspersprint'
#         anchor_projects_df['nearest_doc_date'] = pd.to_datetime(anchor_projects_df['nearest_doc_date']).dt.tz_localize(None)
#         upcoming_sprints_df['Start_date'] = pd.to_datetime(upcoming_sprints_df['Start_date'])
#         for _, epic in projects_df.iterrows():
#             remaining_effort = epic['total_effort_from_pbis']
#             nearest_due_date = pd.to_datetime(epic['nearest_doc_date']) if not pd.isnull(epic['nearest_doc_date']) else None

#             # Filter relevant sprints based on the nearest due date
#             sprints = upcoming_sprints_df.copy()
#             sprints['Start_date'] = pd.to_datetime(sprints['Start_date'], errors='coerce').dt.tz_localize(None)

#             if nearest_due_date is not None:
#                 nearest_due_date = pd.to_datetime(nearest_due_date).tz_localize(None)
#                 print(type(sprints['Start_date']))
#                 print(type(nearest_due_date))
#                 sprints = sprints[sprints['Start_date'] <= nearest_due_date]

#             # Calculate average effort per sprint and check against minimumEpicPoints
#             sprints_remaining = len(sprints)
#             avg_effort_per_sprint = remaining_effort / sprints_remaining if sprints_remaining > 0 else 0
#             minimum_epic_points = upcoming_sprints_df['minimumEpicPoints'].iloc[0]

#             if avg_effort_per_sprint < minimum_epic_points:
#                 if nearest_due_date is None:
#                     sprints['proximity'] = float('inf')  # Assign infinite proximity for missing due dates
#                 else:
#                     sprints['proximity'] = abs((sprints['Start_date'] - nearest_due_date).dt.days)
#                 sprints = sprints.sort_values(by='proximity')
#             else:
#                 # Otherwise, distribute evenly across all eligible sprints
#                 sprints = sprints.sort_values(by='Start_date')

#             # Allocate effort to the selected sprints
#             for _, sprint in sprints.iterrows():
#                 sprint_name = sprint['Iteration']
#                 max_project_effort_per_sprint = sprint[max_effort_per_sprint_column]

#                 remaining_sprint_capacity = sprint_allocations[sprint_name][f'remaining_{project_type}_effort']
#                 allocated_effort = min(remaining_effort, remaining_sprint_capacity, max_project_effort_per_sprint)

#                 # Ensure allocated effort meets the minimum required points
#                 if allocated_effort >= minimum_epic_points:
#                     effort_text = f"{int(epic['projects_Work_Item_ID'])} ({'A' if project_type == 'anchor' else 'NA'}) - {epic['epics_System_Title']} ({allocated_effort}{' overdue' if nearest_due_date and sprint['Start_date'] > nearest_due_date else ''})"
#                     sprint_allocations[sprint_name][project_type].append({'project_epic_effort': effort_text})

#                     # Update capacities and remaining effort
#                     sprint_allocations[sprint_name][f'remaining_{project_type}_effort'] -= allocated_effort
#                     remaining_effort -= allocated_effort

#                 if remaining_effort <= 0:
#                     break

#     # Allocate efforts for anchor and non-anchor projects separately
#     allocate_projects(anchor_projects_df, 'anchor')
#     allocate_projects(non_anchor_projects_df, 'non_anchor')

#     # Convert allocations to a list of dictionaries for each sprint with combined data in each cell
#     allocation_results = []
#     for sprint_name, allocations in sprint_allocations.items():
#         for project_type, items in {'anchor': allocations['anchor'], 'non_anchor': allocations['non_anchor']}.items():
#             for item in items:
#                 allocation_results.append({
#                     'Sprint': sprint_name,
#                     'Effort': item['project_epic_effort']
#                 })

#     # Create DataFrame and pivot it so that each sprint is a column with combined efforts in each cell
#     allocations_df = pd.DataFrame(allocation_results)
#     sprint_order = upcoming_sprints_df['Iteration'].tolist()
#     pivot_df = allocations_df.pivot(columns='Sprint', values='Effort').reindex(columns=sprint_order).reset_index(drop=True)
#     df_uniform = pivot_df.apply(lambda x: pd.Series(x.dropna().values), axis=0)

#     # Return the resulting allocation DataFrame
#     return df_uniform, anchor_projects_df, non_anchor_projects_df


def distribute_epics_to_sprints(anchor_projects_df, non_anchor_projects_df, upcoming_sprints_df):
    # Initialize dictionary to store sprint allocations
    sprint_allocations = {sprint: {'anchor': [], 'non_anchor': [], 'remaining_anchor_effort': 0, 'remaining_non_anchor_effort': 0}
                          for sprint in upcoming_sprints_df['Iteration']}
    anchor_projects_df['nearest_doc_date'] = pd.to_datetime(anchor_projects_df['nearest_doc_date'], errors='coerce').dt.tz_localize(None)
    non_anchor_projects_df['nearest_doc_date'] = pd.to_datetime(non_anchor_projects_df['nearest_doc_date'], errors='coerce').dt.tz_localize(None)
   
    upcoming_sprints_df['Start_date'] = pd.to_datetime(upcoming_sprints_df['Start_date'], errors='coerce').dt.tz_localize(None)

    # Initialize remaining capacity for each sprint based on total allowed capacity
    for _, sprint in upcoming_sprints_df.iterrows():
        sprint_name = sprint['Iteration']
        sprint_allocations[sprint_name]['remaining_anchor_effort'] = sprint['AnchorEffortPoints']
        sprint_allocations[sprint_name]['remaining_non_anchor_effort'] = sprint['NonAnchorEffortPoints']

    # Function to allocate effort for a set of projects (either anchor or non-anchor)
    def allocate_projects(projects_df, project_type):
        max_effort_per_sprint_column = 'MaxAnchorEffortPointspersprint' if project_type == 'anchor' else 'MaxNonAnchorEffortPointspersprint'
        for _, epic in projects_df.iterrows():
            remaining_effort = epic['total_effort_from_pbis']
            nearest_due_date = pd.to_datetime(epic['nearest_doc_date']) if not pd.isnull(epic['nearest_doc_date']) else None

            # Filter relevant sprints based on the nearest due date
            sprints = upcoming_sprints_df.copy()
            sprints['overdue'] = False  # Flag for overdue efforts
            if nearest_due_date is not None:
                print(sprints['Start_date'])
                print("nearest")
                print(nearest_due_date)
                sprints.loc[sprints['Start_date'] > nearest_due_date, 'overdue'] = True

            # Calculate average effort per sprint and check against minimumEpicPoints
            sprints_remaining = len(sprints)
            avg_effort_per_sprint = remaining_effort / sprints_remaining if sprints_remaining > 0 else 0
            minimum_epic_points = upcoming_sprints_df['minimumEpicPoints'].iloc[0]

            # Adjust start sprint if average effort falls below the minimumEpicPoints
            if avg_effort_per_sprint < minimum_epic_points:
                total_effort = remaining_effort
                for idx, sprint in enumerate(sprints.itertuples()):
                    remaining_sprints = len(sprints) - idx
                    avg_effort_from_here = total_effort / remaining_sprints
                    if avg_effort_from_here >= minimum_epic_points:
                        sprints = sprints.iloc[idx:]
                        break

            # Sort sprints by start date to allocate sequentially
            #sprints = sprints.sort_values(by=['overdue', 'Start_date'])

            # Allocate effort to the selected sprints
            for _, sprint in sprints.iterrows():
                sprint_name = sprint['Iteration']
                max_project_effort_per_sprint = sprint[max_effort_per_sprint_column]

                remaining_sprint_capacity = sprint_allocations[sprint_name][f'remaining_{project_type}_effort']
                allocated_effort = min(remaining_effort, remaining_sprint_capacity, max_project_effort_per_sprint)

                # Mark effort as overdue if applicable
                overdue_label = " overdue" if sprint['overdue'] else ""

                # Allocate the effort
                if allocated_effort > 0:
                    effort_text = f"{int(epic['projects_Work_Item_ID'])} ({'A' if project_type == 'anchor' else 'NA'}) - {epic['epics_System_Title']} ({allocated_effort}{overdue_label})"
                    sprint_allocations[sprint_name][project_type].append({'project_epic_effort': effort_text})

                    # Update capacities and remaining effort
                    sprint_allocations[sprint_name][f'remaining_{project_type}_effort'] -= allocated_effort
                    remaining_effort -= allocated_effort

                if remaining_effort <= 0:
                    break

    # Allocate efforts for anchor and non-anchor projects separately
    allocate_projects(anchor_projects_df, 'anchor')
    allocate_projects(non_anchor_projects_df, 'non_anchor')

    # Convert allocations to a list of dictionaries for each sprint with combined data in each cell
    allocation_results = []
    for sprint_name, allocations in sprint_allocations.items():
        for project_type, items in {'anchor': allocations['anchor'], 'non_anchor': allocations['non_anchor']}.items():
            for item in items:
                allocation_results.append({
                    'Sprint': sprint_name,
                    'Effort': item['project_epic_effort']
                })

    # Create DataFrame and pivot it so that each sprint is a column with combined efforts in each cell
    allocations_df = pd.DataFrame(allocation_results)
    print(allocations_df)
    sprint_order = upcoming_sprints_df['Iteration'].tolist()
    pivot_df = allocations_df.pivot(columns='Sprint', values='Effort').reindex(columns=sprint_order).reset_index(drop=True)
    df_uniform = pivot_df.apply(lambda x: pd.Series(x.dropna().values), axis=0)

    # Return the resulting allocation DataFrame
    return df_uniform, anchor_projects_df, non_anchor_projects_df




# Sample usage
# formatted_df = distribute_epics_to_sprints(anchor_projects_df, non_anchor_projects_df, upcoming_sprints_df)
# formatted_df  # Display the styled pivot table without the extra index column

# Sample usage with upcoming_sprints_df containing the max effort limits
# formatted_df = distribute_epics_to_sprints(anchor_projects_df, non_anchor_projects_df, upcoming_sprints_df)
# formatted_df  # Display the styled pivot table without the extra index column

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
    
    #aggregated_df.to_excel('work_items_latest_monday.xlsx', index=False)
    aggregated_df =  pd.read_excel("testdata.xlsx")
    # Filter rows containing "Post" or "post" (case-insensitive)
    aggregated_df = aggregated_df[~aggregated_df['epics_System_Title'].str.contains('Post', case=False, na=False)]


     # Define conditions and corresponding choices for nearest_doc_date
    conditions = [
    aggregated_df['epics_System_Title'].str.contains('QAQC|QA/QC|PS&E', case=False, na=False),
    aggregated_df['epics_System_Title'].str.contains('30%|Preliminary', case=False, na=False),
    aggregated_df['epics_System_Title'].str.contains('Intermediate', case=False, na=False),
    aggregated_df['epics_System_Title'].str.contains('75%', case=False, na=False),
    aggregated_df['epics_System_Title'].str.contains('Doc Design', case=False, na=False)

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

    return anchor_project_df, non_anchor_project_df



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
    print(df.to_excel("sample.xlsx"))
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

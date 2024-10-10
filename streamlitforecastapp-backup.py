# import streamlit as st
import pandas as pd
import requests
import sqlite3
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
from datetime import datetime
import os
import utilities

# Load environment variables from a .env file
load_dotenv()

# Read organization and project name from a .env file
ORGANIZATION = os.getenv('AZURE_DEVOPS_ORGANIZATION')
PROJECT = os.getenv('AZURE_DEVOPS_PROJECT')
PAT = os.getenv('AZURE_DEVOPS_PAT')

# Azure DevOps REST API endpoints
WIQL_URL = f'https://dev.azure.com/{ORGANIZATION}/{PROJECT}/_apis/wit/wiql?api-version=7.2-preview'
WORK_ITEMS_URL = f'https://dev.azure.com/{ORGANIZATION}/{PROJECT}/_apis/wit/workitems?ids={{}}&api-version=7.2-preview'

# SQLite database setup
DB_NAME = 'NDOTDATA.db'

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Create table for Projects
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            state TEXT NOT NULL,
            work_item_type TEXT NOT NULL,
            assigned_to TEXT,
            area_path TEXT,
            created_by TEXT,
            created_date TEXT,
            changed_date TEXT,
            reason TEXT,
            iteration_path TEXT,
            tags TEXT,
            description TEXT,
            history TEXT,
            board_column TEXT
        )
    ''')

     # Create the table with pre-defined columns based on the flattened JSON structure
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS epics (
            id INTEGER PRIMARY KEY,
            rev INTEGER,
            System_AreaPath TEXT,
            System_TeamProject TEXT,
            System_IterationPath TEXT,
            System_WorkItemType TEXT,
            System_State TEXT,
            System_Reason TEXT,
            System_CreatedDate TEXT,
            System_CreatedBy_DisplayName TEXT,
            System_CreatedBy_Url TEXT,
            System_CreatedBy_Id TEXT,
            System_CreatedBy_UniqueName TEXT,
            System_CreatedBy_ImageUrl TEXT,
            System_ChangedDate TEXT,
            System_ChangedBy_DisplayName TEXT,
            System_ChangedBy_Url TEXT,
            System_ChangedBy_Id TEXT,
            System_ChangedBy_UniqueName TEXT,
            System_ChangedBy_ImageUrl TEXT,
            System_Title TEXT,
            Microsoft_VSTS_Common_StateChangeDate TEXT,
            Microsoft_VSTS_Common_ActivatedDate TEXT,
            Microsoft_VSTS_Common_ActivatedBy_DisplayName TEXT,
            Microsoft_VSTS_Common_ActivatedBy_Url TEXT,
            Microsoft_VSTS_Common_ActivatedBy_Id TEXT,
            Microsoft_VSTS_Common_Priority INTEGER,
            Microsoft_VSTS_Common_BacklogPriority FLOAT,
            WEF_05580586AD3F4F25BFC82C24587654EC_Kanban_Column_Done BOOLEAN,
            url TEXT
        )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS features (
        id INTEGER,
        rev INTEGER,
        System_AreaPath TEXT,
        System_TeamProject TEXT,
        System_IterationPath TEXT,
        System_WorkItemType TEXT,
        System_State TEXT,
        System_Reason TEXT,
        System_CreatedDate TEXT,
        System_CreatedBy_DisplayName TEXT,
        System_CreatedBy_Url TEXT,
        System_CreatedBy_Id TEXT,
        System_CreatedBy_UniqueName TEXT,
        System_ChangedDate TEXT,
        System_ChangedBy_DisplayName TEXT,
        System_ChangedBy_Url TEXT,
        System_ChangedBy_Id TEXT,
        System_ChangedBy_UniqueName TEXT,
        System_CommentCount INTEGER,
        System_Title TEXT,
        System_BoardColumn TEXT,
        System_BoardColumnDone INTEGER,
        Microsoft_VSTS_Common_StateChangeDate TEXT,
        Microsoft_VSTS_Common_ClosedDate TEXT,
        Microsoft_VSTS_Common_ClosedBy_DisplayName TEXT,
        Microsoft_VSTS_Common_ClosedBy_Url TEXT,
        Microsoft_VSTS_Common_Priority INTEGER,
        Microsoft_VSTS_Common_ValueArea TEXT,
        System_Tags TEXT,
        url TEXT
    )
''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS  productbacklogitems (
    id INTEGER PRIMARY KEY,
    rev INTEGER,
    area_path TEXT,
    team_project TEXT,
    iteration_path TEXT,
    work_item_type TEXT,
    state TEXT,
    reason TEXT,
    assigned_to_name TEXT,
    assigned_to_id TEXT,
    assigned_to_email TEXT,
    assigned_to_url TEXT,
    assigned_to_avatar_url TEXT,
    created_date TEXT,
    created_by_name TEXT,
    created_by_id TEXT,
    created_by_email TEXT,
    created_by_url TEXT,
    created_by_avatar_url TEXT,
    changed_date TEXT,
    changed_by_name TEXT,
    changed_by_id TEXT,
    changed_by_email TEXT,
    changed_by_url TEXT,
    changed_by_avatar_url TEXT,
    closed_date TEXT,
    closed_by_name TEXT,
    closed_by_id TEXT,
    closed_by_email TEXT,
    closed_by_url TEXT,
    closed_by_avatar_url TEXT,
    comment_count INTEGER,
    title TEXT,
    board_column TEXT,
    board_column_done BOOLEAN,
    state_change_date TEXT,
    activated_date TEXT,
    activated_by_name TEXT,
    activated_by_id TEXT,
    activated_by_email TEXT,
    activated_by_url TEXT,
    activated_by_avatar_url TEXT,
    priority INTEGER,
    value_area TEXT,
    business_value INTEGER,
    effort REAL,
    backlog_priority REAL,
    kanban_column TEXT,
    kanban_column_done BOOLEAN,
    ea_number TEXT,
    description TEXT,
    acceptance_criteria TEXT,
    url TEXT
)
                    ''')


    conn.commit()
    conn.close()

# Function to fetch work item IDs in a given date range with a specific type
def fetch_work_item_ids(start_date, end_date, work_item_type):
    # Format the dates to ensure no time component
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    if work_item_type=='Product Backlog Item':
        query = {
        "query": f"Select [System.Id] From WorkItems Where [System.WorkItemType] = '{work_item_type}' And [System.CreatedDate] >= '{start_date_str}' And [System.CreatedDate] <= '{end_date_str}' And [System.State] IN ('New', 'Approved', 'Committed')"
         }
    else: 
        query = {
        "query": f"Select [System.Id] From WorkItems Where [System.WorkItemType] = '{work_item_type}' And [System.CreatedDate] >= '{start_date_str}' And [System.CreatedDate] <= '{end_date_str}'"
         }
         
    response = requests.post(WIQL_URL, json=query, auth=HTTPBasicAuth('', PAT))
    if response.status_code == 200:
        return response.json()["workItems"]
    else:
        # st.error(f"Failed to fetch work item IDs for range {start_date_str} - {end_date_str}: {response.status_code}")
        # st.error(response.json())
        return []

# Function to fetch work item details by IDs
def fetch_work_item_details(ids):
    print(len(ids))
    if not ids:
        return []

    ids_str = ','.join(map(str, ids))

    url = WORK_ITEMS_URL.format(ids_str)
    response = requests.get(url, auth=HTTPBasicAuth('', PAT))

    if response.status_code == 200:
        print(response)
        return response.json()['value']
    else:
        # st.error(f"Failed to fetch work item details: {response.status_code}")
        # st.error(response.json())
        return []

# Function to insert work items into the SQLite database table
def insert_projects_into_db(work_items):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for item in work_items:
        fields = item['fields']
        cursor.execute('''
            INSERT OR REPLACE INTO projects (
                id, title, state, work_item_type, assigned_to, area_path, created_by,
                created_date, changed_date, reason, iteration_path, tags, description,
                history, board_column
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)
        ''', (
            fields.get('System.Id'),
            fields.get('System.Title'),
            fields.get('System.State'),
            fields.get('System.WorkItemType'),
            fields.get('System.AssignedTo', {}).get('displayName') if fields.get('System.AssignedTo') else None,
            fields.get('System.AreaPath'),
            fields.get('System.CreatedBy', {}).get('displayName') if fields.get('System.CreatedBy') else None,
            fields.get('System.CreatedDate'),
            fields.get('System.ChangedDate'),
            fields.get('System.Reason'),
            fields.get('System.IterationPath'),
            fields.get('System.Tags'),
            fields.get('System.Description'),
            fields.get('System.History'),
            fields.get('System.BoardColumn')
        ))

    conn.commit()
    conn.close()

# Function to insert a flattened work item into the SQLite database
def insert_epics_into_db(work_item):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    # Flatten the work item dictionary
    flattened_work_item = utilities.flatten_dict(work_item)

    # Prepare insert statement for pre-defined table
    insert_query = '''
        INSERT OR REPLACE INTO epics (
            id, rev, System_AreaPath, System_TeamProject, System_IterationPath, 
            System_WorkItemType, System_State, System_Reason, System_CreatedDate,
            System_CreatedBy_DisplayName, System_CreatedBy_Url, System_CreatedBy_Id,
            System_CreatedBy_UniqueName, System_CreatedBy_ImageUrl, System_ChangedDate,
            System_ChangedBy_DisplayName, System_ChangedBy_Url, System_ChangedBy_Id,
            System_ChangedBy_UniqueName, System_ChangedBy_ImageUrl, System_Title,
            Microsoft_VSTS_Common_StateChangeDate, Microsoft_VSTS_Common_ActivatedDate,
            Microsoft_VSTS_Common_ActivatedBy_DisplayName, Microsoft_VSTS_Common_ActivatedBy_Url,
            Microsoft_VSTS_Common_ActivatedBy_Id, Microsoft_VSTS_Common_Priority,
            Microsoft_VSTS_Common_BacklogPriority, WEF_05580586AD3F4F25BFC82C24587654EC_Kanban_Column_Done,
            url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)
    '''

    # Prepare values for insertion
    values = (
        flattened_work_item.get('id'),
        flattened_work_item.get('rev'),
        flattened_work_item.get('fields.System.AreaPath'),
        flattened_work_item.get('fields.System.TeamProject'),
        flattened_work_item.get('fields.System.IterationPath'),
        flattened_work_item.get('fields.System.WorkItemType'),
        flattened_work_item.get('fields.System.State'),
        flattened_work_item.get('fields.System.Reason'),
        flattened_work_item.get('fields.System.CreatedDate'),
        flattened_work_item.get('fields.System.CreatedBy.displayName'),
        flattened_work_item.get('fields.System.CreatedBy.url'),
        flattened_work_item.get('fields.System.CreatedBy.id'),
        flattened_work_item.get('fields.System.CreatedBy.uniqueName'),
        flattened_work_item.get('fields.System.CreatedBy.imageUrl'),
        flattened_work_item.get('fields.System.ChangedDate'),
        flattened_work_item.get('fields.System.ChangedBy.displayName'),
        flattened_work_item.get('fields.System.ChangedBy.url'),
        flattened_work_item.get('fields.System.ChangedBy.id'),
        flattened_work_item.get('fields.System.ChangedBy.uniqueName'),
        flattened_work_item.get('fields.System.ChangedBy.imageUrl'),
        flattened_work_item.get('fields.System.Title'),
        flattened_work_item.get('fields.Microsoft.VSTS.Common.StateChangeDate'),
        flattened_work_item.get('fields.Microsoft.VSTS.Common.ActivatedDate'),
        flattened_work_item.get('fields.Microsoft.VSTS.Common.ActivatedBy.displayName'),
        flattened_work_item.get('fields.Microsoft.VSTS.Common.ActivatedBy.url'),
        flattened_work_item.get('fields.Microsoft.VSTS.Common.ActivatedBy.id'),
        flattened_work_item.get('fields.Microsoft.VSTS.Common.Priority'),
        flattened_work_item.get('fields.Microsoft.VSTS.Common.BacklogPriority'),
        flattened_work_item.get('fields.WEF_05580586AD3F4F25BFC82C24587654EC_Kanban.Column.Done'),
        flattened_work_item.get('url')
    )

    # Execute the insert statement
    cursor.execute(insert_query, values)
    conn.commit()
    conn.close()


# Function to insert the JSON fields into the SQLite table
def insert_features_into_db(json_data):

    flat_json = utilities.flatten_dict(json_data)
    # Connect to the SQLite database
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    # Define the specific fields we want to insert
    query = '''
        INSERT INTO features (
            id, rev, System_AreaPath, System_TeamProject, System_IterationPath,
            System_WorkItemType, System_State, System_Reason, System_CreatedDate,
            System_CreatedBy_DisplayName, System_CreatedBy_Url, System_CreatedBy_Id, System_CreatedBy_UniqueName,
            System_ChangedDate, System_ChangedBy_DisplayName, System_ChangedBy_Url, System_ChangedBy_Id, System_ChangedBy_UniqueName,
            System_CommentCount, System_Title, System_BoardColumn, System_BoardColumnDone,
            Microsoft_VSTS_Common_StateChangeDate, Microsoft_VSTS_Common_ClosedDate,
            Microsoft_VSTS_Common_ClosedBy_DisplayName, Microsoft_VSTS_Common_ClosedBy_Url,
            Microsoft_VSTS_Common_Priority, Microsoft_VSTS_Common_ValueArea,
            System_Tags, url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

    # Extract the corresponding values from the flattened JSON
    values = (
        flat_json.get('id'),
        flat_json.get('rev'),
        flat_json.get('fields.System.AreaPath'),
        flat_json.get('fields.System.TeamProject'),
        flat_json.get('fields.System.IterationPath'),
        flat_json.get('fields.System.WorkItemType'),
        flat_json.get('fields.System.State'),
        flat_json.get('fields.System.Reason'),
        flat_json.get('fields.System.CreatedDate'),
        flat_json.get('fields.System.CreatedBy.DisplayName'),
        flat_json.get('fields.System.CreatedBy.Url'),
        flat_json.get('fields.System.CreatedBy.Id'),
        flat_json.get('fields.System.CreatedBy.UniqueName'),
        flat_json.get('fields.System.ChangedDate'),
        flat_json.get('fields.System.ChangedBy.DisplayName'),
        flat_json.get('fields.System.ChangedBy.Url'),
        flat_json.get('fields.System.ChangedBy.Id'),
        flat_json.get('fields.System.ChangedBy.UniqueName'),
        flat_json.get('fields.System.CommentCount'),
        flat_json.get('fields.System.Title'),
        flat_json.get('fields.System.BoardColumn'),
        int(flat_json.get('fields.System.BoardColumnDone', False)),  # Convert boolean to integer
        flat_json.get('fields.Microsoft.VSTS.Common.StateChangeDate'),
        flat_json.get('fields.Microsoft.VSTS.Common.ClosedDate'),
        flat_json.get('fields.Microsoft.VSTS.Common.ClosedBy.DisplayName'),
        flat_json.get('fields.Microsoft.VSTS.Common.ClosedBy.Url'),
        flat_json.get('fields.Microsoft.VSTS.Common.Priority'),
        flat_json.get('fields.Microsoft.VSTS.Common.ValueArea'),
        flat_json.get('fields.System.Tags'),
        flat_json.get('url')
    )

    # Execute the insert query
    c.execute(query, values)

    # Commit and close the connection
    conn.commit()
    conn.close()

def insert_pbis_into_db(json_data):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Flatten the JSON structure for insertion
    assigned_to = json_data['fields'].get('System.AssignedTo', {})
    created_by = json_data['fields'].get('System.CreatedBy', {})
    changed_by = json_data['fields'].get('System.ChangedBy', {})
    closed_by = json_data['fields'].get('Microsoft.VSTS.Common.ClosedBy', {})
    activated_by = json_data['fields'].get('Microsoft.VSTS.Common.ActivatedBy', {})

    # Insert into WorkItem table
    cursor.execute('''
        INSERT INTO productbacklogitems (
            id, rev, area_path, team_project, iteration_path, work_item_type, state, reason, 
            assigned_to_name, assigned_to_id, assigned_to_email, assigned_to_url, assigned_to_avatar_url,
            created_date, created_by_name, created_by_id, created_by_email, created_by_url, created_by_avatar_url, 
            changed_date, changed_by_name, changed_by_id, changed_by_email, changed_by_url, changed_by_avatar_url, 
            closed_date, closed_by_name, closed_by_id, closed_by_email, closed_by_url, closed_by_avatar_url, 
            comment_count, title, board_column, board_column_done, state_change_date, activated_date, 
            activated_by_name, activated_by_id, activated_by_email, activated_by_url, activated_by_avatar_url,
            priority, value_area, business_value, effort, backlog_priority, kanban_column, 
            kanban_column_done, ea_number, description, acceptance_criteria, url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        json_data['id'], json_data['rev'],
        json_data['fields'].get('System.AreaPath'),
        json_data['fields'].get('System.TeamProject'),
        json_data['fields'].get('System.IterationPath'),
        json_data['fields'].get('System.WorkItemType'),
        json_data['fields'].get('System.State'),
        json_data['fields'].get('System.Reason'),
        assigned_to.get('displayName'),
        assigned_to.get('id'),
        assigned_to.get('uniqueName'),
        assigned_to.get('url'),
        assigned_to.get('_links', {}).get('avatar', {}).get('href'),
        json_data['fields'].get('System.CreatedDate'),
        created_by.get('displayName'),
        created_by.get('id'),
        created_by.get('uniqueName'),
        created_by.get('url'),
        created_by.get('_links', {}).get('avatar', {}).get('href'),
        json_data['fields'].get('System.ChangedDate'),
        changed_by.get('displayName'),
        changed_by.get('id'),
        changed_by.get('uniqueName'),
        changed_by.get('url'),
        changed_by.get('_links', {}).get('avatar', {}).get('href'),
        json_data['fields'].get('Microsoft.VSTS.Common.ClosedDate'),
        closed_by.get('displayName'),
        closed_by.get('id'),
        closed_by.get('uniqueName'),
        closed_by.get('url'),
        closed_by.get('_links', {}).get('avatar', {}).get('href'),
        json_data['fields'].get('System.CommentCount'),
        json_data['fields'].get('System.Title'),
        json_data['fields'].get('System.BoardColumn'),
        json_data['fields'].get('System.BoardColumnDone'),
        json_data['fields'].get('Microsoft.VSTS.Common.StateChangeDate'),
        json_data['fields'].get('Microsoft.VSTS.Common.ActivatedDate'),
        activated_by.get('displayName'),
        activated_by.get('id'),
        activated_by.get('uniqueName'),
        activated_by.get('url'),
        activated_by.get('_links', {}).get('avatar', {}).get('href'),
        json_data['fields'].get('Microsoft.VSTS.Common.Priority'),
        json_data['fields'].get('Microsoft.VSTS.Common.ValueArea'),
        json_data['fields'].get('Microsoft.VSTS.Common.BusinessValue'),
        json_data['fields'].get('Microsoft.VSTS.Scheduling.Effort'),
        json_data['fields'].get('Microsoft.VSTS.Common.BacklogPriority'),
        json_data['fields'].get('WEF_43428808543C421CB964EE5CB8995594_Kanban.Column'),
        json_data['fields'].get('WEF_43428808543C421CB964EE5CB8995594_Kanban.Column.Done'),
        json_data['fields'].get('Custom.EANumber'),
        json_data['fields'].get('System.Description'),
        json_data['fields'].get('Microsoft.VSTS.Common.AcceptanceCriteria'),
        json_data['url']
    ))

    # Commit the transaction
    conn.commit()
    conn.close()
# Function to refresh data by hitting the API and updating the database
def refresh_data(work_item_type):
    start_date = datetime(2020, 1, 1)
    end_date = datetime.today()  # Set end_date to today's date

    # Fetch work item IDs
    work_item_ids = fetch_work_item_ids(start_date, end_date, work_item_type)


    # Fetch work item details and update the database
    if work_item_ids:
        id_list = [item['id'] for item in work_item_ids]
        id_chunks = list(utilities.chunk_list(id_list, chunk_size=100))
        for chunks in id_chunks:
            all_work_items = fetch_work_item_details(chunks)
            match work_item_type:
                case "Project":
                    insert_projects_into_db(all_work_items)
                case "Epic":
                # Insert each work item into the pre-defined table
                    for work_item in all_work_items:
                        insert_epics_into_db(work_item)    
                case "Feature":
                # Insert each work item into the pre-defined table
                    for work_item in all_work_items:
                        insert_features_into_db(work_item) 
                case "Product Backlog Item":
                    for work_item in all_work_items:
                        insert_pbis_into_db(work_item)
    else:
        pass
        # st.warning(f"No {work_item_type} items found for the given criteria.")

def getDataFromDevops():
    # Initialize the database
    init_db()
    # refresh_data('Project')
    #refresh_data('Epic')
    #refresh_data('Feature')
    refresh_data('Product Backlog Item')
    
   


getDataFromDevops()


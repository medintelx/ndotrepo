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
WORK_ITEMS_BATCH_URL = f"https://dev.azure.com/{ORGANIZATION}/_apis/wit/workitemsbatch?api-version=7.2-preview"

# SQLite database setup
DB_NAME = 'NDOTDATA.db'

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Create table for Projects
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS projects (
    Work_Item_ID INTEGER,
    Area_ID INTEGER,
    Area_Path TEXT,
    Team_Project TEXT,
    Node_Name TEXT,
    Area_Level_1 TEXT,
    Revision INTEGER,
    Authorized_Date TEXT,
    Revised_Date TEXT,
    Iteration_ID INTEGER,
    Iteration_Path TEXT,
    Iteration_Level_1 TEXT,
    Work_Item_Type TEXT,
    State TEXT,
    Reason_for_State_Change TEXT,
    Assigned_To TEXT,
    Person_ID INTEGER,
    Watermark INTEGER,
    Comment_Count INTEGER,
    Title TEXT,
    Board_Column TEXT,
    Is_Board_Column_Done BOOLEAN,
    State_Change_Date TEXT,
    Business_Value INTEGER,
    Backlog_Priority REAL,
    Health TEXT,
    Scoping_30_Percent TEXT,
    SeventyFivePercentComplete TEXT,
    Intermediate_Date TEXT,
    QAQC_Submittal_Date TEXT,
    Document_Submittal_Date TEXT,
    Extension_Marker BOOLEAN,
    Kanban_Column TEXT,
    Kanban_Column_Done BOOLEAN,
    EA_Number TEXT,
    Priority_Traffic_Ops TEXT,
    Fiscal_Year TEXT,
    Funding_Source TEXT,
    Route_Type TEXT,
    Construction_EA_Number TEXT,
    Official_DOC_Date TEXT,
    Official_Advertise_Date TEXT,
    Anchor_Project BOOLEAN,
    Complexity_Signals BOOLEAN,
    Complexity_Lighting BOOLEAN,
    Complexity_ITS BOOLEAN,
    Complexity_Power_Design BOOLEAN,
    Complexity_RoW_Coordination BOOLEAN,
    Complexity_SLI_Project_Lead BOOLEAN,
    Complexity_Solar_Design BOOLEAN,
    Complexity_Trunkline BOOLEAN
        )
    ''')

     # Create the table with pre-defined columns based on the flattened JSON structure
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS epics (
             System_Id INTEGER PRIMARY KEY,                  -- Work item ID
    System_AreaPath TEXT,                           -- Area path
    System_TeamProject TEXT,                        -- Team project name
    System_IterationPath TEXT,                      -- Iteration path
    System_WorkItemType TEXT,                       -- Work item type (e.g., Feature, Bug)
    System_State TEXT,                              -- Work item state (e.g., New, Active)
    System_Reason TEXT,                             -- Reason for state change
    System_CreatedDate TEXT,                        -- Created date (stored as TEXT)
    System_ChangedDate TEXT,                        -- Last changed date (stored as TEXT)
    System_Title TEXT,                              -- Title of the work item
    System_BoardColumn TEXT,                        -- Board column name
    System_BoardColumnDone BOOLEAN,                 -- Is the board column done (TRUE/FALSE)
    Microsoft_VSTS_Common_StateChangeDate TEXT,     -- State change date (stored as TEXT)
    Microsoft_VSTS_Common_Priority INTEGER,         -- Work item priority
    Microsoft_VSTS_Common_ValueArea TEXT,           -- Value area (e.g., Business, Architectural)
    Microsoft_VSTS_Common_BusinessValue INTEGER,    -- Business value
    Microsoft_VSTS_Common_BacklogPriority REAL,     -- Backlog priority (floating-point number)
    Custom_EANumber TEXT,                           -- Custom EA number
    System_Parent INTEGER                           -- Parent work item ID (foreign key)
        )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS features (
        system_Id INTEGER PRIMARY KEY,                  -- Work item ID (Primary key)
    System_AreaPath TEXT,                           -- Area path
    System_TeamProject TEXT,                        -- Team project name
    System_IterationPath TEXT,                      -- Iteration path
    System_WorkItemType TEXT,                       -- Work item type (e.g., Feature, Bug)
    System_State TEXT,                              -- Work item state (e.g., New, Active)
    System_Reason TEXT,                             -- Reason for state change
    System_CreatedDate TEXT,                        -- Created date (stored as TEXT)
    System_ChangedDate TEXT,                        -- Last changed date (stored as TEXT)
    System_Title TEXT,                              -- Title of the work item
    System_BoardColumn TEXT,                        -- Board column name
    System_BoardColumnDone BOOLEAN,                 -- Is the board column done (TRUE/FALSE)
    Microsoft_VSTS_Common_StateChangeDate TEXT,     -- State change date (stored as TEXT)
    Microsoft_VSTS_Common_Priority INTEGER,         -- Work item priority
    Microsoft_VSTS_Common_ValueArea TEXT,           -- Value area (e.g., Business, Architectural)
    Microsoft_VSTS_Common_BusinessValue INTEGER,    -- Business value
    Microsoft_VSTS_Common_BacklogPriority REAL,     -- Backlog priority (floating-point number)
    Custom_EANumber TEXT,                           -- Custom EA number
    System_Parent INTEGER                           -- Parent work item ID (foreign key)
    )
''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS  productbacklogitems (
    System_Id INTEGER PRIMARY KEY,                -- Primary key for the work item
    System_AreaId INTEGER,                        -- Area ID
    System_AreaPath TEXT,                         -- Area path (text)
    System_TeamProject TEXT,                      -- Team project name
    System_NodeName TEXT,                         -- Node name
    System_AreaLevel1 TEXT,                       -- Area level 1
    System_Rev INTEGER,                           -- Revision number
    System_AuthorizedDate TEXT,                   -- Authorized date (stored as TEXT for date formatting)
    System_RevisedDate TEXT,                      -- Revised date (stored as TEXT for date formatting)
    System_IterationId INTEGER,                   -- Iteration ID
    System_IterationPath TEXT,                    -- Iteration path (text)
    System_IterationLevel1 TEXT,                  -- Iteration level 1
    System_WorkItemType TEXT,                     -- Work item type (e.g., Bug, Feature)
    System_State TEXT,                            -- Current state (e.g., New, Active)
    System_Reason TEXT,                           -- Reason for state change
    System_Parent INTEGER,                        -- Parent work item ID (foreign key)
    System_CreatedDate TEXT,                      -- Created date (stored as TEXT)
    System_ChangedDate TEXT,                      -- Last changed date (stored as TEXT)
    System_PersonId INTEGER,                      -- ID of the person associated with the work item
    System_Watermark INTEGER,                     -- Watermark field
    System_CommentCount INTEGER,                  -- Number of comments
    System_Title TEXT,                            -- Title of the work item
    System_BoardColumn TEXT,                      -- Board column name
    System_BoardColumnDone BOOLEAN,               -- Is the board column done (TRUE/FALSE)
    Microsoft_VSTS_Common_StateChangeDate TEXT,   -- State change date (stored as TEXT)
    Microsoft_VSTS_Common_Priority INTEGER,       -- Work item priority (numeric)
    Microsoft_VSTS_Common_ValueArea TEXT,         -- Value area (e.g., Business, Architectural)
    Microsoft_VSTS_Common_BusinessValue INTEGER,  -- Business value
    Microsoft_VSTS_Scheduling_Effort REAL,        -- Effort in hours (floating point)
    Microsoft_VSTS_Common_BacklogPriority REAL,   -- Backlog priority (floating point)
    Custom_EANumber TEXT                          -- Custom EA number (text)
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
def fetch_work_item_details(ids, worditemtype):
    print(len(ids))
    print(ids)
    if not ids:
        return []
    if (worditemtype=='Project'):
        payload = {
        "ids": ids,  # List of work item IDs,
        "fields": [
    "System.Id",
    "System.AreaId",
    "System.AreaPath",
    "System.TeamProject",
    "System.NodeName",
    "System.AreaLevel1",
    "System.Rev",
    "System.AuthorizedDate",
    "System.RevisedDate",
    "System.IterationId",
    "System.IterationPath",
    "System.IterationLevel1",
    "System.WorkItemType",
    "System.State",
    "System.Reason",
    "System.PersonId",
    "System.Watermark",
    "System.CommentCount",
    "System.Title",
    "System.BoardColumn",
    "System.BoardColumnDone",
    "Microsoft.VSTS.Common.StateChangeDate",
    "Microsoft.VSTS.Common.BusinessValue",
    "Microsoft.VSTS.Common.BacklogPriority",
    "Custom.Health",
    "Custom.30PercentScoping",
    "Custom.75PercentComplete",
    "Custom.IntermediateDate",
    "Custom.QAQCSubmittalDate",
    "Custom.DocumentSubmittalDate",
    "Custom.EANumber",
    "Custom.PriorityTrafficOps",
    "Custom.FiscalYear",
    "Custom.FundingSource",
    "Custom.RouteType",
    "Custom.ConstructionEANumber",
    "Custom.OfficialDOCDate",
    "Custom.OfficialAdvertiseDate",
    "Custom.AnchorProject",
    "Custom.Complexity_Signals",
    "Custom.Complexity_Lighting",
    "Custom.Complexity_ITS",
    "Custom.Complexity_Power_Design",
    "Custom.Complexity_RoW_Coordination",
    "Custom.Complexity_SLI_Project_Lead",
    "Custom.Complexity_Solar_Design",
    "Custom.Complexity_Trunkline"
]
        }
    elif (worditemtype=='Epic'):
        payload = {
            "ids": ids,  # List of work item IDs,
            "fields": ["system.Id","System.AreaPath", "System.TeamProject", "System.IterationPath", "System.WorkItemType", "System.State", "System.Reason", "System.CreatedDate", "System.ChangedDate", "System.Title", "System.BoardColumn", "System.BoardColumnDone", "Microsoft.VSTS.Common.StateChangeDate", "Microsoft.VSTS.Common.Priority", "Microsoft.VSTS.Common.ValueArea", "Microsoft.VSTS.Common.BusinessValue", "Microsoft.VSTS.Common.BacklogPriority", "Custom.EANumber", "System.Parent"]
            }
    elif (worditemtype=='Feature'):
        payload = {
    "ids": ids,  # List of work item IDs,
    "fields": ["system.Id","System.AreaPath", "System.TeamProject", "System.IterationPath", "System.WorkItemType", "System.State", "System.Reason", "System.CreatedDate", "System.ChangedDate", "System.Title", "System.BoardColumn", "System.BoardColumnDone", "Microsoft.VSTS.Common.StateChangeDate", "Microsoft.VSTS.Common.Priority", "Microsoft.VSTS.Common.ValueArea", "Microsoft.VSTS.Common.BusinessValue", "Microsoft.VSTS.Common.BacklogPriority", "Custom.EANumber", "System.Parent"]
}
    elif (worditemtype=='Product Backlog Item'):
        payload = {
    "ids": ids,  # List of work item IDs,
    "fields": [
        "System.Id", "System.AreaId", "System.AreaPath", "System.TeamProject", "System.NodeName", "System.AreaLevel1", "System.Rev", "System.AuthorizedDate", "System.RevisedDate", "System.IterationId", "System.IterationPath", "System.IterationLevel1", "System.WorkItemType", "System.State", "System.Reason",
        "System.Parent","System.CreatedDate","System.ChangedDate","System.PersonId", "System.Watermark", "System.CommentCount", "System.Title", "System.BoardColumn", "System.BoardColumnDone", "Microsoft.VSTS.Common.StateChangeDate", "Microsoft.VSTS.Common.Priority", "Microsoft.VSTS.Common.ValueArea", "Microsoft.VSTS.Common.BusinessValue", "Microsoft.VSTS.Scheduling.Effort", "Microsoft.VSTS.Common.BacklogPriority", "Custom.EANumber"
           
    ]
}
    url = WORK_ITEMS_BATCH_URL
    response = requests.post(url, json=payload, auth=HTTPBasicAuth('', PAT))

    if response.status_code == 200:
        print(response.json())
        return response.json()['value']
    else:
        # st.error(f"Failed to fetch work item details: {response.status_code}")
        # st.error(response.json())
        return []
    


# Function to insert work items into the SQLite database table
def insert_projects_into_db(data):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Insert data into the table
    for entry in data:
        fields = entry['fields']
    


# Prepare the SQL insert statement with placeholders
        sql_insert_statement = '''
        INSERT INTO projects (
        Work_Item_ID, Area_ID, Area_Path, Team_Project, Node_Name, Area_Level_1, Revision, 
        Authorized_Date, Revised_Date, Iteration_ID, Iteration_Path, Iteration_Level_1, Work_Item_Type, 
        State, Reason_for_State_Change, Assigned_To, Person_ID, Watermark, Comment_Count, 
        Title, Board_Column, Is_Board_Column_Done, State_Change_Date, Business_Value, 
        Backlog_Priority, Health, Scoping_30_Percent, Intermediate_Date, SeventyFivePercentComplete,QAQC_Submittal_Date, 
        Document_Submittal_Date, Extension_Marker, Kanban_Column, Kanban_Column_Done, 
        EA_Number, Priority_Traffic_Ops, Fiscal_Year, Funding_Source, Route_Type, 
        Construction_EA_Number, Official_DOC_Date, Official_Advertise_Date, Anchor_Project, 
        Complexity_Signals, Complexity_Lighting, Complexity_ITS, Complexity_Power_Design, 
        Complexity_RoW_Coordination, Complexity_SLI_Project_Lead, Complexity_Solar_Design, 
        Complexity_Trunkline
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
'''

# Prepare the values from JSON for insertion
        values = (
        fields.get('System.Id', None),
    fields.get('System.AreaId', None),
    fields.get('System.AreaPath', ''),
    fields.get('System.TeamProject', ''),
    fields.get('System.NodeName', ''),
    fields.get('System.AreaLevel1', ''),
    fields.get('System.Rev', None),
    fields.get('System.AuthorizedDate', None),
    fields.get('System.RevisedDate', None),
    fields.get('System.IterationId', None),
    fields.get('System.IterationPath', ''),
    fields.get('System.IterationLevel1', ''),
    fields.get('System.WorkItemType', ''),
    fields.get('System.State', ''),
    fields.get('System.Reason', ''),
    fields.get('System.AssignedTo', {}).get('displayName', None),
    fields.get('System.PersonId', None),
    fields.get('System.Watermark', None),
    fields.get('System.CommentCount', None),
    fields.get('System.Title', ''),
    fields.get('System.BoardColumn', ''),
    int(fields.get('System.BoardColumnDone', False)),
    fields.get('Microsoft.VSTS.Common.StateChangeDate', None),
    fields.get('Microsoft.VSTS.Common.BusinessValue', None),
    fields.get('Microsoft.VSTS.Common.BacklogPriority', None),
    fields.get('Custom.Health', 'Unknown'),
    fields.get('Custom.30PercentScoping', None),
    fields.get('Custom.IntermediateDate', None),
    fields.get('Custom.75PercentComplete', None),
    fields.get('Custom.QAQCSubmittalDate', None),
    fields.get('Custom.DocumentSubmittalDate', None),
    int(fields.get('WEF_3EF069D225F848D0A794779F40639E36_System.ExtensionMarker', False)),
    fields.get('WEF_3EF069D225F848D0A794779F40639E36_Kanban.Column', None),
    int(fields.get('WEF_3EF069D225F848D0A794779F40639E36_Kanban.Column.Done', False)),
    fields.get('Custom.EANumber', None),
    fields.get('Custom.PriorityTrafficOps', None),
    fields.get('Custom.FiscalYear', None),
    fields.get('Custom.FundingSource', None),
    fields.get('Custom.RouteType', None),
    fields.get('Custom.ConstructionEANumber', None),
    fields.get('Custom.OfficialDOCDate', None),
    fields.get('Custom.OfficialAdvertiseDate', None),
    int(fields.get('Custom.AnchorProject', False)),
    int(fields.get('Custom.Complexity_Signals', False)),
    int(fields.get('Custom.Complexity_Lighting', False)),
    int(fields.get('Custom.Complexity_ITS', False)),
    int(fields.get('Custom.Complexity_Power_Design', False)),
    int(fields.get('Custom.Complexity_RoW_Coordination', False)),
    int(fields.get('Custom.Complexity_SLI_Project_Lead', False)),
    int(fields.get('Custom.Complexity_Solar_Design', False)),
    int(fields.get('Custom.Complexity_Trunkline', False))
)

# Execute the insert statement with values
        cursor.execute(sql_insert_statement, values)

# Commit the transaction and close the connection
    conn.commit()
    conn.close()


# Function to insert a flattened work item into the SQLite database
def insert_epics_into_db(data):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
     # Insert data into the table
    for entry in data:
        fields = entry['fields']
    


# Prepare the SQL insert statement with placeholders
        sql_insert_statement = '''
         INSERT INTO epics (
        System_Id, System_AreaPath, System_TeamProject, System_IterationPath,
        System_WorkItemType, System_State, System_Reason, System_CreatedDate,
        System_ChangedDate, System_Title, System_BoardColumn, System_BoardColumnDone,
        Microsoft_VSTS_Common_StateChangeDate, Microsoft_VSTS_Common_Priority,
        Microsoft_VSTS_Common_ValueArea, Microsoft_VSTS_Common_BacklogPriority, System_Parent
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) '''

# Prepare the values from JSON for insertion
        values = (
        fields.get('System.Id'),
        fields.get('System.AreaPath'),
        fields.get('System.TeamProject'),
        fields.get('System.IterationPath'),
        fields.get('System.WorkItemType'),
        fields.get('System.State'),
        fields.get('System.Reason'),
        fields.get('System.CreatedDate'),
        fields.get('System.ChangedDate'),
        fields.get('System.Title'),
        fields.get('System.BoardColumn'),
        int(fields.get('System.BoardColumnDone', False)),  # Convert boolean to int
        fields.get('Microsoft.VSTS.Common.StateChangeDate'),
        fields.get('Microsoft.VSTS.Common.Priority'),
        fields.get('Microsoft.VSTS.Common.ValueArea'),
        fields.get('Microsoft.VSTS.Common.BacklogPriority'),
        fields.get('System.Parent')
    )
    
        cursor.execute(sql_insert_statement, values)



# Commit the transaction and close the connection
    conn.commit()
    conn.close()



# Function to insert the JSON fields into the SQLite table
def insert_features_into_db(data):

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
     # Insert data into the table
    for entry in data:
        fields = entry['fields']
    


# Prepare the SQL insert statement with placeholders
        sql_insert_statement = '''
        INSERT INTO features (
        system_Id, System_AreaPath, System_TeamProject, System_IterationPath,
        System_WorkItemType, System_State, System_Reason, System_CreatedDate,
        System_ChangedDate, System_Title, System_BoardColumn, System_BoardColumnDone,
        Microsoft_VSTS_Common_StateChangeDate, Microsoft_VSTS_Common_Priority,
        Microsoft_VSTS_Common_ValueArea, Microsoft_VSTS_Common_BacklogPriority, System_Parent
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
'''
        values = (
# Prepare the values from JSON for insertion
        fields.get('System.Id', None),  # Assign None if not present
        fields.get('System.AreaPath', ""),  # Empty string if not present
        fields.get('System.TeamProject', ""),  # Empty string if not present
        fields.get('System.IterationPath', ""),  # Empty string if not present
        fields.get('System.WorkItemType', ""),  # Empty string if not present
        fields.get('System.State', ""),  # Empty string if not present
        fields.get('System.Reason', ""),  # Empty string if not present
        fields.get('System.CreatedDate', ""),  # Empty string if not present
        fields.get('System.ChangedDate', ""),  # Empty string if not present
        fields.get('System.Title', ""),  # Empty string if not present
        fields.get('System.BoardColumn', ""),  # Empty string if not present
        int(fields.get('System.BoardColumnDone', 0)),  # Default to 0 (False) if not present
        fields.get('Microsoft.VSTS.Common.StateChangeDate', ""),  # Empty string if not present
        fields.get('Microsoft.VSTS.Common.Priority', 0),  # Default to 0 if not present
        fields.get('Microsoft.VSTS.Common.ValueArea', ""),  # Empty string if not present
        fields.get('Microsoft.VSTS.Common.BacklogPriority', 0.0),  # Default to 0.0 if not present
        fields.get('System.Parent', None)  # Assign None if not present
    )
    
        cursor.execute(sql_insert_statement, values)



# Commit the transaction and close the connection
    conn.commit()
    conn.close()

def insert_pbis_into_db(data):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
     # Insert data into the table
    for entry in data:
        fields = entry['fields']
    


# Prepare the SQL insert statement with placeholders
        sql_insert_statement = '''
        INSERT INTO productbacklogitems (
        System_Id, System_AreaId, System_AreaPath, System_TeamProject, 
        System_NodeName, System_AreaLevel1, System_Rev, System_AuthorizedDate, 
        System_RevisedDate, System_IterationId, System_IterationPath, System_IterationLevel1, 
        System_WorkItemType, System_State, System_Reason, System_Parent, 
        System_CreatedDate, System_ChangedDate, System_PersonId, System_Watermark, 
        System_CommentCount, System_Title, System_BoardColumn, System_BoardColumnDone, 
        Microsoft_VSTS_Common_StateChangeDate, Microsoft_VSTS_Common_Priority, 
        Microsoft_VSTS_Common_ValueArea, Microsoft_VSTS_Common_BusinessValue, 
        Microsoft_VSTS_Scheduling_Effort, Microsoft_VSTS_Common_BacklogPriority, Custom_EANumber
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
'''

        values = (
        fields.get('System.Id', None),
        fields.get('System.AreaId', None),
        fields.get('System.AreaPath', ""),
        fields.get('System.TeamProject', ""),
        fields.get('System.NodeName', ""),
        fields.get('System.AreaLevel1', ""),
        fields.get('System.Rev', None),
        fields.get('System.AuthorizedDate', ""),
        fields.get('System.RevisedDate', ""),
        fields.get('System.IterationId', None),
        fields.get('System.IterationPath', ""),
        fields.get('System.IterationLevel1', ""),
        fields.get('System.WorkItemType', ""),
        fields.get('System.State', ""),
        fields.get('System.Reason', ""),
        fields.get('System.Parent', None),
        fields.get('System.CreatedDate', ""),
        fields.get('System.ChangedDate', ""),
        fields.get('System.PersonId', None),
        fields.get('System.Watermark', None),
        fields.get('System.CommentCount', 0),
        fields.get('System.Title', ""),
        fields.get('System.BoardColumn', ""),
        int(fields.get('System.BoardColumnDone', False)),  # Convert boolean to int (0 or 1)
        fields.get('Microsoft.VSTS.Common.StateChangeDate', ""),
        fields.get('Microsoft.VSTS.Common.Priority', 0),
        fields.get('Microsoft.VSTS.Common.ValueArea', ""),
        fields.get('Microsoft.VSTS.Common.BusinessValue', None),
        fields.get('Microsoft.VSTS.Scheduling.Effort', 0.0),
        fields.get('Microsoft.VSTS.Common.BacklogPriority', 0.0),
        fields.get('Custom.EANumber', "")
    )
        cursor.execute(sql_insert_statement, values)



# Commit the transaction and close the connection
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
            all_work_items = fetch_work_item_details(chunks,work_item_type)
            match work_item_type:
                case "Project":
                    insert_projects_into_db(all_work_items)
                case "Epic":
                    insert_epics_into_db(all_work_items)    
                case "Feature":
                    insert_features_into_db(all_work_items) 
                case "Product Backlog Item":
                    insert_pbis_into_db(all_work_items)
    else:
        pass
        # st.warning(f"No {work_item_type} items found for the given criteria.")

def getDataFromDevops():
    # Initialize the database
    init_db()
    refresh_data('Project')
    refresh_data('Epic')
    refresh_data('Feature')
    refresh_data('Product Backlog Item')
    
   


getDataFromDevops()


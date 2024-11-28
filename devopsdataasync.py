import asyncio
import aiohttp
import sqlite3
import base64
import os
from datetime import datetime
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv(override=True)

# Azure DevOps settings
ORGANIZATION = os.getenv('AZURE_DEVOPS_ORGANIZATION')
PROJECT = os.getenv('AZURE_DEVOPS_PROJECT')
PAT = os.getenv('AZURE_DEVOPS_PAT')

WIQL_URL = os.getenv('WIQL_URL')
WORK_ITEMS_BATCH_URL = os.getenv('WORK_ITEMS_BATCH_URL')
ITERATIONS_API_URL = os.getenv('ITERATIONS_API_URL')

DB_NAME = os.getenv('DB_PATH')


# Helper to encode PAT to Base64
def encode_pat_to_base64(pat):
    """Encodes the PAT to Base64 format for Azure DevOps API."""
    pat_with_colon = f"{pat}:"
    return base64.b64encode(pat_with_colon.encode('utf-8')).decode('utf-8')


# Format date from ISO 8601 to YYYY-MM-DD
def format_date(date_str):
    """Format date from ISO 8601 to YYYY-MM-DD."""
    if date_str:
        return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d")
    return None


# Asynchronous method to fetch iteration data
async def fetch_iteration_data():
    """Fetch iteration data from Azure DevOps API asynchronously."""
    encoded_pat = encode_pat_to_base64(PAT)
    headers = {"Authorization": f"Basic {encoded_pat}"}

    async with aiohttp.ClientSession() as session:
        async with session.get(ITERATIONS_API_URL, headers=headers) as response:
            if response.status == 200:
                return await response.json()
            else:
                print(f"Failed to fetch iteration data: {response.status} - {await response.text()}")
                return None


# Asynchronous method to fetch work item IDs
async def fetch_work_item_ids(start_date, end_date, work_item_type):
    """Fetch work item IDs asynchronously."""
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    if work_item_type == 'Product Backlog Item':
        query = {
            "query": f"Select [System.Id] From WorkItems Where [System.WorkItemType] = '{work_item_type}' "
                     f"And [System.CreatedDate] >= '{start_date_str}' And [System.CreatedDate] <= '{end_date_str}' "
                     f"And [System.State] IN ('New', 'Approved', 'Committed')"
        }
    else:
        query = {
            "query": f"Select [System.Id] From WorkItems Where [System.WorkItemType] = '{work_item_type}' "
                     f"And [System.CreatedDate] >= '{start_date_str}' And [System.CreatedDate] <= '{end_date_str}'"
        }

    encoded_pat = encode_pat_to_base64(PAT)
    headers = {"Authorization": f"Basic {encoded_pat}"}

    async with aiohttp.ClientSession() as session:
        async with session.post(WIQL_URL, json=query, headers=headers) as response:
            if response.status == 200:
                return (await response.json()).get("workItems", [])
            else:
                print(f"Failed to fetch work item IDs: {response.status} - {await response.text()}")
                return []


# Asynchronous method to fetch work item details
async def fetch_work_item_details(ids, work_item_type):
    """Fetch work item details asynchronously."""
    if not ids:
        return []

    encoded_pat = encode_pat_to_base64(PAT)
    headers = {"Authorization": f"Basic {encoded_pat}"}

    # Define fields based on work item type
    fields = {
        "Project": [
            "System.Id", "System.AreaId", "System.AreaPath", "System.TeamProject",
            "System.NodeName", "System.AreaLevel1", "System.Rev", "System.AuthorizedDate",
            "System.RevisedDate", "System.IterationId", "System.IterationPath",
            "System.IterationLevel1", "System.WorkItemType", "System.State", "System.Reason",
            "System.PersonId", "System.Watermark", "System.CommentCount", "System.Title",
            "System.BoardColumn", "System.BoardColumnDone", "Microsoft.VSTS.Common.StateChangeDate",
            "Microsoft.VSTS.Common.BusinessValue", "Microsoft.VSTS.Common.BacklogPriority",
            "Custom.Health", "Custom.30PercentScoping", "Custom.75PercentComplete",
            "Custom.IntermediateDate", "Custom.QAQCSubmittalDate", "Custom.DocumentSubmittalDate",
            "Custom.EANumber", "Custom.PriorityTrafficOps", "Custom.FiscalYear", "Custom.FundingSource",
            "Custom.RouteType", "Custom.ConstructionEANumber", "Custom.OfficialDOCDate",
            "Custom.OfficialAdvertiseDate", "Custom.AnchorProject", "Custom.Complexity_Signals",
            "Custom.Complexity_Lighting", "Custom.Complexity_ITS", "Custom.Complexity_Power_Design",
            "Custom.Complexity_RoW_Coordination", "Custom.Complexity_SLI_Project_Lead",
            "Custom.Complexity_Solar_Design", "Custom.Complexity_Trunkline"
        ],
        "Epic": [
            "System.Id", "System.AreaPath", "System.TeamProject", "System.IterationPath",
            "System.WorkItemType", "System.State", "System.Reason", "System.CreatedDate",
            "System.ChangedDate", "System.Title", "System.BoardColumn", "System.BoardColumnDone",
            "Microsoft.VSTS.Common.StateChangeDate", "Microsoft.VSTS.Common.Priority",
            "Microsoft.VSTS.Common.ValueArea", "Microsoft.VSTS.Common.BacklogPriority",
            "Custom.EANumber", "System.Parent"
        ],
        "Feature": [
            "System.Id", "System.AreaPath", "System.TeamProject", "System.IterationPath",
            "System.WorkItemType", "System.State", "System.Reason", "System.CreatedDate",
            "System.ChangedDate", "System.Title", "System.BoardColumn", "System.BoardColumnDone",
            "Microsoft.VSTS.Common.StateChangeDate", "Microsoft.VSTS.Common.Priority",
            "Microsoft.VSTS.Common.ValueArea", "Microsoft.VSTS.Common.BacklogPriority",
            "Custom.EANumber", "System.Parent"
        ],
        "Product Backlog Item": [
            "System.Id", "System.AreaId", "System.AreaPath", "System.TeamProject",
            "System.NodeName", "System.AreaLevel1", "System.Rev", "System.AuthorizedDate",
            "System.RevisedDate", "System.IterationId", "System.IterationPath",
            "System.IterationLevel1", "System.WorkItemType", "System.State", "System.Reason",
            "System.Parent", "System.CreatedDate", "System.ChangedDate", "System.PersonId",
            "System.Watermark", "System.CommentCount", "System.Title", "System.BoardColumn",
            "System.BoardColumnDone", "Microsoft.VSTS.Common.StateChangeDate",
            "Microsoft.VSTS.Common.Priority", "Microsoft.VSTS.Common.ValueArea",
            "Microsoft.VSTS.Common.BusinessValue", "Microsoft.VSTS.Scheduling.Effort",
            "Microsoft.VSTS.Common.BacklogPriority", "Custom.EANumber"
        ]
    }.get(work_item_type, [])

    payload = {"ids": ids, "fields": fields}

    async with aiohttp.ClientSession() as session:
        async with session.post(WORK_ITEMS_BATCH_URL, json=payload, headers=headers) as response:
            if response.status == 200:
                return (await response.json()).get('value', [])
            else:
                print(f"Failed to fetch work item details: {response.status} - {await response.text()}")
                return []


# Function to parse iteration data
def parse_iteration_data(response):
    """Parse API response and extract iteration details."""
    iterations = []
    for item in response.get('value', []):
        if item.get('structureType') == 'iteration':
            for child in item.get('children', []):
                iterations.append({
                    'name': child['name'],
                    'start_date': format_date(child.get('attributes', {}).get('startDate')),
                    'finish_date': format_date(child.get('attributes', {}).get('finishDate')),
                    'path': child['path']
                })
    return iterations


# Function to insert or update iterations into the database
def insert_or_update_iterations(db_path, iterations):
    """Insert or update iteration data into the database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for iteration in iterations:
        cursor.execute("""
            INSERT INTO iterations (Iteration, Start_date, End_date, modified_date)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(Iteration) DO UPDATE SET
                Start_date = excluded.Start_date,
                End_date = excluded.End_date,
                modified_date = excluded.modified_date
        """, (iteration['name'], iteration['start_date'], iteration['finish_date'], datetime.now()))

    conn.commit()
    conn.close()
def format_date(date_value):
    if date_value:
        try:
            # Handle ISO 8601 format with fractional seconds and Z (Zulu time)
            if date_value.endswith('Z'):
                date_value = date_value[:-1]  # Remove the 'Z'
                return datetime.strptime(date_value, '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S')
            else:
                # Handle ISO 8601 without 'Z'
                return datetime.strptime(date_value, '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            # Log error and return None if the date format is invalid
            print(f"Invalid date format: {date_value}")
            return None
    return None

# Function to insert work items into the SQLite database table
def insert_projects_into_db(data):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # SQL insert or update on conflict statement
    sql_insert_statement = '''
        INSERT INTO projects (
            Work_Item_ID, Area_ID, Area_Path, Team_Project, Node_Name, Area_Level_1, Revision, 
            Authorized_Date, Revised_Date, Iteration_ID, Iteration_Path, Iteration_Level_1, Work_Item_Type, 
            State, Reason_for_State_Change, Assigned_To, Person_ID, Watermark, Comment_Count, 
            Title, Board_Column, Is_Board_Column_Done, State_Change_Date, Business_Value, 
            Backlog_Priority, Health, Scoping_30_Percent, Intermediate_Date, SeventyFivePercentComplete, QAQC_Submittal_Date, 
            Document_Submittal_Date, Extension_Marker, Kanban_Column, Kanban_Column_Done, 
            EA_Number, Priority_Traffic_Ops, Fiscal_Year, Funding_Source, Route_Type, 
            Construction_EA_Number, Official_DOC_Date, Official_Advertise_Date, Anchor_Project, 
            Complexity_Signals, Complexity_Lighting, Complexity_ITS, Complexity_Power_Design, 
            Complexity_RoW_Coordination, Complexity_SLI_Project_Lead, Complexity_Solar_Design, 
            Complexity_Trunkline
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(Work_Item_ID) DO UPDATE SET
            Area_ID = excluded.Area_ID,
            Area_Path = excluded.Area_Path,
            Team_Project = excluded.Team_Project,
            Node_Name = excluded.Node_Name,
            Area_Level_1 = excluded.Area_Level_1,
            Revision = excluded.Revision,
            Authorized_Date = excluded.Authorized_Date,
            Revised_Date = excluded.Revised_Date,
            Iteration_ID = excluded.Iteration_ID,
            Iteration_Path = excluded.Iteration_Path,
            Iteration_Level_1 = excluded.Iteration_Level_1,
            Work_Item_Type = excluded.Work_Item_Type,
            State = excluded.State,
            Reason_for_State_Change = excluded.Reason_for_State_Change,
            Assigned_To = excluded.Assigned_To,
            Person_ID = excluded.Person_ID,
            Watermark = excluded.Watermark,
            Comment_Count = excluded.Comment_Count,
            Title = excluded.Title,
            Board_Column = excluded.Board_Column,
            Is_Board_Column_Done = excluded.Is_Board_Column_Done,
            State_Change_Date = excluded.State_Change_Date,
            Business_Value = excluded.Business_Value,
            Backlog_Priority = excluded.Backlog_Priority,
            Health = excluded.Health,
            Scoping_30_Percent = excluded.Scoping_30_Percent,
            Intermediate_Date = excluded.Intermediate_Date,
            SeventyFivePercentComplete = excluded.SeventyFivePercentComplete,
            QAQC_Submittal_Date = excluded.QAQC_Submittal_Date,
            Document_Submittal_Date = excluded.Document_Submittal_Date,
            Extension_Marker = excluded.Extension_Marker,
            Kanban_Column = excluded.Kanban_Column,
            Kanban_Column_Done = excluded.Kanban_Column_Done,
            EA_Number = excluded.EA_Number,
            Priority_Traffic_Ops = excluded.Priority_Traffic_Tops,
            Fiscal_Year = excluded.Fiscal_Year,
            Funding_Source = excluded.Funding_Source,
            Route_Type = excluded.Route_Type,
            Construction_EA_Number = excluded.Construction_EA_Number,
            Official_DOC_Date = excluded.Official_DOC_Date,
            Official_Advertise_Date = excluded.Official_Advertise_Date,
            Anchor_Project = excluded.Anchor_Project,
            Complexity_Signals = excluded.Complexity_Signals,
            Complexity_Lighting = excluded.Complexity_Lighting,
            Complexity_ITS = excluded.Complexity_ITS,
            Complexity_Power_Design = excluded.Complexity_Power_Design,
            Complexity_RoW_Coordination = excluded.Complexity_RoW_Coordination,
            Complexity_SLI_Project_Lead = excluded.Complexity_SLI_Project_Lead,
            Complexity_Solar_Design = excluded.Complexity_Solar_Design,
            Complexity_Trunkline = excluded.Complexity_Trunkline;
    '''

    # Insert data
    try:
        for entry in data:
            fields = entry['fields']

            # Prepare values
            values = (
                fields.get('System.Id', None),
                fields.get('System.AreaId', None),
                fields.get('System.AreaPath', ''),
                fields.get('System.TeamProject', ''),
                fields.get('System.NodeName', ''),
                fields.get('System.AreaLevel1', ''),
                fields.get('System.Rev', None),
                format_date(fields.get('System.AuthorizedDate', None)),
                format_date(fields.get('System.RevisedDate', None)),
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
                format_date(fields.get('Microsoft.VSTS.Common.StateChangeDate', None)),
                fields.get('Microsoft.VSTS.Common.BusinessValue', None),
                fields.get('Microsoft.VSTS.Common.BacklogPriority', None),
                fields.get('Custom.Health', 'Unknown'),
                fields.get('Custom.30PercentScoping', None),
                format_date(fields.get('Custom.IntermediateDate', None)),
                format_date(fields.get('Custom.75PercentComplete', None)),
                format_date(fields.get('Custom.QAQCSubmittalDate', None)),
                format_date(fields.get('Custom.DocumentSubmittalDate', None)),
                int(fields.get('WEF_3EF069D225F848D0A794779F40639E36_System.ExtensionMarker', False)),
                fields.get('WEF_3EF069D225F848D0A794779F40639E36_Kanban.Column', None),
                int(fields.get('WEF_3EF069D225F848D0A794779F40639E36_Kanban.Column.Done', False)),
                fields.get('Custom.EANumber', None),
                fields.get('Custom.PriorityTrafficOps', None),
                fields.get('Custom.FiscalYear', None),
                fields.get('Custom.FundingSource', None),
                fields.get('Custom.RouteType', None),
                fields.get('Custom.ConstructionEANumber', None),
                format_date(fields.get('Custom.OfficialDOCDate', None)),
                format_date(fields.get('Custom.OfficialAdvertiseDate', None)),
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

            # Execute the SQL statement
            cursor.execute(sql_insert_statement, values)

        # Commit the transaction
        conn.commit()

    except sqlite3.Error as e:
        print(f"Error inserting data into the database: {e}")

    finally:
        # Close the connection
        conn.close()

# #Function to insert work items into the SQLite database table
# def insert_projects_into_db(data):
#     conn = sqlite3.connect(DB_NAME)
#     cursor = conn.cursor()

#     # Insert data into the table
#     for entry in data:
#         fields = entry['fields']
    
#         print(fields.get('Custom.IntermediateDate'))

# # Prepare the SQL insert statement with placeholders
#     sql_insert_statement = '''
#         INSERT OR REPLACE INTO projects (
#     Work_Item_ID, Area_ID, Area_Path, Team_Project, Node_Name, Area_Level_1, Revision, 
#     Authorized_Date, Revised_Date, Iteration_ID, Iteration_Path, Iteration_Level_1, Work_Item_Type, 
#     State, Reason_for_State_Change, Assigned_To, Person_ID, Watermark, Comment_Count, 
#     Title, Board_Column, Is_Board_Column_Done, State_Change_Date, Business_Value, 
#     Backlog_Priority, Health, Scoping_30_Percent, Intermediate_Date, SeventyFivePercentComplete, QAQC_Submittal_Date, 
#     Document_Submittal_Date, Extension_Marker, Kanban_Column, Kanban_Column_Done, 
#     EA_Number, Priority_Traffic_Ops, Fiscal_Year, Funding_Source, Route_Type, 
#     Construction_EA_Number, Official_DOC_Date, Official_Advertise_Date, Anchor_Project, 
#     Complexity_Signals, Complexity_Lighting, Complexity_ITS, Complexity_Power_Design, 
#     Complexity_RoW_Coordination, Complexity_SLI_Project_Lead, Complexity_Solar_Design, 
#     Complexity_Trunkline
# ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?)'''

# # Prepare the values from JSON for insertion
#     values = (
#         fields.get('System.Id', None),
#     fields.get('System.AreaId', None),
#     fields.get('System.AreaPath', ''),
#     fields.get('System.TeamProject', ''),
#     fields.get('System.NodeName', ''),
#     fields.get('System.AreaLevel1', ''),
#     fields.get('System.Rev', None),
#     fields.get('System.AuthorizedDate', None),
#     fields.get('System.RevisedDate', None),
#     fields.get('System.IterationId', None),
#     fields.get('System.IterationPath', ''),
#     fields.get('System.IterationLevel1', ''),
#     fields.get('System.WorkItemType', ''),
#     fields.get('System.State', ''),
#     fields.get('System.Reason', ''),
#     fields.get('System.AssignedTo', {}).get('displayName', None),
#     fields.get('System.PersonId', None),
#     fields.get('System.Watermark', None),
#     fields.get('System.CommentCount', None),
#     fields.get('System.Title', ''),
#     fields.get('System.BoardColumn', ''),
#     int(fields.get('System.BoardColumnDone', False)),
#     fields.get('Microsoft.VSTS.Common.StateChangeDate', None),
#     fields.get('Microsoft.VSTS.Common.BusinessValue', None),
#     fields.get('Microsoft.VSTS.Common.BacklogPriority', None),
#     fields.get('Custom.Health', 'Unknown'),
#     fields.get('Custom.30PercentScoping', None),
#     fields.get('Custom.IntermediateDate', None),
#     fields.get('Custom.75PercentComplete', None),
#     fields.get('Custom.QAQCSubmittalDate', None),
#     fields.get('Custom.DocumentSubmittalDate', None),
#     int(fields.get('WEF_3EF069D225F848D0A794779F40639E36_System.ExtensionMarker', False)),
#     fields.get('WEF_3EF069D225F848D0A794779F40639E36_Kanban.Column', None),
#     int(fields.get('WEF_3EF069D225F848D0A794779F40639E36_Kanban.Column.Done', False)),
#     fields.get('Custom.EANumber', None),
#     fields.get('Custom.PriorityTrafficOps', None),
#     fields.get('Custom.FiscalYear', None),
#     fields.get('Custom.FundingSource', None),
#     fields.get('Custom.RouteType', None),
#     fields.get('Custom.ConstructionEANumber', None),
#     fields.get('Custom.OfficialDOCDate', None),
#     fields.get('Custom.OfficialAdvertiseDate', None),
#     int(fields.get('Custom.AnchorProject', False)),
#     int(fields.get('Custom.Complexity_Signals', False)),
#     int(fields.get('Custom.Complexity_Lighting', False)),
#     int(fields.get('Custom.Complexity_ITS', False)),
#     int(fields.get('Custom.Complexity_Power_Design', False)),
#     int(fields.get('Custom.Complexity_RoW_Coordination', False)),
#     int(fields.get('Custom.Complexity_SLI_Project_Lead', False)),
#     int(fields.get('Custom.Complexity_Solar_Design', False)),
#     int(fields.get('Custom.Complexity_Trunkline', False))
# )

# # Execute the insert statement with values
#     cursor.execute(sql_insert_statement, values)

# # Commit the transaction and close the connection
#     conn.commit()
#     conn.close()




# Function to insert a flattened work item into the SQLite database
def insert_epics_into_db(data):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
     # Insert data into the table
    for entry in data:
        fields = entry['fields']
    


# Prepare the SQL insert statement with placeholders
        sql_insert_statement = '''
        INSERT OR REPLACE INTO epics (
    System_Id, System_AreaPath, System_TeamProject, System_IterationPath,
    System_WorkItemType, System_State, System_Reason, System_CreatedDate,
    System_ChangedDate, System_Title, System_BoardColumn, System_BoardColumnDone,
    Microsoft_VSTS_Common_StateChangeDate, Microsoft_VSTS_Common_Priority,
    Microsoft_VSTS_Common_ValueArea, Microsoft_VSTS_Common_BacklogPriority, System_Parent
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);'''
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
        INSERT OR REPLACE INTO  features (
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
        INSERT OR REPLACE INTO  productbacklogitems (
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

# Function to refresh data by fetching and updating
async def refresh_data(work_item_type):
    """Refresh data by fetching and updating the database asynchronously."""
    start_date = datetime(2020, 1, 1)
    end_date = datetime.today()

    work_item_ids = await fetch_work_item_ids(start_date, end_date, work_item_type)

    if work_item_ids:
        id_list = [item['id'] for item in work_item_ids]
        id_chunks = [id_list[i:i + 100] for i in range(0, len(id_list), 100)]

        for chunk in id_chunks:
            work_items = await fetch_work_item_details(chunk, work_item_type)
            match work_item_type:
                case "Project":
                    print(work_items)
                    insert_projects_into_db(work_items)
                case "Epic":
                    insert_epics_into_db(work_items)
                case "Feature":
                    insert_features_into_db(work_items)
                case "Product Backlog Item":
                    insert_pbis_into_db(work_items)
    else:
        pass
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO data_refresh_log (last_refresh_time) VALUES (CURRENT_TIMESTAMP)")
    conn.commit()
    conn.close()  


# Main function to fetch data from DevOps
async def get_data_from_devops():
    """Fetch data from Azure DevOps and update the database asynchronously."""
    response = await fetch_iteration_data()
    if not response:
        print("No iteration data fetched.")
        return

    iterations = parse_iteration_data(response)
    insert_or_update_iterations(DB_NAME, iterations)
    print(f"Inserted {len(iterations)} iterations into the database.")

    await refresh_data('Project')
    await refresh_data('Epic')
    await refresh_data('Feature')
    await refresh_data('Product Backlog Item')



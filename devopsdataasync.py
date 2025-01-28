import asyncio
import aiohttp
import sqlite3
import base64
import os
from datetime import datetime,timedelta
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv


# Load environment variables from a .env file
load_dotenv(override=True)
import logging

# Configure logging for both file and Streamlit logs
logging.basicConfig(
    level=logging.INFO,  # Set the log level to control verbosity
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Stream logs to the console (captured by Streamlit Cloud)
    ]
)

# Azure DevOps settings
ORGANIZATION = os.getenv('AZURE_DEVOPS_ORGANIZATION')
PROJECT = os.getenv('AZURE_DEVOPS_PROJECT')
PAT = os.getenv('AZURE_DEVOPS_PAT')

WIQL_URL = os.getenv('WIQL_URL')
WORK_ITEMS_BATCH_URL = os.getenv('WORK_ITEMS_BATCH_URL')
ITERATIONS_API_URL = os.getenv('ITERATIONS_API_URL')
BASE_URL = os.getenv('BASE_URL')

DB_NAME = os.getenv('DB_PATH')


# Helper to encode PAT to Base64
def encode_pat_to_base64(pat):
    """Encodes the PAT to Base64 format for Azure DevOps API."""
    pat_with_colon = f"{pat}:"
    return base64.b64encode(pat_with_colon.encode('utf-8')).decode('utf-8')


def format_date(timestamp):
    formats = [
        "%Y-%m-%dT%H:%M:%SZ",     # Format without fractional seconds
        "%Y-%m-%dT%H:%M:%S.%fZ"   # Format with fractional seconds
    ]
    for fmt in formats:
        try:
            # Try parsing with each format
            dt = datetime.strptime(timestamp, fmt)
            # Format the datetime object to the desired date string
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Raise an error if no format matches
    raise ValueError(f"Timestamp does not match expected formats: {timestamp}")

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
                logging.info(f"Failed to fetch iteration data: {response} {response.status} - {await response.text()}")
                return None

# API Request Functions
async def get_current_sprint():
    """Fetch current sprint details asynchronously."""
    url = f"{BASE_URL}/_apis/work/teamsettings/iterations?api-version=7.1-preview.1"
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth("", PAT)) as session:
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()
            iterations = data["value"]

            for iteration in iterations:
                if iteration["attributes"]["timeFrame"] == "past":
                    return iteration
                
            return None


async def get_work_item_ids(iteration_path):
    """Fetch work item IDs for the given iteration path asynchronously."""
    url = f"{BASE_URL}/_apis/wit/wiql?api-version=7.1-preview.2"
    query = {
        "query": f"SELECT [System.Id] FROM WorkItems WHERE [System.IterationPath] = '{iteration_path}'"
    }
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth("", PAT)) as session:
        async with session.post(url, json=query) as response:
            response.raise_for_status()
            data = await response.json()
            return [item["id"] for item in data["workItems"]]

async def get_work_item_details(work_item_ids):
    """Fetch detailed information about work items asynchronously."""
    url = f"{BASE_URL}/_apis/wit/workitemsbatch?api-version=7.1-preview.1"
    body = {
        "ids": work_item_ids,
        "fields": [
            "System.Id",
            "System.Title",
            "System.State",
            "System.WorkItemType",
            "Microsoft.VSTS.Scheduling.Effort",
        ],
    }
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth("", PAT)) as session:
        async with session.post(url, json=body) as response:
            response.raise_for_status()
            data = await response.json()
            return data["value"]


def insert_work_items_into_db(work_items, sprint_name):
    """Insert or update work items into the SQLite database."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for item in work_items:
        cursor.execute("""
            INSERT INTO sprint_trends_data (id, title, state, type, effort, sprint_name)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title=excluded.title,
                state=excluded.state,
                type=excluded.type,
                effort=excluded.effort,
                sprint_name=excluded.sprint_name,
                modified_time=CURRENT_TIMESTAMP
        """, (
            item["id"],
            item["fields"]["System.Title"],
            item["fields"]["System.State"],
            item["fields"]["System.WorkItemType"],
            item["fields"].get("Microsoft.VSTS.Scheduling.Effort", None),
            sprint_name
        ))

    conn.commit()
    conn.close()
# Asynchronous method to fetch work item IDs
async def fetch_work_item_ids(start_date, end_date, work_item_type, batch_days=50):
    """Fetch work item IDs asynchronously with batch processing for Product Backlog Items."""
    encoded_pat = encode_pat_to_base64(PAT)
    headers = {"Authorization": f"Basic {encoded_pat}"}
    work_items = []

    # Check if work_item_type is 'Product Backlog Item' and enable batching
    if work_item_type == 'Product Backlog Item':
        current_start_date = start_date

        async with aiohttp.ClientSession() as session:
            while current_start_date <= end_date:
                # Calculate the end date for the current batch
                current_end_date = min(current_start_date + timedelta(days=batch_days - 1), end_date)
                start_date_str = current_start_date.strftime('%Y-%m-%d')
                end_date_str = current_end_date.strftime('%Y-%m-%d')

                query = {
                    "query": f"Select [System.Id] From WorkItems Where [System.WorkItemType] = '{work_item_type}' "
                             f"And [System.CreatedDate] >= '{start_date_str}' And [System.CreatedDate] <= '{end_date_str}'"
                }

                # Make the API call for the current batch
                async with session.post(WIQL_URL, json=query, headers=headers) as response:
                    if response.status == 200:
                        batch_work_items = (await response.json()).get("workItems", [])
                        work_items.extend(batch_work_items)
                        logging.info(f"Fetched {len(batch_work_items)} items from {start_date_str} to {end_date_str}.")
                    else:
                        logging.info(f"Failed to fetch batch: {response.status} - {await response.text()}")

                # Move to the next batch
                current_start_date = current_end_date + timedelta(days=1)
    else:
        # Single query for other work item types
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        query = {
            "query": f"Select [System.Id] From WorkItems Where [System.WorkItemType] = '{work_item_type}' "
                     f"And [System.CreatedDate] >= '{start_date_str}' And [System.CreatedDate] <= '{end_date_str}'"
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(WIQL_URL, json=query, headers=headers) as response:
                if response.status == 200:
                    work_items = (await response.json()).get("workItems", [])
                else:
                    logging.info(f"Failed to fetch work item IDs: {response.status} - {await response.text()}")

    logging.info(f"Total work items fetched: {len(work_items)}")
    return work_items
# # Asynchronous method to fetch work item IDs
# async def fetch_work_item_ids(start_date, end_date, work_item_type):
#     """Fetch work item IDs asynchronously."""
#     start_date_str = start_date.strftime('%Y-%m-%d')
#     end_date_str = end_date.strftime('%Y-%m-%d')

#     if work_item_type == 'Product Backlog Item':
#         query = {
#             "query": f"Select [System.Id] From WorkItems Where [System.WorkItemType] = '{work_item_type}' "
#                      f"And [System.CreatedDate] >= '{start_date_str}' And [System.CreatedDate] <= '{end_date_str}' "
#                     #  f"And [System.State] IN ('New', 'Approved', 'Committed','Done')"
#         }
#     else:
#         query = {
#             "query": f"Select [System.Id] From WorkItems Where [System.WorkItemType] = '{work_item_type}' "
#                      f"And [System.CreatedDate] >= '{start_date_str}' And [System.CreatedDate] <= '{end_date_str}'"
#         }

#     encoded_pat = encode_pat_to_base64(PAT)
#     headers = {"Authorization": f"Basic {encoded_pat}"}

#     async with aiohttp.ClientSession() as session:
#         async with session.post(WIQL_URL, json=query, headers=headers) as response:
#             if response.status == 200:
#                 return (await response.json()).get("workItems", [])
#             else:
#                 print(f"Failed to fetch work item IDs: {response.status} - {await response.text()}")
#                 return []


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
    #print(response)
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
            Priority_Traffic_Ops = excluded.Priority_Traffic_Ops,
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

            # Execute the SQL statement
            cursor.execute(sql_insert_statement, values)

        # Commit the transaction
        conn.commit()

    except sqlite3.Error as e:
        logging.error(f"Error inserting data into the database: {e}")

    finally:
        # Close the connection
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
    start_date = datetime(2023, 1, 1)
    end_date = datetime.today()

    try:
        work_item_ids = await fetch_work_item_ids(start_date, end_date, work_item_type)

        if work_item_ids:
            id_list = [item['id'] for item in work_item_ids]
            id_chunks = [id_list[i:i + 100] for i in range(0, len(id_list), 100)]

            for chunk in id_chunks:
                work_items = await fetch_work_item_details(chunk, work_item_type)
                match work_item_type:
                    case "Project":
                        insert_projects_into_db(work_items)
                    case "Epic":
                        insert_epics_into_db(work_items)
                    case "Feature":
                        insert_features_into_db(work_items)
                    case "Product Backlog Item":
                        insert_pbis_into_db(work_items)

        return True  # Indicate success
    except Exception as e:
        logging.info(f"Error during data refresh for {work_item_type}: {e}")
        return False  # Indicate failure

async def get_devops_sprint_details():
    try:
        sprint = await get_current_sprint()
        if sprint:
            sprint_name = sprint["name"]
            sprint_path = sprint["path"]
            start_date = sprint["attributes"]["startDate"]
            end_date = sprint["attributes"]["finishDate"]

            # Fetch Work Items for Sprint
            work_item_ids = await get_work_item_ids(sprint_path)
            num_work_items = len(work_item_ids)
            if num_work_items > 0:
                work_items = await get_work_item_details(work_item_ids)
                insert_work_items_into_db(work_items, sprint_name)
                return True
    except Exception as e:
        return False

# Main function to fetch data from DevOps
async def get_data_from_devops():
    """Fetch data from Azure DevOps and update the database asynchronously."""
    response = await fetch_iteration_data()
    if not response:
        print("No iteration data fetched.")
        return

    iterations = parse_iteration_data(response)
    insert_or_update_iterations(DB_NAME, iterations)
    logging.info(f"Inserted {len(iterations)} iterations into the database.")

    


    # Refresh data for all work item types
    refresh_status = {
        "Project": await refresh_data('Project'),
        "Epic": await refresh_data('Epic'),
        "Feature": await refresh_data('Feature'),
        "Product Backlog Item": await refresh_data('Product Backlog Item'),
        "devopsData": await get_devops_sprint_details()
    }

    # Check if all API calls were successful
    if all(refresh_status.values()):
        # Update the data_refresh_log table
        logging.info(refresh_status)
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()

        # Clear all records before insertion
        cursor.execute("DELETE FROM data_refresh_log")

        cursor.execute("INSERT INTO data_refresh_log (last_refresh_time) VALUES (CURRENT_TIMESTAMP)")
        conn.commit()
        conn.close()
        logging.info("Data refresh log updated successfully.")
    else:
        logging.error("Some API calls failed. Data refresh log not updated.")



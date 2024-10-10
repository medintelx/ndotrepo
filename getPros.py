import requests
import os
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from a .env file
load_dotenv()

# Read organization and project name from .env file
ORGANIZATION = os.getenv('AZURE_DEVOPS_ORGANIZATION')
PROJECT = os.getenv('AZURE_DEVOPS_PROJECT')

# Replace with your personal access token
PAT = os.getenv('AZURE_DEVOPS_PAT')

# Azure DevOps REST API endpoints
WIQL_URL = f'https://dev.azure.com/{ORGANIZATION}/{PROJECT}/_apis/wit/wiql?api-version=7.2-preview'
WORK_ITEMS_URL = f'https://dev.azure.com/{ORGANIZATION}/{PROJECT}/_apis/wit/workitems/{{}}?$expand=all&api-version=7.2-preview'

WORK_ITEMS_BATCH_URL = f"https://dev.azure.com/{ORGANIZATION}/_apis/wit/workitemsbatch?api-version=7.2-preview"


#work_item_data = extract_work_item_data(json_data)
def split_list_to_strings(input_list, max_length=200):
    result = []
    current_string = ""

    for item in input_list:
        item_str = str(item)
        if len(current_string) + len(item_str) + 1 > max_length:
            result.append(current_string)
            current_string = item_str
        else:
            if current_string:
                current_string += "," + item_str
            else:
                current_string = item_str

    if current_string:
        result.append(current_string)

    return result



# Function to fetch work item IDs in a given date range with a specific type
def fetch_work_item_ids(start_date, end_date, work_item_type):
    query = {
        "query": f"Select [System.Id] From WorkItems Where [System.WorkItemType] = '{work_item_type}' And [System.CreatedDate] >= '{start_date}' And [System.CreatedDate] < '{end_date}'"
    }
    response = requests.post(WIQL_URL, json=query, auth=HTTPBasicAuth('', PAT))
    if response.status_code == 200:
        return [item['id'] for item in response.json()['workItems']]
    else:
        print(f"Failed to fetch work item IDs for range {start_date} - {end_date}: {response.status_code}")
        print(response.json())
        return []

# Function to fetch work item details by IDs
def fetch_pbi_work_item_details(ids):
    if not ids:
        return []

    url =  WORK_ITEMS_BATCH_URL
    # Payload for batch request
    WORK_ITEM_IDS = ids
    payload = {
    "ids": WORK_ITEM_IDS,  # List of work item IDs,
    "fields": [
        "System.Id", "System.AreaId", "System.AreaPath", "System.TeamProject", "System.NodeName", "System.AreaLevel1", "System.Rev", "System.AuthorizedDate", "System.RevisedDate", "System.IterationId", "System.IterationPath", "System.IterationLevel1", "System.WorkItemType", "System.State", "System.Reason",
        "System.Parent","System.CreatedDate","System.ChangedDate","System.PersonId", "System.Watermark", "System.CommentCount", "System.Title", "System.BoardColumn", "System.BoardColumnDone", "Microsoft.VSTS.Common.StateChangeDate", "Microsoft.VSTS.Common.Priority", "Microsoft.VSTS.Common.ValueArea", "Microsoft.VSTS.Common.BusinessValue", "Microsoft.VSTS.Scheduling.Effort", "Microsoft.VSTS.Common.BacklogPriority", "Custom.EANumber"
           
    ]
}

    print(url)
    response = requests.post(url, json=payload, auth=HTTPBasicAuth('', PAT))

    if response.status_code == 200:
        print(response.json())
        return #response.json()['value']  # Corrected to return the 'value' key from JSON
        
    else:
        print(f"Failed to fetch work item details: {response.status_code}")
        print(response)
        return []
    
# Function to fetch work item details by IDs
def fetch_feature_work_item_details(ids):
    if not ids:
        return []

    url =  WORK_ITEMS_BATCH_URL
    # Payload for batch request
    WORK_ITEM_IDS = [35556]
    payload = {
    "ids": WORK_ITEM_IDS,  # List of work item IDs,
    "fields": ["system.Id","System.AreaPath", "System.TeamProject", "System.IterationPath", "System.WorkItemType", "System.State", "System.Reason", "System.CreatedDate", "System.ChangedDate", "System.Title", "System.BoardColumn", "System.BoardColumnDone", "Microsoft.VSTS.Common.StateChangeDate", "Microsoft.VSTS.Common.Priority", "Microsoft.VSTS.Common.ValueArea", "Microsoft.VSTS.Common.BusinessValue", "Microsoft.VSTS.Common.BacklogPriority", "Custom.EANumber", "System.Parent"]
}

    print(url)
    response = requests.post(url, json=payload, auth=HTTPBasicAuth('', PAT))

    if response.status_code == 200:
        print(response.json())
        return #response.json()['value']  # Corrected to return the 'value' key from JSON
        
    else:
        print(f"Failed to fetch work item details: {response.status_code}")
        print(response)
        return []

# Function to fetch work item details by IDs
def fetch_project_work_item_details(ids):
    if not ids:
        return []

    url =  WORK_ITEMS_BATCH_URL
    # Payload for batch request
    WORK_ITEM_IDS = [7451]
    payload = {
    "ids": WORK_ITEM_IDS,  # List of work item IDs,
    "fields": []
}

    print(url)
    response = requests.post(url, json=payload, auth=HTTPBasicAuth('', PAT))

    if response.status_code == 200:
        print(response.json())
        return #response.json()['value']  # Corrected to return the 'value' key from JSON
        
    else:
        print(f"Failed to fetch work item details: {response.status_code}")
        print(response)
        return []
    
# Function to fetch work item details by IDs
def fetch_epic_work_item_details(ids):
    if not ids:
        return []

    url =  WORK_ITEMS_BATCH_URL
    # Payload for batch request
    WORK_ITEM_IDS = [44061]
    payload = {
    "ids": WORK_ITEM_IDS,  # List of work item IDs,
    "fields": ["system.Id","System.AreaPath", "System.TeamProject", "System.IterationPath", "System.WorkItemType", "System.State", "System.Reason", "System.CreatedDate", "System.ChangedDate", "System.Title", "System.BoardColumn", "System.BoardColumnDone", "Microsoft.VSTS.Common.StateChangeDate", "Microsoft.VSTS.Common.Priority", "Microsoft.VSTS.Common.ValueArea", "Microsoft.VSTS.Common.BusinessValue", "Microsoft.VSTS.Common.BacklogPriority", "Custom.EANumber", "System.Parent"]
}

    print(url)
    response = requests.post(url, json=payload, auth=HTTPBasicAuth('', PAT))

    if response.status_code == 200:
        print(response.json())
        return #response.json()['value']  # Corrected to return the 'value' key from JSON
        
    else:
        print(f"Failed to fetch work item details: {response.status_code}")
        print(response)
        return []
# Main logic to fetch work items by date range and type
def main():
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 1)
    work_item_type = 'feature'  # Adjust the work item type as needed

    # Fetch work item IDs
    work_item_ids = fetch_work_item_ids(start_date.isoformat(), end_date.isoformat(), work_item_type)
    print(work_item_ids)
    
    # Fetch work item details
    if work_item_ids:
        all_work_items = fetch_project_work_item_details(work_item_ids)
        for item in all_work_items:
            print(item)  # Print each work item's details
            print("************")

if __name__ == "__main__":
    main()

import requests
import base64
import json

# Azure DevOps organization and project details
organization = "ndotazure"
project_id = "a639f3c7-7716-4495-823a-fe99301988eb"
pat = "jia34yc6g4ecspx4ztuwlalilw2lybrejvhafxomkq4vahqbxvgq"  # Replace with your PAT

# Encode PAT for Basic Authentication
encoded_pat = base64.b64encode(f":{pat}".encode()).decode()

# API endpoint to get all fields for the project
url = f"https://dev.azure.com/{organization}/{project_id}/_apis/wit/fields?api-version=7.2-preview"

# Headers for the request, including the encoded PAT for authentication
headers = {
    'Authorization': f'Basic {encoded_pat}',
    'Content-Type': 'application/json'
}

# Send a GET request to the API endpoint
response = requests.get(url, headers=headers)

# Check if the request was successful
if response.status_code == 200:
    # Parse the JSON response
    data = response.json()
    
    # Write the JSON response to a text file
    with open("api_response.txt", "w") as file:
        json.dump(data, file, indent=4)
    
    print("API response saved to 'api_response.txt'")
else:
    # Print an error message if the request failed
    print(f"Error: {response.status_code}")
    print(response.text)
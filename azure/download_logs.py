"""Util module to download log analytics query data using a REST API

Requires:
  - azure-loganalytics
  - azure-identity
"""

import requests
from azure.identity import DefaultAzureCredential
 
workspace_id = "<your-workspace-id>"
scope = "https://api.loganalytics.io/.default"
credential = DefaultAzureCredential()
token = credential.get_token(scope)
token = token.token
query = "requests | where timestamp > ago(1h) | summarize count() by name"
url = f"https://api.loganalytics.io/v1/workspaces/{workspace_id}/query"
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}
body = {"query": query}
response = requests.post(url, json=body, headers=headers)
print(response.json())

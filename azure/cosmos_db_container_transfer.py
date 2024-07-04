"""Simple script to move data from one Cosmos DB container to another.

- Ok for small to medium-sized data transfer tasks
- For moving records in 100k+ range, consider using Azure Data Factory or other bulk data transfer tools
"""
import os
import time
from tqdm import tqdm
from azure.cosmos import CosmosClient, exceptions, PartitionKey

# Initialize Cosmos Client
source_client = CosmosClient.from_connection_string(os.environ["COSMOS_CONNECTION_STRING__SOURCE"])
target_client = CosmosClient.from_connection_string(os.environ["COSMOS_CONNECTION_STRING__TARGET"])

# Source Database and Container
source_database_id = 'FMIDB'
source_container_id = 'FMIBaseContainer'
source_database = source_client.get_database_client(source_database_id)
source_container = source_database.get_container_client(source_container_id)

# Destination Database and Container
destination_database_id = 'FMIDB'
destination_container_id = 'FMIBaseContainer'
destination_database = target_client.create_database_if_not_exists(destination_database_id)
destination_container = destination_database.create_container_if_not_exists(destination_container_id, PartitionKey(path="/id"))

# Query to fetch all items in the source container
# Modify the query as per your requirement
query = "SELECT * FROM c"

max_retries = 5
initial_wait_time = 5  # seconds
page_no = 0

def upsert_with_retry(item, max_retries, initial_wait_time):
    retries = 0
    wait_time = initial_wait_time
    while retries < max_retries:
        try:
            destination_container.upsert_item(item)
            print(f"Item with id {item['id']} transferred successfully.")
            return True
        except exceptions.CosmosHttpResponseError as e:
            print(f"Error occurred while transferring item with id {item['id']}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
            wait_time *= 2  # Simulate exponential backoff
            retries += 1
    print(f"Failed to transfer item with id {item['id']} after {max_retries} attempts.")
    return False

for page in source_container.query_items(
        query=query,
        max_item_count=1000,
        enable_cross_partition_query=True).by_page():
    page_no += 1
    print(f"[START] Processing page {page_no}")
    print("-" * 50)

    for item in tqdm(page, desc=f"Transferring items from PAGE {page_no}"):
      upsert_with_retry(item, max_retries, initial_wait_time)

    print("-" * 50)
    print(f"[END] Processing page {page_no}")
    print()

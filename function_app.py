#
#
#
#
#
#
#


import datetime
import logging
import os
import csv
import math
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.mgmt.resourcegraph import ResourceGraphClient
from azure.mgmt.resourcegraph.models import QueryRequest
from azure.mgmt.resourcegraph.models import QueryRequestOptions
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

@app.function_name(name="resourceQuery")
@app.timer_trigger(schedule="0 */5 * * * *", 
              arg_name="timer",
              run_on_startup=True) 
def query_1(timer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

    if timer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    credential = DefaultAzureCredential(managed_identity_client_id=os.environ["MANAGED_IDENTITY_CLIENT_ID"])

    client = ResourceGraphClient(credential)
    skipCount = 0

    skipToken = None

# Instead of changing the column names down below, I've updated the ARG query to handle the rename of the column names.
# This is an example of renaming the columns: 'diskSizeGb = properties.diskSizeGB'. It is advised to do this for all future queries.
    query = """
Resources
| where type == 'microsoft.compute/disks'
| extend vmId = tostring(properties.ownerId)
| extend diskState = iif(isnull(vmId) or vmId == '', 'Unattached', 'Attached')
| extend isManagedDisk = iif(isnull(properties.osType), 'Unmanaged', 'Managed')
| project id, name, location, resourceGroup, 
          diskSizeGb = properties.diskSizeGB, 
          timeCreated = properties.timeCreated, 
          diskState, isManagedDisk

    """

    query_request = QueryRequest(
        query=query,
        options=QueryRequestOptions(
            top=1000,
            skip=skipCount,
            skip_token=skipToken,
            )
    )

    query_response = client.resources(query_request)

    if query_response.total_records == 0:
        logging.info("No results found for the query.")
    
    elif query_response.total_records <= 1000:
        csv_file_path = "/tmp/azure_disks.csv"
        with open(csv_file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
                        
            writer.writerow(query_response.data[0].keys())
        
            for item in query_response.data:
                writer.writerow(item.values())
        
        logging.info(f"Query results saved to {csv_file_path}")
    
    else:
        csv_file_path = "/tmp/azure_disks.csv"
        with open(csv_file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
                        
            writer.writerow(query_response.data[0].keys())
        
            for item in query_response.data:
                writer.writerow(item.values())
            
        i = math.ceil(query_response.total_records/1000)-1
    
        skipToken=query_response.skip_token
    
        while(i > 0):
            skipCount += 1000
            query_request = QueryRequest(
            query=query,
            options=QueryRequestOptions(
                top=1000,
                skip=skipCount,
                skip_token=skipToken,
                )
        )
            query_response = client.resources(query_request)
        
            csv_file_path = f"/tmp/azure_disks.csv"
            with open(csv_file_path, mode='a', newline='') as file:
                writer = csv.writer(file)
            
                for item in query_response.data:
                    writer.writerow(item.values())
            
            logging.info(f"Query results saved to {csv_file_path}")
        
            i -= 1
            skipToken=query_response.skip_token
    
    # Upload the CSV file to Azure Blob Storage
            
    blob_service_client = BlobServiceClient(os.environ["STORAGE_ACCOUNT_URL"],credential)
    container_name = os.environ["STORAGE_CONTAINER_NAME"]
    blob_name = f"azure_disks_{datetime.datetime.now().strftime('%Y%m%d')}.csv"

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    with open(csv_file_path, "rb") as data:
        try:
            blob_client.upload_blob(data, overwrite=True)
        except Exception as e:
            logging.error(f"Failed to upload blob: {e}")

            logging.info(f"CSV file uploaded to blob storage: {container_name}/{blob_name}")
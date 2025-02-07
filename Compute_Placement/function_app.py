### Setup notes ###
###################
# This function app is designed to be triggered by a timer trigger, which is set to run every hour based on the below schedule
# Info on configuring the time can be found below: 
# https://learn.microsoft.com/en-us/azure/azure-functions/functions-bindings-timer?tabs=python-v2%2Cisolated-process%2Cnodejs-v4&pivots=programming-language-python#ncrontab-expressions  
#
# The Function will need to have the following environment variables set in the Azure portal:
# - MANAGED_IDENTITY_CLIENT_ID: The client ID of the managed identity that the function app is using
# - STORAGE_ACCOUNT_URL: The URL of the Azure Blob Storage account where the data will be uploaded
# - STORAGE_CONTAINER_NAME: The name of the container in the Azure Blob Storage account where the data will be uploaded
#
# The Function needs to have a managed identity assigned to it, which will be used to authenticate with the Azure REST API and Azure Blob Storage
# I advise using a user-assigned managed identity vs a system-assigned identity
#
# The managed identity used by the Function will need to have the following permissions assigned to the managed identity at the scope of the Storage Account:
# - Storage Blob Data Contributor
#
# Once you have done the avbove, you can deploy the function app to Azure and it will run on the schedule you have defined
# Be sure to review the 'Variables to be defined' section below and update the variables as needed
#
# You can deploy this Function in many ways, one method would be to use Azure Cloud Shell set to Bash
# Then upload the function_app.zip file to the Azure Cloud Shell and run the below commands:
# az webapp deploy --resource-group <resource-group-name-where-function-resides> --name <name-of-function-app> --src-path $HOME/function_app.zip --type zip
###################

import datetime
import logging
import os
import requests
import pandas as pd
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


app = func.FunctionApp()


@app.function_name(name="placementScoreQuery")
@app.timer_trigger(schedule="0 0 * * * *", 
              arg_name="timer",
              run_on_startup=True) 


def placementScoreQuery(timer: func.TimerRequest) -> None:


    ### Variables to be defined ###
    ###############################
    
    # Vars for REST URL, subscription_id as the name sets the subscription to be queried 
    # api_version has been left as a variable to allow for easy updating in the future since Azure API versions change over time
    
    subscription_id = '00000000-0000-0000-0000-000000000000'
    api_version = '2024-06-01-preview'
    
    # This is the name of the file that will be uploaded to the blob storage, an example name is shown below
    # placement_score_6a4eea0d-60cf-4a28-b6ca-ded6c8c5a4af_10250206.xlsx
    blob_name = f"placement_Score_{subscription_id}_{datetime.datetime.now().strftime('%Y%m%d')}.xlsx"
    
    # Parameters that define the payload passed to the Placement Score REST API
    desiredLocations = ["westus", "eastus", "westcentralus"]
    desiredSizes = [
            {"sku": "Standard_D2_v2"},
            {"sku": "Standard_D8s_v3"}
        ]
    desiredCount = 10
    
    ###############################
    
    
    if timer.past_due:
        logging.info('The timer is past due!')
    utc_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    # Authenticate using the user managed identity and get the token
    credential = DefaultAzureCredential(managed_identity_client_id=os.environ["MANAGED_IDENTITY_CLIENT_ID"])
    token = credential.get_token("https://management.azure.com/.default")


    # Define the URL and headers
    domain = 'management.azure.com/subscriptions'
    path = 'providers/Microsoft.Compute/locations/eastus/placementScores/regular/generate?api-version='
    
    url = f'https://{domain}/{subscription_id}/{path}/{api_version}'
    headers = {
        'Accept': 'application/json',
        'Authorization': 'Bearer '+token.token,
        'Content-Type': 'application/json; charset=utf-8'
    }

    # Define the data payload
    data = {
        "desiredLocations": desiredLocations,
        "desiredSizes": desiredSizes,
        "desiredCount": desiredCount,
        "availabilityZones": True
    }

    # Make the POST request
    response = requests.post(url, headers=headers, json=data)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        json_response = response.json()
    
        # Extract the placementScores data
        placement_scores = json_response['placementScores']
    
        # Add the data payload details to each row of the placementScores data
        # for score in placement_scores:
        # score['desiredLocations'] = data['desiredLocations']
        # score['desiredSizes'] = [size['sku'] for size in data['desiredSizes']]
        # score['availabilityZones'] = data['availabilityZones']
       
        # Convert the placementScores data to a DataFrame
        df = pd.DataFrame(placement_scores)
        df['desiredCount'] = data['desiredCount']
    
    
        # Save the DataFrame to an Excel file
        excel_file_path = f"/tmp/temp.xlsx"
        
        df.to_excel(excel_file_path, index=False)
    
        print("The response has been successfully saved to 'placement_scores.xlsx'.")
    else:
        print(f"Failed to retrieve data: {response.status_code} - {response.text}")
    
        # Upload the CSV file to Azure Blob Storage
            
    blob_service_client = BlobServiceClient(os.environ["STORAGE_ACCOUNT_URL"],credential)
    container_name = os.environ["STORAGE_CONTAINER_NAME"]
    

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    with open(excel_file_path, "rb") as data:
        try:
            blob_client.upload_blob(data, overwrite=True)
        except Exception as e:
            logging.error(f"Failed to upload blob: {e}")

            logging.info(f"Excel file uploaded to blob storage: {container_name}/{blob_name}")
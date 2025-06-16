# End-to-End Real-Time Weather Data Streaming 🌦️
## Powered by Azure

![](https://media4.giphy.com/media/v1.Y2lkPTc5MGI3NjExbG43bXIwOWxseDRqN3d4Y253dnZ0enJ1ZGwxemVneGwzOXVpbW1xbiZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/RRl5VP2IaeInK6OQYc/giphy.gif)

## Table of Contents

1. [Overview](#overview) 
2. [Motivation](#motivation) 
3. [Functionalities Used](#functionalities-used)
4.  [Workflow](#workflow) 
5.  [Execution with Screenshots](#execution-with-screenshots) 
6.  [FAQ](#faq)

## Overview
This project implements a **fully automated, real-time weather data streaming pipeline** on Azure. A public Weather API (polled every few seconds) feeds JSON weather data into Azure Event Hubs. Two ingestion methods are used and compared against: an Azure Databricks notebook (Spark polling) and an Azure Functions timer trigger (serverless polling). Both push messages into the same Event Hub for comparison of cost/latency. From there, Microsoft Fabric runs an **Event Stream pipeline** that continuously harvests incoming events and writes them into an Azure Data Explorer (Kusto) database – a time-series-optimized store for fast analytics. A Power BI dashboard then connects live to this database (with auto-refresh) to provide an up-to-the-second visualization of metrics (temperature, air quality, etc.). Threshold-based alerts (e.g. for high wind or very high AQI) are handled via Fabric’s Data Activator to send an alert to your email whenever there are any emergency situations. All sensitive credentials (API keys, connection strings) are stored securely in Azure Key Vault.

![Architecture Overview.jpg](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/Weather%20API%20Data%20Engineering.png?raw=true) _Architecture Diagram._

The pipeline is structured for continuous end-to-end data flow:

-   **Data Source (Weather API):** A public weather data API (e.g. weatherapi.com) is called every few seconds (via code) to retrieve current conditions.
    
-   **Event Hub Ingestion:** The JSON payload from the API is sent to an **Azure Event Hub** named `weather-streaming-event-hub`. A shared-access policy with _Send_ permissions is created for upstream publishers.
    
-   **Stream Processing Pipeline:** Microsoft Fabric’s **Event Stream** feature connects to the Event Hub and continuously reads new messages. Each event is immediately written into an **Azure Data Explorer (Kusto) database** (with tables like `WeatherData`). This enables low-latency, time-series storage of each weather recordfile-gtdsayi4bemytduos8vxz4.
    
-   **Data Storage (Kusto DB):** The Kusto database holds the raw and aggregated weather data. Every new record written is instantly queryable.
    
-   **Visualization (Power BI):** A Power BI report is published that uses DirectQuery to the Kusto DB. The report is configured to **auto-refresh** (e.g. every second) to show the latest temperature, humidity, AQI, etc. as soon as data arrives. For example, when the Function App was started, the live dashboard’s row count jumped from 541 to 544 without delayfile-gtdsayi4bemytduos8vxz4, demonstrating end-to-end real-time update.
    
-   **Alerts:** Fabric’s **Data Activator** is set up with KQL rules (e.g. “AQI > 150” or “wind_speed > 70 km/h”). When a rule is triggered by incoming data, an email alert is sent immediately.


---


## Motivation

I built this real-time weather streaming pipeline to:

-   **Master end-to-end Azure streaming services** by designing, deploying, and operating a complete data flow—from a public Weather API through Event Hubs, Fabric Event Stream, and Kusto DB to real-time Power BI dashboards and alerting.
    
-   **Compare serverless vs. Spark–based ingestion**, evaluating cost, latency, and operational overhead between an Azure Functions timer trigger (lightweight, pay-per-execution) and an Azure Databricks streaming notebook (Spark-powered, cluster-based).
    
-   **Sharpen production-grade skills** in key Azure services (Event Hubs, Key Vault, Functions, Databricks, Fabric, Data Activator) and best practices for secure secret management, managed identities, and infrastructure as code.
    
-   **Deliver true real-time analytics** on time-series data, leveraging Azure Data Explorer’s speed and Power BI’s DirectQuery auto-refresh to show up-to-the-second weather metrics and trigger instant alerts for critical conditions.
    
-   **Demonstrate a compelling portfolio project** that showcases my ability to architect, implement, and document a fully automated streaming data solution—preparing me to excel in roles focused on real-time data engineering and cloud-native analytics.

---

## Functionalities Used


This solution leverages a suite of Azure services and tools:

-   **Azure Event Hubs**  – ingest streaming weather events.
    
-   **Azure Databricks (Spark)** – alternate ingestion path: periodically polls the Weather API and sends batches to Event Hub.
    
-   **Azure Functions (Python Timer Trigger)** – serverless ingestion: fetches live weather data via API and sends it to Event Hub.
    
-   **Azure Key Vault** – secure storage of secrets (Weather API key, Event Hub connection string, etc.).
    
-   **Microsoft Fabric (Event Stream pipeline & Data Activator)** – orchestrates the real-time pipeline and alerting.
    
-   **Power BI** – real-time dashboard visualizing incoming data and metrics.
    
-   **Azure Resource Manager / CLI** – for provisioning the Resource Group, Event Hubs namespace/hub, Databricks cluster, Function App, etc.

---


## Workflow
### 1. Resource Provisioning & Event Hub Setup

First, provision the core resources in Azure:

-   **Resource Group:** Create a new resource group (e.g. `RG-weather-streaming`).
    
-   **Event Hubs Namespace & Hub:** In that RG, create an **Event Hubs namespace**. Inside it, add an Event Hub instance named `weather-streaming-event-hub`.
    
-   **Access Policy:** Under the Event Hub, create a shared-access policy (e.g. `send-policy`) with **Send** permissions. Copy its Primary Connection String.
    
-   **Key Vault:** Create an Azure Key Vault (e.g. `Key-weather-streaming`) in the same RG. Store secrets for:
    
    -   **Weather API Key** (secret name `weatherApiKey`).
        
    -   **Event Hub Conn String** (secret name `eventhub-connection-string-secret`).
        
-   **Access Control:** Grant your user (or service principals) the **Key Vault Secrets User** role so they can retrieve these secrets at runtime. (For example, assign it to the Databricks service principal and the Function App’s managed identity.)
    

### 2. Databricks Ingestion (Batch Polling)

As one ingestion path, set up a Databricks notebook to poll the API and send to Event Hub:

-   **Databricks Cluster:** Launch a cluster (`streaming-cluster`) with a recent LTS runtime (e.g. 10.4 Spark 3.2) and default VM size. Disable auto-terminate to keep it running.
    
-   **Libraries:** On the cluster, install the PyPI package `azure-eventhub==5.12.2` and restart the cluster.
    
-   **Notebook Code:** Create a PySpark/Python notebook (`weather-streaming-notebook`). In it, read the Event Hub connection string from Key Vault (or paste it temporarily for testing). Use `EventHubProducerClient` to send events. 
    
-   **Testing:** Run the notebook to send a test event to Event Hub. You can verify events under the Event Hub namespace (Data Explorer → **View events**) to confirm messages are arriving.
    

### 3. Azure Functions Ingestion (Real-time)

As a lightweight serverless alternative, create an Azure Function to push data continuously:

-   **Function App:** In the RG, create a new Function App (Consumption plan, Python 3.12 on Linux, e.g. name `funcApp-weather-streaming`). Disable Application Insights to reduce cost.
    
-   **Configuration:** Enable the Function App’s **System-Assigned Managed Identity**. In Key Vault, add an access policy granting this identity **Secret User** rights.
    
-   **Function Code (Timer Trigger):** Implement a Python function (timer trigger, e.g. every 30 seconds) that:
    
    -   Retrieves the Weather API key and Event Hub connection string from Key Vault.
        
    -   Calls the weather API and parses the JSON.
        
    -   Sends the JSON payload to Event Hub using the Azure Event Hubs SDK (similarly to the Databricks code).  
        The code follows a pattern like the snippet above, but inside the Function.
        
-   **Deployment:** Deploy the function (via VS Code, ZIP, or CLI). In Function App → Configuration, ensure any necessary settings (e.g. time interval) are set.
    
-   **Start Function:** Initially stop the function to simulate outage. When you start it, it will begin pushing events.
    

### 4. Stream Processing in Microsoft Fabric

Next, set up Fabric to move data from Event Hub into Kusto:

-   **Fabric Event Stream Pipeline:** In Microsoft Fabric’s Data Factory-like UI, create an **Event Stream** pipeline. Configure it to read from the Event Hubs namespace and `weather-streaming-event-hub`.
    
-   **Kusto Sink:** Configure the sink to write to an existing Fabric Kusto database (create one if needed, e.g. `WeatherKustoDB` with table `WeatherData`). Map the incoming JSON fields to table columns (e.g. `timestamp`, `temperature`, `AQI`, etc.).
    
-   **Activate Pipeline:** Run the pipeline. Verify the pipeline status is **Active**; it should continuously pull new events as they arrive. In the Fabric UI, monitor the pipeline to ensure data is being loaded without errors.file-gtdsayi4bemytduos8vxz4
    
-   **Data Validation:** Use the **Kusto Query Explorer** (in Fabric or Azure Data Explorer) to query the `WeatherData` table. The query results should show up-to-date metrics as new events are streamed.
    

### 5. Data Visualization & Alerts

Finally, connect the data to a real-time dashboard and alerts:

-   **Power BI Report:** Open Power BI (Desktop or Fabric). Connect to the Kusto table via DirectQuery (using the Fabric workspace). Build visuals (tables, cards, graphs) for key metrics. Publish the report to the Fabric workspace.
    
-   **Live Refresh:** Configure the dataset to use **Live (DirectQuery)** mode so visuals auto-update. (In Fabric, dashboards auto-refresh as data changes.)
    
-   **Real-time Dashboard:** Open the Power BI dashboard in the workspace; it should display current weather stats. For example, after restarting the Function App, you will see the total row count increase immediately (e.g. “Rows = 544” from a previous 541)file-gtdsayi4bemytduos8vxz4, demonstrating real-time refresh.
    
-   **Alerts (Data Activator):** In Microsoft Fabric, use **Data Activator** to define rules on the Kusto data. For instance: `WeatherData | where Temperature > 40`. Configure an email action so that when the rule fires (e.g. new data point crosses threshold), stakeholders get notified.

---

## Execution-with-Screenshots
### 1. Configure the Weather API Data Source

1.  Find the API  
	-   Search “weather API” → select [weatherapi.com](http://weatherapi.com).  
2.  Sign Up & Verify
	-   Enter email, confirm email, set a strong password, complete CAPTCHA, accept terms, Sign up.
	-   Click the verification link sent by email.  
3.  Retrieve API Key  
	-   Log into the portal → copy the API key displayed on the dashboard (you’ll use this in Azure services).
![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/1.PNG?raw=true)

### 2. Create an Azure Resource Group

1.  Open Azure Portal (portal.azure.com).  
2.  Create Resource Group  
	-   Select Resource groups → Create.  
	-   Subscription: Your subscription  
	-   Name: e.g. RG-weather-streaming  
	-   Region: nearest  
	-   (Optional) Add tags (e.g. CreatedBy=YourName)  


### 3. Provision Azure Databricks Workspace

1.  In RG → Create Resource → filter Azure services only → search “Databricks” → Create Azure Databricks.    
2.  Basics  
-   Subscription & Resource Group: RG-weather-streaming  
-   Workspace Name: DataB-weather-streaming  
-   Region: Nearest  
-   Pricing Tier: Premium (recommended for RBAC, Unity Catalog, networking (check FAQ section for explanation)) 
3.  Networking, Encryption, Security & Tags  
	-   Leave defaults (outsourced to managed RG).
	- Deploy Azure Databricks workspace with Secure Cluster Connectivity (No Public IP) -> No  
4. Review & Create → wait for deployment → Go to resource → Launch Workspace.    


### 4. Deploy Azure Function App

1.  In RG → Create Resource → filter Azure services only → find Function App → Create.  
2.  Hosting Plan  
	-   Choose Consumption Plan (pay-per-execution).
![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/2.png?raw=true)
3.  Basics  
	-   Subscription & RG: RG-weather-streaming    
	-   Name: funcApp-weather-streaming    
	-   Runtime Stack: Python (version 3.12)     
	-   Region: Central India    
	-   OS: Linux  

4.  Storage
	-   Accept default (auto-create Storage Account).
	![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/3.PNG?raw=true)
Function app uses its own storage to store cache, it will be later used to create checkpoint, logging, etc.

5.  Networking  
	-   Leave defaults (no VNet).  
6.  Monitoring  
	-   Disable Application Insights (to reduce cost).  
7.  Deployment  
	-   Skip CI/CD for now (can add later).  
8.  Tags  
	-   Skip or add as needed.  
9.  Review & Create → wait for deployment → Go to Resource.
![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/4.PNG?raw=true)
### 5. Set Up Azure Event Hub Namespace

1.  In RG → Create Resource → search Event Hubs → Create.  
2.  Basics  
	-   Subscription & RG: RG-weather-streaming  
	-   Namespace Name: EventH-weather-streaming-namespace  
	-   Region: Central India  
3.  Pricing Tier  
	-   Select Basic (0.028 NZD per 1 M events; 1 day retention)     
	-   Throughput Units: 1 (up to 1 MB or 1 000 events/sec)  
4.  Advanced & Networking  
	-   Leave defaults (TLS v1.2, no private endpoints).  
5.  Tags  
	-   Skip or add as desired.  
6. Review & Create → wait for deployment → Go to Resource.

### 6. Create Azure Key Vault & Store API Key
1.  In RG → Create Resource → filter Azure services only → find Key Vault → Create.  
2.  Basics  
	-   Subscription & RG: RG-weather-streaming  
	-   Name: Key-weather-streaming2    
	-   Region: Central India  
	-   Pricing Tier: Standard  
	-   Soft-delete: Enabled (90 days)  
	-   Purge Protection: Disabled  
3.  Access Configuration  
	-   Permission Model: Azure RBAC (recommended)  
4.  Networking & Tags  
	-   Leave defaults.  
5.  Review & Create → wait for deployment → Go to Resource.

Initially after key vault creation it will not let us create secret as our email id doesn't have proper access:
![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/5.PNG?raw=true)
To access we need to enable the right permission hence the below step.  
  
6.  Grant Yourself Secret-Officer Role  
	-   Access Control → Role assignments → Add → Key Vault Secrets Officer → assign to your user.
    
Please note after this access we will be able to access Key Vault secret however we won’t be able to access keys and certificates, the same error will pop up. It's a good practice as this will be more secure.
I will be using the weather API key in other ms resources like databricks, its not recommended to directly copy paste the key instead the API key will be in the secret vault and the secret should be used by other resources. Its safer this way

7.  Add Secret
	-   Secrets → Generate/Import → Name: weatherApiKey  
	- Paste the API key value → Create.

### 7. Configure the Event Hub inside event hub namespace  
-   Inside that namespace, add an Event Hub instance named weather-streaming-event-hub.   
-   Use the default 1 partition and 1-hour retention (Basic tier), since downstream storage will handle longer-term data.  
-  Leave Capture off (unsupported on Basic and not needed for real-time processing).

### 8. Set Up a Send-Only Shared Access Policy (Used for connecting Event hubs and databricks)  
-   In the Event Hub’s Shared access policies, add a policy called for-databricks with only Send permission.
![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/6.PNG?raw=true)
- Copy its Primary connection string.
![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/7.PNG?raw=true)

### 9. Secure the Event Hub Connection String in Key Vault  
-   In your Azure Key Vault, create a new secret named eventhub-connection-string-secret.  
-   Paste in the connection string you just copied.
- Ensure your Databricks service principal (or your user) has the Key Vault Secrets User role so it can retrieve this secret.


### 10. Provision & Configure Your Databricks Cluster  
-   In Azure Databricks, spin up a cluster named streaming cluster:  
	-   Runtime: 10.4 LTS (includes Apache Spark 3.2.1, Scala 2.12)  
	-   Driver & Worker type: Default  
	-  Auto-termination: Off (so it stays up for continuous streaming)
![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/8.PNG?raw=true)
### 11. Install the Azure Event Hubs Library on Databricks  
-   On the streaming-cluster, install the PyPI library azure-eventhub==5.12.2.
-  Restart the cluster to load the library.

### 12. We can check what data is present in the event hub
- By accessing the Event hub namespace -> Data Explorer -> view events.

### 13. Send a Test Event from Databricks  
-   Create a notebook (weather-streaming-notebook) attached to streaming-cluster.
- Hard-code the connection string and Event Hub name, then run (Sending Test event to Event Hub):
```python
# Import the necessary classes from the Azure Event Hubs SDK
from azure.eventhub import EventHubProducerClient, EventData
import json

# ──────────────────────────────────────────────────────────────────────────────

# 1. Event Hub Configuration

# ──────────────────────────────────────────────────────────────────────────────

  

# Replace with your Event Hub namespace connection string, Event hub -> shared access policies -> open our databrick policy -> copy connection string primary key

EVENT_HUB_CONNECTION_STRING =  "Endpoint=sb://eventh-weather-streaming-namespace.servicebus.windows.net/;SharedAccessKeyName=for-databricks;SharedAccessKey=viPgu7KNfVILLyz+A7X2vP0uI1HBbXnEN+AEhI4oOqg=;EntityPath=weather-streaming-event-hub"

  

# Replace with your Event Hub name (the specific hub within the namespace)

EVENT_HUB_NAME =  "weather-streaming-event-hub"

  

# ──────────────────────────────────────────────────────────────────────────────

# 2. Initialize the Event Hub Producer

# ──────────────────────────────────────────────────────────────────────────────

  

# Create a producer client to send messages to the Event Hub, responsidble for sending the vents towards the event hub

producer = EventHubProducerClient.from_connection_string(

conn_str=EVENT_HUB_CONNECTION_STRING,

eventhub_name=EVENT_HUB_NAME

)

  

# ──────────────────────────────────────────────────────────────────────────────

# 3. Define a Function to Send Events

# ──────────────────────────────────────────────────────────────────────────────

  

def  send_event(event: dict):

"""

Sends a single JSON-serializable event to Azure Event Hub.

  

Parameters:

event (dict): The event payload to send.

"""

# Create a new batch. Batching helps optimize throughput.

event_data_batch = producer.create_batch()

# Serialize the event dict to a JSON string and wrap it in EventData

event_json = json.dumps(event)

event_data =  EventData(event_json)

  

"""

event = {

"event_id": 42,

"event_name": "temperature_reading",

"value_celsius": 23.7,

"timestamp": "2025-05-27T12:34:56Z"

}

event_json = json.dumps(event)

print(event_json)

# Output:

# {"event_id": 42, "event_name": "temperature_reading", "value_celsius": 23.7, "timestamp": "2025-05-27T12:34:56Z"}

  

# 3) Wrap that JSON string in an EventData object:

event_data = EventData(event_json)

  

# Internally, EventData stores the JSON as its body (in bytes).

# You can inspect it in Databricks like so:

print(event_data.body_as_str(encoding='UTF-8'))

# Output:

# {"event_id": 42, "event_name": "temperature_reading", "value_celsius": 23.7, "timestamp": "2025-05-27T12:34:56Z"}

  

"""

  

# Add this EventData to the batch

event_data_batch.add(event_data)

# Send the batch to the Event Hub

producer.send_batch(event_data_batch)

print(f"Sent event: {event_json}")

  

# ──────────────────────────────────────────────────────────────────────────────

# 4. Create a Sample Event and Send It

# ──────────────────────────────────────────────────────────────────────────────

  

if  __name__  ==  "__main__":

# Example payload — can be any JSON-serializable content

sample_event = {

"event_id": 1111,

"event_name": "Test Event"

}

# Send the sample event to validate connectivity

send_event(sample_event)

# ──────────────────────────────────────────────────────────────────────────

# 5. Clean Up

# ──────────────────────────────────────────────────────────────────────────

# Close the producer to free up resources

producer.close()

print("Event Hub producer closed.")
```
#### How this works:

-   Configuration: You specify your Event Hub connection string and the hub name.  
      
    
-   Producer Initialization: A single EventHubProducerClient handles communication.  
      
    
-   Batching: We create a batch before sending to optimize performance.  
      
    
-   Serialization: Events are JSON-encoded, wrapped in EventData, then added to the batch.  
      
-   Sending & Cleanup: We send the batch and close the producer when done.
    
-   In the Azure portal’s Event Hub Data Explorer, confirm you see your message.
![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/9.PNG?raw=true)

### 14. Wire Up Key Vault Secrets in Databricks  
-   First establish connection between adb and key vault.
- In Databricks, create a Secret Scope (e.g. KeyVaultScope) backed by your Key Vault.

#### Create an Azure Key Vault-backed secret scope

- Go to https://databricks-instance#secrets/createScope. Replace databricks-instance with the [workspace URL](https://learn.microsoft.com/en-us/azure/databricks/workspace/workspace-details#workspace-url) of your Azure Databricks deployment. This URL is case sensitive. For example, scope in createScope must use an uppercase S).
![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/10.PNG?raw=true)
-  Enter the name of the secret scope. Secret scope names are case insensitive.
    
- In Manage Principal select Creator or All workspace users to specify which users have the MANAGE permission on the secret scope.  
    The MANAGE permission allows users to read, write, and grant permissions on the scope. Your account must have the [Premium plan](https://databricks.com/product/pricing/platform-addons) to choose Creator.
    
-  Enter DNS Name and Resource ID:
![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/11.PNG?raw=true)
- These properties are available from the Key vault -> Settings > Properties tab.
![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/12.PNG?raw=true)
- Click Create.

#### Update your notebook to fetch the connection string securely: 
```python
# Import the necessary classes from the Azure Event Hubs SDK

from azure.eventhub import EventHubProducerClient, EventData

import json

  

# ──────────────────────────────────────────────────────────────────────────────

# 1. Event Hub Configuration

# ──────────────────────────────────────────────────────────────────────────────

  

  

# Retrieve Connection String Securely from Key Vault

# ──────────────────────────────────────────────────────────────────────────────

# This assumes you've already created a Databricks secret scope named "key-vault-secret-scope"

# and stored your Event Hub connection string under the secret name "eventhub-connection-string-secret".

eventhub_connection_string = dbutils.secrets.get(

scope="key-vault-secret-scope",

key="eventhub-connection-string-secret"

)

  

# Replace with your Event Hub name (the specific hub within the namespace)

EVENT_HUB_NAME =  "weather-streaming-event-hub"

  

# ──────────────────────────────────────────────────────────────────────────────

# 2. Initialize the Event Hub Producer

# ──────────────────────────────────────────────────────────────────────────────

  

# Create a producer client to send messages to the Event Hub, responsidble for sending the vents towards the event hub

producer = EventHubProducerClient.from_connection_string(

conn_str=eventhub_connection_string,

eventhub_name=EVENT_HUB_NAME

)

  

# ──────────────────────────────────────────────────────────────────────────────

# 3. Define a Function to Send Events

# ──────────────────────────────────────────────────────────────────────────────

  

def  send_event(event: dict):

"""

Sends a single JSON-serializable event to Azure Event Hub.

  

Parameters:

event (dict): The event payload to send.

"""

# Create a new batch. Batching helps optimize throughput.

event_data_batch = producer.create_batch()

# Serialize the event dict to a JSON string and wrap it in EventData

event_json = json.dumps(event)

event_data =  EventData(event_json)

# Add this EventData to the batch

event_data_batch.add(event_data)

# Send the batch to the Event Hub

producer.send_batch(event_data_batch)

print(f"Sent event: {event_json}")

  

# ──────────────────────────────────────────────────────────────────────────────

# 4. Create a Sample Event and Send It

# ──────────────────────────────────────────────────────────────────────────────

  

if  __name__  ==  "__main__":

# Example payload — can be any JSON-serializable content

sample_event = {

"event_id": 2222,

"event_name": "Secret scope test"

}

# Send the sample event to validate connectivity

send_event(sample_event)

# ──────────────────────────────────────────────────────────────────────────

# 5. Clean Up

# ──────────────────────────────────────────────────────────────────────────

# Close the producer to free up resources

producer.close()

print("Event Hub producer closed.")
```
  
But if we run the above we will get an error, additional permission is required for the dbw to access keyvault so:

Key vault-> Access control -> add roles -> choose key vault secret user -> azuredatabricks will be the service principle
![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/13.PNG?raw=true)
Rerun the test send; verify in Data Explorer that the message arrives.

### 15. Review Weather API Docs

-   Navigate into the Azure Databricks workspace.
    
-   Open a second browser tab to weatherapi.com and click Docs → Current to inspect authentication, endpoint URLs, and parameters.
    
-   Identify the required parameters:
	-   key – your API key (stored in Key Vault).  
	-   q – query string for location (city name, coords, ZIP, etc.).

- Note available API methods:

	-   current.json for live data  
      
	-   forecast.json (with days parameter)  
      
	-   alerts.json (with alerts flag)  
   
	- aqi toggle for Air Quality Index


### 16. Paste & Explain the “Current Weather” Test Script

Insert a new code cell and paste the test script:
```python
# ──────────────────────────────────────────────────────────────────────────────

# 1. Imports

# ──────────────────────────────────────────────────────────────────────────────

import requests # For making HTTP requests to the Weather API

import json # For parsing and pretty-printing JSON responses

  

# ──────────────────────────────────────────────────────────────────────────────

# 2. Retrieve API Key from Azure Key Vault

# ──────────────────────────────────────────────────────────────────────────────

# This assumes you have a Databricks secret scope named "key-vault-scope"

# and that you've stored your Weather API key under the secret name "weather-api-key" in Azure key vault.

weather_api_key = dbutils.secrets.get(

scope="key-vault-secret-scope",

key="weather-api-key"

)

  

# ──────────────────────────────────────────────────────────────────────────────

# 3. Define the API Endpoint and Parameters

# ──────────────────────────────────────────────────────────────────────────────

location =  "Kolkata"  # City for which you want the weather

base_url =  "http://api.weatherapi.com/v1"

current_url =  f"{base_url}/current.json"

  

# The Weather API expects two query parameters:

# - 'key': your API key

# - 'q': the location string (city name, coordinates, ZIP, etc.)

params = {

"key": weather_api_key,

"q": location

}

  

# ──────────────────────────────────────────────────────────────────────────────

# 4. Make the HTTP GET Request

# ──────────────────────────────────────────────────────────────────────────────

response = requests.get(current_url, params=params)

  

# ──────────────────────────────────────────────────────────────────────────────

# 5. Handle the Response

# ──────────────────────────────────────────────────────────────────────────────

if response.status_code ==  200:

# 200 means Success: parse the JSON payload

current_weather = response.json()

# Pretty-print the weather data

print("Current Weather:")

print(json.dumps(current_weather, indent=2))

else:

# Something went wrong: print status code and error text

print(f"Error: {response.status_code}, {response.text}")
```

### What’s happening here?

1.  Imports  
    We bring in requests to call the Weather API and json to decode and neatly print the JSON response.  
      
    
2.  Secure Configuration  
    Rather than hard-coding your API key, we retrieve it safely from Azure Key Vault via a Databricks secret scope (dbutils.secrets.get).  
      
    
3.  Endpoint & Parameters  
    We construct the full URL for the “current weather” endpoint and define a params dictionary containing our API key and desired location.  
      
    
4.  HTTP Request  
    requests.get(...) sends a GET request with our parameters → returns a response object.  
      
    
5.  Response Handling  
      
    
-   If response.status_code == 200, we know it succeeded → call .json() to decode the payload into a Python dict → pretty-print it.  
      
    
-   Otherwise, we log the error code and any error message from the API.  
      
    
This simple pattern—secure key retrieval, parameter setup, request, and response check—is a reliable template for calling any RESTful API in a production notebook.


### 17. Define the Full “Fetch Weather Data” Code

-   Imports: requests, json  

-   handle_response(resp) → checks status_code == 200, returns resp.json() or error message  

-   get_current_weather(base_url, key, loc) → calls current.json + aqi=y, returns JSON via handle_response  

-   get_forecast_weather(base_url, key, loc, days) → calls forecast.json?days={days}  

-   get_alerts(base_url, key, loc) → calls alerts.json?alerts=y  

-   flatten_data(curr, forecast, alerts) → extracts and merges fields into a single flat dict with:  

	-   Location metadata  

	-   Temperature, condition, wind, pressure  

	-   AQI fields  

	-   Alert descriptions  

	-   Forecast days’ min/max temps + conditions  

-   fetch_weather_data(...) → orchestrates calls to the three “get_…” functions, then flatten_data, returns merged data  
    Run the cell and inspect the printed merged JSON to confirm correct structure: location, current → AQI → alerts → 3-day forecast.

```python
# # COMMAND ----------

# ### Complete code for getting weather data

  

# # COMMAND ----------

  

# ──────────────────────────────────────────────────────────────────────────────

# 0) Imports for full-featured API functions

# ──────────────────────────────────────────────────────────────────────────────

import requests

import json

  

# ──────────────────────────────────────────────────────────────────────────────

# 1) Helper: Uniform response handling

# ──────────────────────────────────────────────────────────────────────────────

def  handle_response(response):

"""

Returns parsed JSON on HTTP 200, otherwise an error message.

"""

if response.status_code ==  200:

return response.json()

return {"error": f"{response.status_code}: {response.text}"}

  

# ──────────────────────────────────────────────────────────────────────────────

# 2) Fetch current weather + AQI

# ──────────────────────────────────────────────────────────────────────────────

def  get_current_weather(base_url, api_key, location):

url =  f"{base_url}current.json"

params = {"key": api_key, "q": location, "aqi": "yes"}

response = requests.get(url, params=params)

return  handle_response(response)

  

# ──────────────────────────────────────────────────────────────────────────────

# 3) Fetch multi-day forecast

# ──────────────────────────────────────────────────────────────────────────────

def  get_forecast_weather(base_url, api_key, location, days):

url =  f"{base_url}/forecast.json"

params = {"key": api_key, "q": location, "days": days}

response = requests.get(url, params=params)

return  handle_response(response)

  

# ──────────────────────────────────────────────────────────────────────────────

# 4) Fetch weather alerts

# ──────────────────────────────────────────────────────────────────────────────

def  get_alerts(base_url, api_key, location):

url =  f"{base_url}/alerts.json"

params = {"key": api_key, "q": location, "alerts": "yes"}

response = requests.get(url, params=params)

return  handle_response(response)

  

# ──────────────────────────────────────────────────────────────────────────────

# 5) Flatten & merge disparate API responses

# ──────────────────────────────────────────────────────────────────────────────

def  flatten_data(current_weather, forecast_weather, alerts):

"""

Extracts key fields from each response and merges them into one flat dict.

"""

# Location & current conditions

loc = current_weather.get("location", {})

curr = current_weather.get("current", {})

cond = curr.get("condition", {})

aqi = curr.get("air_quality", {})

# Forecast days list and alerts list

forecast_days = forecast_weather.get("forecast", {}).get("forecastday", [])

alert_items = alerts.get("alerts", {}).get("alert", [])

# Build a single flat record

flattened = {

# Location metadata

"name": loc.get("name"),

"region": loc.get("region"),

"country": loc.get("country"),

"lat": loc.get("lat"),

"lon": loc.get("lon"),

"localtime": loc.get("localtime"),

# Current weather details

"temp_c": curr.get("temp_c"),

"condition_text": cond.get("text"),

"wind_kph": curr.get("wind_kph"),

"humidity": curr.get("humidity"),

# Air quality sub-fields

"air_quality": {k: aqi.get(k) for k in aqi.keys()},

# Active alerts (if any)

"alerts": [

{

"headline": a.get("headline"),

"severity": a.get("severity"),

"description": a.get("desc")

}

for a in alert_items

],

# 3-day forecast summary

"forecast": [

{

"date": d.get("date"),

"maxtemp_c": d.get("day", {}).get("maxtemp_c"),

"mintemp_c": d.get("day", {}).get("mintemp_c"),

"condition": d.get("day", {}).get("condition", {}).get("text")

}

for d in forecast_days

]

}

return flattened

  

# ──────────────────────────────────────────────────────────────────────────────

# 6) Main orchestration function

# ──────────────────────────────────────────────────────────────────────────────

def  fetch_weather_data():

"""

Retrieves current, forecast, and alert data, merges it,

and prints a single consolidated JSON payload.

"""

base_url =  "http://api.weatherapi.com/v1/"

location =  "Kolkata"

api_key = dbutils.secrets.get(scope="key-vault-secret-scope", key="weather-api-key")

# 1. Pull raw data

current =  get_current_weather(base_url, api_key, location)

forecast =  get_forecast_weather(base_url, api_key, location, days=3)

alerts =  get_alerts(base_url, api_key, location)

# 2. Normalize & merge into one record

merged =  flatten_data(current, forecast, alerts)

# 3. Output for inspection or downstream processing

print("Weather Data:")

print(json.dumps(merged, indent=3))

  

# ──────────────────────────────────────────────────────────────────────────────

# 7) Kick off the pipeline

# ──────────────────────────────────────────────────────────────────────────────

fetch_weather_data()
```

#### Complete Weather Fetch Logic  
  

-   Build reusable functions to get current conditions (incl. AQI), forecast, and alerts.  
      
    
-   Flatten each response into a single, easy-to-analyze dict.  
      
    
-   Print the merged payload for review or to hand off to your streaming pipeline.


### 18. Integrate Weather Fetch with Event Hub (Three-Step Approach)

#### Step 1: Send One Complete Weather Event

```python
# Import libraries for HTTP requests, JSON handling, and Event Hub communication

import requests

import json

from azure.eventhub import EventHubProducerClient, EventData

  

# ────────────────────────────────────────────────────────────────

# 1. Configure Event Hub connection

# ────────────────────────────────────────────────────────────────

  

# Securely retrieve the Event Hub connection string from Azure Key Vault

eventhub_connection_string = dbutils.secrets.get(

scope="key-vault-secret-scope",

key="eventhub-connection-string-secret"

)

  

# Define the Event Hub name where the event will be published

EVENT_HUB_NAME =  "weather-streaming-event-hub"

  

# Initialize the Event Hub producer client using the secure connection string

producer = EventHubProducerClient.from_connection_string(

conn_str=eventhub_connection_string,

eventhub_name=EVENT_HUB_NAME

)

  

# ────────────────────────────────────────────────────────────────

# 2. Function to send an event (JSON) to Azure Event Hub

# ────────────────────────────────────────────────────────────────

  

def  send_event(event):

# Create a new batch to group events before sending

event_data_batch = producer.create_batch()

# Convert the event dictionary into a JSON string and wrap it in EventData

event_data_batch.add(EventData(json.dumps(event)))

# Send the batch to the Event Hub

producer.send_batch(event_data_batch)

  

# ────────────────────────────────────────────────────────────────

# 3. Helper function to handle HTTP response

# ────────────────────────────────────────────────────────────────

  

def  handle_response(response):

if response.status_code ==  200:

# Return parsed JSON if request was successful

return response.json()

else:

# Return an error message if request failed

return  f"Error: {response.status_code}, {response.text}"

  

# ────────────────────────────────────────────────────────────────

# 4. Functions to retrieve weather data from the API

# ────────────────────────────────────────────────────────────────

  

def  get_current_weather(base_url, api_key, location):

# Call the current weather API including air quality info

current_weather_url =  f"{base_url}/current.json"

params = {'key': api_key, 'q': location, "aqi": 'yes'}

response = requests.get(current_weather_url, params=params)

return  handle_response(response)

  

def  get_forecast_weather(base_url, api_key, location, days):

# Call the forecast API for specified number of days

forecast_url =  f"{base_url}/forecast.json"

params = {"key": api_key, "q": location, "days": days}

response = requests.get(forecast_url, params=params)

return  handle_response(response)

  

def  get_alerts(base_url, api_key, location):

# Call the alerts API to check for severe weather

alerts_url =  f"{base_url}/alerts.json"

params = {'key': api_key, 'q': location, "alerts": 'yes'}

response = requests.get(alerts_url, params=params)

return  handle_response(response)

  

# ────────────────────────────────────────────────────────────────

# 5. Merge and flatten all API responses into a single dict

# ────────────────────────────────────────────────────────────────

  

def  flatten_data(current_weather, forecast_weather, alerts):

# Safely extract sections from nested API responses

location_data = current_weather.get("location", {})

current = current_weather.get("current", {})

condition = current.get("condition", {})

air_quality = current.get("air_quality", {})

forecast = forecast_weather.get("forecast", {}).get("forecastday", [])

alert_list = alerts.get("alerts", {}).get("alert", [])

  

# Create a clean, structured event record

flattened_data = {

# Location metadata

'name': location_data.get('name'),

'region': location_data.get('region'),

'country': location_data.get('country'),

'lat': location_data.get('lat'),

'lon': location_data.get('lon'),

'localtime': location_data.get('localtime'),

  

# Current weather conditions

'temp_c': current.get('temp_c'),

'is_day': current.get('is_day'),

'condition_text': condition.get('text'),

'condition_icon': condition.get('icon'),

'wind_kph': current.get('wind_kph'),

'wind_degree': current.get('wind_degree'),

'wind_dir': current.get('wind_dir'),

'pressure_in': current.get('pressure_in'),

'precip_in': current.get('precip_in'),

'humidity': current.get('humidity'),

'cloud': current.get('cloud'),

'feelslike_c': current.get('feelslike_c'),

'uv': current.get('uv'),

  

# Air quality measurements

'air_quality': {

'co': air_quality.get('co'),

'no2': air_quality.get('no2'),

'o3': air_quality.get('o3'),

'so2': air_quality.get('so2'),

'pm2_5': air_quality.get('pm2_5'),

'pm10': air_quality.get('pm10'),

'us-epa-index': air_quality.get('us-epa-index'),

'gb-defra-index': air_quality.get('gb-defra-index')

},

  

# Weather alerts

'alerts': [

{

'headline': alert.get('headline'),

'severity': alert.get('severity'),

'description': alert.get('desc'),

'instruction': alert.get('instruction')

}

for alert in alert_list

],

  

# 3-day forecast

'forecast': [

{

'date': day.get('date'),

'maxtemp_c': day.get('day', {}).get('maxtemp_c'),

'mintemp_c': day.get('day', {}).get('mintemp_c'),

'condition': day.get('day', {}).get('condition', {}).get('text')

}

for day in forecast

]

}

  

return flattened_data

  

# ────────────────────────────────────────────────────────────────

# 6. Main function to coordinate all steps

# ────────────────────────────────────────────────────────────────

  

def  fetch_weather_data():

# Base URL of the Weather API

base_url =  "http://api.weatherapi.com/v1/"

# Desired city

location =  "Kolkata"

  

# Get the Weather API key securely from Key Vault

weatherapikey = dbutils.secrets.get(

scope="key-vault-secret-scope",

key="weather-api-key"

)

  

# Step 1: Call all three API endpoints

current_weather =  get_current_weather(base_url, weatherapikey, location)

forecast_weather =  get_forecast_weather(base_url, weatherapikey, location, 3)

alerts =  get_alerts(base_url, weatherapikey, location)

  

# Step 2: Merge into a flat event structure

merged_data =  flatten_data(current_weather, forecast_weather, alerts)

  

# Step 3: Send to Event Hub

send_event(merged_data)

  

# ────────────────────────────────────────────────────────────────

# 7. Trigger the process

# ────────────────────────────────────────────────────────────────

  

# This line initiates the full process: fetch → merge → send

fetch_weather_data()
```

Run and verify in the Event Hub Data Explorer → view that one event arrives with full weather JSON.

#### The above code only sends one event with the latest weather data.

#### Step 2: Spark Structured Streaming Loop (1 sec rate)

```python
# ──────────────────────────────────────────────────────────────────────────────

# 1) Imports: Event Hub client, JSON, HTTP requests, and Spark

# ──────────────────────────────────────────────────────────────────────────────

from azure.eventhub import EventHubProducerClient, EventData # Azure SDK for sending events

import json # JSON serialization

import requests # REST API calls

# Note: Spark is already available in Databricks for streaming

  

# ──────────────────────────────────────────────────────────────────────────────

# 2) Secure configuration: retrieve secrets from Key Vault

# ──────────────────────────────────────────────────────────────────────────────

EVENT_HUB_NAME =  "weather-streaming-event-hub"

eventhub_connection_string = dbutils.secrets.get(

scope="key-vault-secret-scope",

key="eventhub-connection-string-secret"

)

weatherapikey = dbutils.secrets.get(

scope="key-vault-secret-scope",

key="weather-api-key"

)

  

# ──────────────────────────────────────────────────────────────────────────────

# 3) Initialize the Event Hub producer client once

# ──────────────────────────────────────────────────────────────────────────────

producer = EventHubProducerClient.from_connection_string(

conn_str=eventhub_connection_string,

eventhub_name=EVENT_HUB_NAME

)

  

# ──────────────────────────────────────────────────────────────────────────────

# 4) Helper: wrap a dict as JSON and send to Event Hub

# ──────────────────────────────────────────────────────────────────────────────

def  send_event(event: dict):

batch = producer.create_batch() # Create a new batch container

batch.add(EventData(json.dumps(event))) # Serialize dict → JSON → EventData

producer.send_batch(batch) # Push the batch to the hub

  

# ──────────────────────────────────────────────────────────────────────────────

# 5) Helper: unified API response handling

# ──────────────────────────────────────────────────────────────────────────────

def  handle_response(response):

if response.status_code ==  200:

return response.json() # Return parsed JSON on success

return { "error": f"{response.status_code}: {response.text}" }

  

# ──────────────────────────────────────────────────────────────────────────────

# 6) Weather API calls (current, forecast, alerts)

# ──────────────────────────────────────────────────────────────────────────────

def  get_current_weather(base_url, api_key, location):

url =  f"{base_url}/current.json"

params = {'key': api_key, 'q': location, 'aqi': 'yes'}

return  handle_response(requests.get(url, params=params))

  

def  get_forecast_weather(base_url, api_key, location, days):

url =  f"{base_url}/forecast.json"

params = {'key': api_key, 'q': location, 'days': days}

return  handle_response(requests.get(url, params=params))

  

def  get_alerts(base_url, api_key, location):

url =  f"{base_url}/alerts.json"

params = {'key': api_key, 'q': location, 'alerts': 'yes'}

return  handle_response(requests.get(url, params=params))

  

# ──────────────────────────────────────────────────────────────────────────────

# 7) Flatten nested API outputs into one simple dict

# ──────────────────────────────────────────────────────────────────────────────

def  flatten_data(current, forecast, alerts):

loc = current.get("location", {})

cur = current.get("current", {})

cond = cur.get("condition", {})

aqi = cur.get("air_quality", {})

days = forecast.get("forecast", {}).get("forecastday", [])

alerts_list = alerts.get("alerts", {}).get("alert", [])

  

return {

"name": loc.get("name"),

"region": loc.get("region"),

"country": loc.get("country"),

"temp_c": cur.get("temp_c"),

"condition": cond.get("text"),

"air_quality": {k: aqi.get(k) for k in aqi},

"forecast": [

{

"date": d.get("date"),

"max_temp": d.get("day", {}).get("maxtemp_c"),

"min_temp": d.get("day", {}).get("mintemp_c"),

"condition": d.get("day", {}).get("condition", {}).get("text")

}

for d in days

],

"alerts": [

{

"headline": a.get("headline"),

"severity": a.get("severity"),

"description": a.get("desc")

}

for a in alerts_list

]

}

  

# ──────────────────────────────────────────────────────────────────────────────

# 8) Fetch + flatten all weather data (no sending)

# ──────────────────────────────────────────────────────────────────────────────

def  fetch_weather_data():

base_url =  "http://api.weatherapi.com/v1"

location =  "Kolkata"  # or any city

# Call each endpoint

curr =  get_current_weather(base_url, weatherapikey, location)

forecast=  get_forecast_weather(base_url, weatherapikey, location, days=3)

alrt =  get_alerts(base_url, weatherapikey, location)

# Merge into one record

return  flatten_data(curr, forecast, alrt)

  

# ──────────────────────────────────────────────────────────────────────────────

# 9) Batch processor invoked by Spark Structured Streaming

# ──────────────────────────────────────────────────────────────────────────────

def  process_batch(batch_df, batch_id):

"""

Called for each micro-batch of the Spark stream.

We ignore batch_df content (using rate source) and instead

fetch real weather data and send it.

"""

try:

data =  fetch_weather_data() # Get fresh weather snapshot

send_event(data) # Publish to Event Hub

except  Exception  as e:

# Log and rethrow to allow Spark to surface the error

print(f"Error in batch {batch_id}: {e}")

raise

  

# ──────────────────────────────────────────────────────────────────────────────

# 10) Define a dummy streaming source (rate) for pacing

# ──────────────────────────────────────────────────────────────────────────────

streaming_df = (

spark.readStream

.format("rate") # Built-in source that generates rows at a fixed rate

.option("rowsPerSecond", 1) # 1 row per second → trigger one batch per second

.load()

)

  

# ──────────────────────────────────────────────────────────────────────────────

# 11) Hook our processor into the streaming query

# ──────────────────────────────────────────────────────────────────────────────

query = (

streaming_df.writeStream

.foreachBatch(process_batch) # Call process_batch() each micro-batch

.start()

)

  

# Wait forever (or until manually stopped)

query.awaitTermination()

  

# ──────────────────────────────────────────────────────────────────────────────

# 12) Cleanup: close the Event Hub producer when done

# ──────────────────────────────────────────────────────────────────────────────

producer.close()
```

#### How It Works: High-Level Flow

1.  Secret Retrieval  
    We pull both the Event Hub connection string and the Weather API key securely from Azure Key Vault via Databricks secrets.  
      
    
2.  Event Hub Producer Initialization  
    A single long-lived EventHubProducerClient is created for efficient batching and reuse.  
      
    
3.  API Wrappers + Flattening  
    We define three small functions to call the Weather API endpoints, plus a helper to flatten their nested JSON into a single record.  
      
    
4.  Spark Structured Streaming  

-   We use the built-in rate source purely to trigger a micro-batch at a fixed interval (1 row/s → 1 batch/s).  

-   foreachBatch(process_batch) runs our custom process_batch function once per batch.  
      
6.  Batch Processing  
    Inside process_batch, we ignore the dummy data and instead call fetch_weather_data() to get the latest weather, then immediately send_event(...) to push that record to Event Hub.  
      

7.  Termination & Cleanup  
    When the stream is stopped, we awaitTermination() and finally close the Event Hub producer to free resources.
 
Run and watch — one event per second floods into Event Hub.

#### Step 3: Throttle to Every 30 Seconds (as the weather data doesn't change every second)

```python
# 1) Imports: Event Hub client, JSON, HTTP, and scheduling utilities

# ──────────────────────────────────────────────────────────────────────────────

from azure.eventhub import EventHubProducerClient, EventData # Azure SDK for sending events

import json # JSON serialization

import requests # REST API calls

from datetime import datetime, timedelta # Time comparison for throttling

  

# ──────────────────────────────────────────────────────────────────────────────

# 2) Secure configuration: retrieve secrets from Azure Key Vault

# ──────────────────────────────────────────────────────────────────────────────

EVENT_HUB_NAME =  "weather-streaming-event-hub"

eventhub_connection_string = dbutils.secrets.get(

scope="key-vault-secret-scope",

key="eventhub-connection-string-secret"  # Secret storing the Event Hub connection

)

weather_api_key = dbutils.secrets.get(

scope="key-vault-secret-scope",

key="weather-api-key"  # Secret storing the Weather API key

)

  

# ──────────────────────────────────────────────────────────────────────────────

# 3) Initialize a single Event Hub producer client

# ──────────────────────────────────────────────────────────────────────────────

producer = EventHubProducerClient.from_connection_string(

conn_str=eventhub_connection_string,

eventhub_name=EVENT_HUB_NAME

)

  

# ──────────────────────────────────────────────────────────────────────────────

# 4) Helper: send a Python dict as JSON to Event Hub

# ──────────────────────────────────────────────────────────────────────────────

def  send_event(event: dict):

batch = producer.create_batch() # Create an optimized batch container

batch.add(EventData(json.dumps(event))) # Serialize dict → JSON → EventData

producer.send_batch(batch) # Publish the batch to Event Hub

  

# ──────────────────────────────────────────────────────────────────────────────

# 5) Helper: unified API response handling

# ──────────────────────────────────────────────────────────────────────────────

def  handle_response(response):

if response.status_code ==  200:

return response.json() # Return parsed JSON on success

return { "error": f"{response.status_code}: {response.text}" }

  

# ──────────────────────────────────────────────────────────────────────────────

# 6) Weather API wrappers: current, forecast, alerts

# ──────────────────────────────────────────────────────────────────────────────

def  get_current_weather(base_url, api_key, location):

url =  f"{base_url}/current.json"

params = {'key': api_key, 'q': location, 'aqi': 'yes'}

return  handle_response(requests.get(url, params=params))

  

def  get_forecast_weather(base_url, api_key, location, days):

url =  f"{base_url}/forecast.json"

params = {'key': api_key, 'q': location, 'days': days}

return  handle_response(requests.get(url, params=params))

  

def  get_alerts(base_url, api_key, location):

url =  f"{base_url}/alerts.json"

params = {'key': api_key, 'q': location, 'alerts': 'yes'}

return  handle_response(requests.get(url, params=params))

  

# ──────────────────────────────────────────────────────────────────────────────

# 7) Flatten and merge API responses into one clean dict

# ──────────────────────────────────────────────────────────────────────────────

def  flatten_data(current, forecast, alerts):

loc = current.get("location", {})

cur = current.get("current", {})

cond = cur.get("condition", {})

aqi = cur.get("air_quality", {})

days = forecast.get("forecast", {}).get("forecastday", [])

al = alerts.get("alerts", {}).get("alert", [])

  

return {

# Basic location info

"name": loc.get("name"),

"region": loc.get("region"),

"country": loc.get("country"),

  

# Current conditions

"temp_c": cur.get("temp_c"),

"condition": cond.get("text"),

  

# Air quality readings

"air_quality": {k: aqi.get(k) for k in aqi},

  

# Next 3-day forecast

"forecast": [

{

"date": d.get("date"),

"max_temp": d.get("day", {}).get("maxtemp_c"),

"min_temp": d.get("day", {}).get("mintemp_c"),

"condition": d.get("day", {}).get("condition", {}).get("text")

} for d in days

],

  

# Active weather alerts

"alerts": [

{

"headline": x.get("headline"),

"severity": x.get("severity"),

"description": x.get("desc")

} for x in al

]

}

  

# ──────────────────────────────────────────────────────────────────────────────

# 8) Fetch + flatten all weather data

# ──────────────────────────────────────────────────────────────────────────────

def  fetch_weather_data():

base_url =  "http://api.weatherapi.com/v1"

location =  "Kolkata"  # Change as needed

curr =  get_current_weather(base_url, weather_api_key, location)

forecast=  get_forecast_weather(base_url, weather_api_key, location, days=3)

alrt =  get_alerts(base_url, weather_api_key, location)

return  flatten_data(curr, forecast, alrt)

  

# ──────────────────────────────────────────────────────────────────────────────

# 9) Throttle control: track when last event was sent

# ──────────────────────────────────────────────────────────────────────────────

last_sent_time = datetime.now() -  timedelta(seconds=30) # Ensure immediate first send

#Suppose you run the code at 1:00 a.m, then the value for the variable would be 12:59:30 a.m.

  

# ──────────────────────────────────────────────────────────────────────────────

# 10) Batch processor invoked by Spark Structured Streaming

# ──────────────────────────────────────────────────────────────────────────────

def  process_batch(batch_df, batch_id):

"""

process_batch(batch_df, batch_id)

Purpose: Every time Spark’s streaming “heartbeat” fires (once per batch), this function runs—and only actually sends weather data if at least 30 seconds have passed since the last send.

  

batch_df

A mini-DataFrame of whatever rows Spark just received. In our test we’re using the built-in “rate” source (one dummy row per second). We don’t use its contents—we just need it to trigger our function.

  

batch_id

A simple counter (0, 1, 2, …) Spark assigns to each batch. Handy for logging or troubleshooting if something goes wrong.

"""

  

global last_sent_time

#why global variable? so that it can be used in another function (in this case process batch)

now = datetime.now() # Current timestamp

elapsed = (now - last_sent_time).total_seconds() # Seconds since last send

  

if elapsed >=  30:

"""

We check how many seconds have passed since our last successful send. If it’s 30 seconds or more, we proceed; otherwise we skip this batch.

"""

try:

data =  fetch_weather_data() # Retrieve latest weather snapshot

send_event(data) # Publish to Event Hub

"""

Trigger: Whenever Spark adds a new row to streaming_df, it bundles that row into the micro-batch and calls this function.

  

We ignore batch_df’s contents; instead we treat its arrival as a signal to call the weather API.

"""

  

last_sent_time = now # Update throttle timestamp

print(f"Event sent at {now.isoformat()}")

except  Exception  as e:

print(f"Error in batch {batch_id}: {e}")

raise

  

# ──────────────────────────────────────────────────────────────────────────────

# 11) Define a dummy streaming source to trigger batches

# ──────────────────────────────────────────────────────────────────────────────

streaming_df = (

spark.readStream

.format("rate") # Generates rows at a fixed rate

.option("rowsPerSecond", 1) # One row per second

.load()

)

"""

Rate source: emits exactly one fake row per second.

Since weather API is not a streaming datasource, we cannot directly stream data from it, its just an API. We are using rate to emit it as a streaming source.

Using rate means no external dependency; we generate “rows” purely to drive our logic.

"""

  

# ──────────────────────────────────────────────────────────────────────────────

# 12) Attach processor to the stream and start

# ──────────────────────────────────────────────────────────────────────────────

query = (

streaming_df.writeStream

.foreachBatch(process_batch) # Call our function each batch

.start()

)

"""

writeStream: begins defining how to output the stream.

.foreachBatch(process_batch): tells Spark “for each micro-batch, call process_batch(batch_df, batch_id).”

.start(): kicks off the continuous streaming job.

"""

  

# Await termination (runs until manually stopped)

query.awaitTermination()

  

# ──────────────────────────────────────────────────────────────────────────────

# 13) Cleanup: close the Event Hub producer when done

# ──────────────────────────────────────────────────────────────────────────────

producer.close()
```
### Summary of Flow

#### 1. Library Imports and Utilities

-   Azure Event Hub client is imported to allow our notebook to batch and send messages.  
      
    
-   JSON utilities enable conversion between Python dictionaries and JSON strings.  
      
    
-   HTTP request library is brought in for calling the Weather API endpoints.  
      
    
-   Date/time classes are used to implement a simple “throttle” so that we only send updates every 30 seconds.  
      


#### 2. Securely Loading Credentials

-   Two secrets are fetched from Azure Key Vault (via a Databricks secret scope):  
      
    

1.  Event Hub connection string, which contains the endpoint and access key for publishing.  
      
    
2.  Weather API key, which authenticates each REST call to the weather data provider.  
 
 This approach keeps all sensitive values out of the notebook and under central management.  

#### 3. Creating the Event Hub Producer

-   A single long-lived producer client is initialized once.  
      
    
-   Reusing this client for every batch minimizes overhead and ensures efficient, batched network traffic to Event Hub.  


#### 4. Sending Events Helper

-   A helper routine takes any Python dictionary, serializes it to JSON, wraps it into an Event Hub message, and publishes it.  
      
    
-   Internally it uses the producer’s batch API, which automatically handles grouping messages for higher throughput and retry semantics.  
      

#### 5. Unified API Response Handling

-   Rather than duplicating error checks everywhere, a small helper inspects HTTP status codes:  
      
-   On success, it returns the parsed JSON payload.  
      
    
-   On failure, it produces a structured error object with status and message.  
      

#### 6. Wrappers for Weather Endpoints

-   Three dedicated routines encapsulate calls to the Weather API’s:  

1.  Current conditions (including air quality)  
      
    
2.  Multi-day forecast  
      
    
3.  Severe-weather alerts  
      
Each uses the unified response handler to simplify downstream logic.  


#### 7. Flattening and Merging Data

-   The three separate JSON structures—current, forecast, alerts—are merged into one flat dictionary.  
      
    
-   Key pieces of information (location, temperature, conditions, AQI readings, forecast summaries, alert headlines) are all pulled into top-level fields, making the final payload easy to consume.  
      


#### 8. Orchestrator Function

-   A single orchestration routine calls the three wrappers in turn, then feeds their outputs into the flattener, returning one consolidated event dictionary.  
      
    
-   This keeps the high-level flow clear: “fetch then flatten.”  
      

#### 9. Throttle Initialization

-   A timestamp variable is initialized to a point 30 seconds in the past, ensuring the very first batch triggers an immediate send.  
      
    
-   After that, every attempt to send checks the elapsed time against a 30-second threshold.  
      

#### 10. Batch Processor Function

-   Spark Structured Streaming calls this function once per micro-batch.  
      
    
-   It receives:  

1.  A tiny DataFrame of rows from the streaming source (unused except as a trigger), and  
      
    
2.  A monotonic batch identifier for logging or checkpoint logic.  
      
    

-   Inside, it:  
      
    

1.  Computes how many seconds have passed since the last successful send.  
      
    
2.  If at least 30 seconds have elapsed, invokes the orchestrator to fetch & flatten weather data, then publishes it to Event Hub.  
      
    
3.  Updates the last-sent timestamp and logs a confirmation.  
      
    
4.  If any error occurs, logs the batch ID and exception before re-raising, so Spark’s built-in retry/checkpointing can handle it.  


#### 11. Dummy “Rate” Streaming Source

-   Because the Weather API is not a continuous streaming source, we use Spark’s built-in rate source to generate one dummy row per second.  
      
    
-   Each new row triggers a micro-batch in the streaming engine, acting like a heartbeat.  
      

#### 12. Hooking Up the Stream

-   We define a streaming write that uses foreachBatch, supplying our batch processor as the callback.  
      
    
-   When the job starts, Spark runs continuously: each second it forms a micro-batch (from the rate source) and calls our processor.  
      

#### 13. Shutdown and Cleanup

-   The stream runs until manually terminated.  
      
    
-   On shutdown, we gracefully close the Event Hub producer to release connections and resources.  
      

### End-to-End Flow Summary

1.  Heartbeat: Spark’s rate source emits a row every second.  
      
    
2.  Batch Trigger: Each row becomes a micro-batch, invoking process_batch.  
      
    
3.  Throttle Check: Inside the batch, we calculate elapsed time since the last send; only proceed if ≥30 seconds.  
      
    
4.  Data Retrieval: We call all three Weather API endpoints, merge their responses, and prepare one payload.  
      
    
5.  Publish: That payload is sent to Azure Event Hub in a batched, reliable fashion.  

Repeat: The loop continues—fetching and sending real-time weather data every 30 seconds—until you stop the streaming job.

### 19. Wrap-Up & Cleanup

You’ve now completed all eight data-injection steps using Azure Databricks:

1.  Event Hub setup  
      
    
2.  Shared access policy & Key Vault secret  
      
    
3.  Cluster provisioning  
      
    
4.  Library install  
      
    
5.  Test-event send  
      
    
6.  API testing & full fetch code  
      
    
7.  One-off Event Hub send  
      
    
8.  Structured streaming every 30 seconds  

This detailed sequence—from reading API docs through final throttled streaming—ensures a robust, maintainable, production-ready real-time ingestion pipeline.


### 20. Open and Inspect the Function App

1.  In the RG-weather-streaming resource group, open the existing Function App (nothing inside yet).  

2. On its Overview page, click Functions.

### 21. Choosing the Development Approach

1.  Three creation methods are offered:  
-   Azure Portal (in-browser)  

-   VS Code (Desktop)  

-   Other editors/CLI  
      
2. VS Code is selected for full flexibility, local editing, and its Azure integration.


### 22. Set Up VS Code for Azure Functions

1.  Install VS Code if not already present (from code.visualstudio.com).  

2.  In VS Code’s Extensions pane, install:  

-   Azure Functions (Microsoft-provided)  

-   Python (Microsoft-provided)  
 
3.  After installing Azure Functions, an Azure icon appears in the activity bar.


### 23. Authenticate VS Code to Your Azure Subscription

1.  In the Azure panel, click Sign in to Azure → allow the browser-based login.  

2.  Choose your subscription account.  
      
3. On success, your subscription and all its resources (including the Function App) become visible under Resources.


### 24. Create a New Function Project

1. In the Workspace pane of the Azure Functions extension, click Create Function Project.
![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/14.PNG?raw=true)
2.  Select a local folder (e.g., weather-streaming-function-app) to host your project files.  
      
3.  Choose Python as the language.  
    
4.  Pick Azure Functions v2 (recommended programming model).   
    
5.  Skip creating a local virtual environment (testing will occur in Azure).  
      
6.  Select a trigger: choose Timer Trigger for scheduling executions.  
    
7.  Name the function (e.g., weatherApiFunction).  

8. Enter a CRON expression (*/30 * * * * *) to fire every 30 seconds. This sets by which frequency the function will be triggered.


### 25. Explore Generated Project Files

1. function_app.py: contains the timer-trigger template where your logic will reside.

```python
import  logging

import azure.functions as  func

  

app  =  func.FunctionApp()

  

@app.timer_trigger(schedule="*/30 * * * * *", arg_name="myTimer", run_on_startup=False,

use_monitor=False)

def  weatherApiFunction(myTimer: func.TimerRequest) -> None:

if  myTimer.past_due:

logging.info('The timer is past due!')

  

logging.info('Python timer trigger function executed.')
```
2. requirements.txt: initially lists azure-functions; add any extra packages here to ensure they’re installed in Azure.


### 26. Reuse Databricks Data-Injection Code

1.  In your Azure Databricks workspace, locate the notebook section “Sending the complete weather data to the Event Hub”.  
      
    
2.  Copy that code block (which fetches weather, flattens it, and calls send_event).  
  
3. Paste it into function_app.py, nesting it inside your weatherApiFunction so it runs on each timer trigger.


### 27. Update Imports and Dependencies

1.  Move all import requests, import json, and import azure-eventhub lines to the top of function_app.py.  

2. Add azure-eventhub to requirements.txt so Azure installs the Event Hubs SDK.


### 28. For connecting Func with Azure to run the code, Switch to Managed Identity for Event Hub Authentication

1.  Remove any code that fetched an Event Hub connection string (secret scope) from Key Vault/Databricks.  
      
    
2.  In databricks there is no direct way to access the event hubs via Access control so I used shared access policy and created a secret inside key vault to store the primary key.

![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/15.PNG?raw=true)

3. But we can have direct access between Event hub and Azure function. In the Event Hub’s Access Control (IAM), assign the Azure Event Hubs Data Sender role to the Function App’s managed identity. But we won’t be able to see the same so we need to first enable managed identity of the Func App.
![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/16.PNG?raw=true)
4. In the Azure Portal, enable the System-assigned managed identity on the Function App (Identity → System assigned → On → Save).
![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/17.PNG?raw=true)
5. In the Event Hub’s Access Control (IAM), assign the Azure Event Hubs Data Sender role to the Function App’s managed identity. Choose Managed identity.

6.  In your code which you copied from databricks (Where it fetches single latest weather details), replace connection-string-based producer creation with one that uses DefaultAzureCredential (imports from azure.identity) plus the Event Hub namespace and name.

The namespace name will actually be the hostname:
![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/18.PNG?raw=true)

7. Add azure-identity to requirements.txt.


### 29. Switch to Managed Identity for Key Vault Access

1.  We need to connect the function with the get weather API (previously it was connected to key from the key vault) and retrieve weather data from it via direct IAM connection.  

2.  In Azure Portal, go to the Key Vault → Access Control (IAM) → Add role assignment → choose Key Vault Secrets User → assign to the Function App’s managed identity.  

3.  In your code, remove dbutils.secrets references.  

4.  Import and use DefaultAzureCredential with SecretClient (from azure-keyvault-secrets) to fetch the Weather API key at runtime.  
      
5. Add azure-keyvault-secrets to requirements.txt.


### 30. Deploy the Function to Azure

1. In VS Code’s Azure Functions Workspace pane, click Deploy to Function App.
2.  Select your Function App (FP-weather-streaming) in RG-weather-streaming.   
    
3.  Confirm overwriting any existing content.  
      
    
4.  Wait for deployment to complete.


### 31. Validate End-to-End

1.  In the Event Hub’s Data Explorer, click View events → observe new events~every 30 seconds containing fresh weather data for Chennai (or chosen city).  
      
    
2.  Back in the Function App’s Functions blade, confirm that weatherApiFunction is active with a Timer Trigger.  
      
3. Review the App Files section to see function_app.py and requirements.txt in the cloud.

#### Final Code:
```python
import logging

import azure.functions as func

  

# Import libraries for HTTP requests, JSON handling, and Event Hub communication

import requests

import json

from azure.eventhub import EventHubProducerClient, EventData

from azure.identity import DefaultAzureCredential

from azure.keyvault.secrets import SecretClient

  

  

app = func.FunctionApp()

  

@app.timer_trigger(schedule="*/30 * * * * *", arg_name="myTimer", run_on_startup=False,

use_monitor=False)

def  weatherApiFunction(myTimer: func.TimerRequest) -> None:

if myTimer.past_due:

logging.info('The timer is past due!')

  

logging.info('Python timer trigger function executed.')

  

  

  

# ────────────────────────────────────────────────────────────────

# 1. Configure Event Hub connection

# ────────────────────────────────────────────────────────────────

  

# Define the Event Hub name where the event will be published

EVENT_HUB_NAME =  "weather-streaming-event-hub"

  

# Defining Event hub namespace, Event Hub->Select Namespace->Hostname

EVENT_HUB_NAMESPACE =  "EventH-weather-streaming-namespace.servicebus.windows.net"

  

# Uses Managed Identity of Function App

credential = DefaultAzureCredential()

  

# Initialize the Event Hub producer

producer = EventHubProducerClient(

fully_qualified_namespace=EVENT_HUB_NAMESPACE,

eventhub_name=EVENT_HUB_NAME,

credential=credential

)

  

# ────────────────────────────────────────────────────────────────

# 2. Function to send an event (JSON) to Azure Event Hub

# ────────────────────────────────────────────────────────────────

  

def  send_event(event):

# Create a new batch to group events before sending

event_data_batch = producer.create_batch()

# Convert the event dictionary into a JSON string and wrap it in EventData

event_data_batch.add(EventData(json.dumps(event)))

# Send the batch to the Event Hub

producer.send_batch(event_data_batch)

  

# ────────────────────────────────────────────────────────────────

# 3. Helper function to handle HTTP response

# ────────────────────────────────────────────────────────────────

  

def  handle_response(response):

if response.status_code ==  200:

# Return parsed JSON if request was successful

return response.json()

else:

# Return an error message if request failed

return  f"Error: {response.status_code}, {response.text}"

  

# ────────────────────────────────────────────────────────────────

# 4. Functions to retrieve weather data from the API

# ────────────────────────────────────────────────────────────────

  

def  get_current_weather(base_url, api_key, location):

# Call the current weather API including air quality info

current_weather_url =  f"{base_url}/current.json"

params = {'key': api_key, 'q': location, "aqi": 'yes'}

response = requests.get(current_weather_url, params=params)

return handle_response(response)

  

def  get_forecast_weather(base_url, api_key, location, days):

# Call the forecast API for specified number of days

forecast_url =  f"{base_url}/forecast.json"

params = {"key": api_key, "q": location, "days": days}

response = requests.get(forecast_url, params=params)

return handle_response(response)

  

def  get_alerts(base_url, api_key, location):

# Call the alerts API to check for severe weather

alerts_url =  f"{base_url}/alerts.json"

params = {'key': api_key, 'q': location, "alerts": 'yes'}

response = requests.get(alerts_url, params=params)

return handle_response(response)

  

# ────────────────────────────────────────────────────────────────

# 5. Merge and flatten all API responses into a single dict

# ────────────────────────────────────────────────────────────────

  

def  flatten_data(current_weather, forecast_weather, alerts):

# Safely extract sections from nested API responses

location_data = current_weather.get("location", {})

current = current_weather.get("current", {})

condition = current.get("condition", {})

air_quality = current.get("air_quality", {})

forecast = forecast_weather.get("forecast", {}).get("forecastday", [])

alert_list = alerts.get("alerts", {}).get("alert", [])

  

# Create a clean, structured event record

flattened_data = {

# Location metadata

'name': location_data.get('name'),

'region': location_data.get('region'),

'country': location_data.get('country'),

'lat': location_data.get('lat'),

'lon': location_data.get('lon'),

'localtime': location_data.get('localtime'),

  

# Current weather conditions

'temp_c': current.get('temp_c'),

'is_day': current.get('is_day'),

'condition_text': condition.get('text'),

'condition_icon': condition.get('icon'),

'wind_kph': current.get('wind_kph'),

'wind_degree': current.get('wind_degree'),

'wind_dir': current.get('wind_dir'),

'pressure_in': current.get('pressure_in'),

'precip_in': current.get('precip_in'),

'humidity': current.get('humidity'),

'cloud': current.get('cloud'),

'feelslike_c': current.get('feelslike_c'),

'uv': current.get('uv'),

  

# Air quality measurements

'air_quality': {

'co': air_quality.get('co'),

'no2': air_quality.get('no2'),

'o3': air_quality.get('o3'),

'so2': air_quality.get('so2'),

'pm2_5': air_quality.get('pm2_5'),

'pm10': air_quality.get('pm10'),

'us-epa-index': air_quality.get('us-epa-index'),

'gb-defra-index': air_quality.get('gb-defra-index')

},

  

# Weather alerts

'alerts': [

{

'headline': alert.get('headline'),

'severity': alert.get('severity'),

'description': alert.get('desc'),

'instruction': alert.get('instruction')

}

for alert in alert_list

],

  

# 3-day forecast

'forecast': [

{

'date': day.get('date'),

'maxtemp_c': day.get('day', {}).get('maxtemp_c'),

'mintemp_c': day.get('day', {}).get('mintemp_c'),

'condition': day.get('day', {}).get('condition', {}).get('text')

}

for day in forecast

]

}

  

return flattened_data

# ────────────────────────────────────────────────────────────────

# 6. Fetch secret from Key Vault

# ────────────────────────────────────────────────────────────────

def  get_secret_from_keyvault(vault_url, secret_name):

credential = DefaultAzureCredential()

secret_client = SecretClient(vault_url=vault_url, credential=credential)

retrieved_secret = secret_client.get_secret(secret_name)

return retrieved_secret.value

  

  

# ────────────────────────────────────────────────────────────────

# 8. Main function to coordinate all steps

# ────────────────────────────────────────────────────────────────

  

def  fetch_weather_data():

# Base URL of the Weather API

base_url =  "http://api.weatherapi.com/v1/"

# Desired city

location =  "Kolkata"

# Fetch the API key from Key Vault

VAULT_URL =  "https://key-weather-streaming2.vault.azure.net/"

#key vault -> overview-> Vault URI

API_KEY_SECRET_NAME =  "weather-api-key"

  

weatherapikey = get_secret_from_keyvault(VAULT_URL, API_KEY_SECRET_NAME)

  

# Step 1: Call all three API endpoints

current_weather = get_current_weather(base_url, weatherapikey, location)

forecast_weather = get_forecast_weather(base_url, weatherapikey, location, 3)

alerts = get_alerts(base_url, weatherapikey, location)

  

# Step 2: Merge into a flat event structure

merged_data = flatten_data(current_weather, forecast_weather, alerts)

  

# Step 3: Send to Event Hub

send_event(merged_data)

  

# ────────────────────────────────────────────────────────────────

# 7. Trigger the process

# ────────────────────────────────────────────────────────────────

  

# This line initiates the full process: fetch → merge → send

fetch_weather_data()
```

### 32. Summary

-   You’ve replaced a Databricks-Spark streaming solution with a lightweight Python Function App scheduled every 30 seconds.  
      
-   Managed identities now secure both Event Hub sends and Key Vault reads—no shared keys remain in code.  
      
    
-   The same core logic (weather fetch → flatten → send) is reused, illustrating code portability across Azure compute options.


### 33. Importance of Cost Estimation in Azure Projects

-   Why cost estimation is critical:  
-   Companies make project decisions based on cost-effectiveness.  

-   A good data engineer doesn’t just complete a task—they do it efficiently.  
 
-   Cost planning helps in avoiding unnecessary expenses and ensures optimal use of resources.  

Key takeaway: You must be able to estimate the total Azure cost before beginning any project. This is a skill that develops over time through experience with multiple projects.


## 34. Using the Azure Pricing Calculator

-   Open the [Azure Pricing Calculator](https://azure.microsoft.com/en-us/pricing/calculator/) to estimate costs of resources used in the project.  
      
    

### Example 1: Estimating Azure Databricks Cost

-   Go to the Pricing Calculator and add Azure Databricks.  
      
    
-   Select the following options:  
      
    

-   Region: Australia East  
      
    
-   Workload Type: All-purpose compute  
      
    
-   Tier: Premium (necessary for features like RBAC and Unity Catalog)  
      
    
-   Instance Type: Standard_DS3_v2 (14 GB RAM, 4 cores) — the same cluster used in Databricks.  
      
    

-   Monthly Hours: Set to 730 hours (24x7 for 30 days).  
      
    
-   Pricing model: Pay-As-You-Go (PSU Go), but savings plans (1 year, 3 years) are also available.  
      
    

#### Breakdown:

-   Compute cost: $245/month  
      
    
-   Databricks Units (DBU) cost:  
      
    

-   DBU rate: 0.75  
      
    
-   DBU cost per hour: $0.55  
      
    
-   DBU monthly cost: ~ $300  
      
    

-   Total cost: ~$546.41 USD/month  
      
    

🟠 Conclusion: Even running a small Databricks cluster continuously is expensive.


### 35. Estimating Azure Functions Cost

-   In the Pricing Calculator, add Azure Functions.  
    
-   Choose:  
      
	-   Region: Central India
    
	-   Plan: Consumption tier  
      

#### Key Benefits:

-   Free Grant: 1 million executions per month are completely free.  


-   In this project, the API is triggered every 30 seconds = ~86,400 executions per month → Well below the free limit.  

-   Cost after limit: Only $0.20 per million additional executions.  


🟢 Conclusion: For this use case, Azure Functions cost = $0



### 36. Performance Comparison: Databricks vs Azure Functions

### Use Case:

-   Making API calls every 30 seconds  
      
    
-   Sending small weather data events to Event Hub  
      
    
-   Not processing large datasets or doing complex computations  
      
    

### Why Azure Functions is better for this:

-   It is serverless: No infrastructure to manage.  
      
    
-   Instant compute availability: Microsoft Azure manages the backend, reducing delays.  
      
    
-   Quick execution: Ideal for lightweight, event-driven tasks like API polling and forwarding.  
      
    

### Databricks in this context:

-   Requires setting up and maintaining compute clusters.  
      
    
-   More suited for heavy data transformation, big data processing, and advanced Spark workloads.  
      
    
-   Overkill for simple periodic API calls.  
      
    

📌 Conclusion:

-   Use Azure Functions for lightweight, low-cost event ingestion.  
      
    

Use Azure Databricks only when dealing with large-scale, high-complexity data pipelines.



### 37. Summary Decision

From now on in this project:

-   ✅ Azure Functions will be used for data ingestion.  
      
    
-   ❌ Azure Databricks will not be used any further.  
      
    

🧠 Architectural Insight:  
This is a standard design pattern where:

-   Azure Functions are triggered (e.g., on a timer)  
      
    
-   They call APIs or ingest data  
      
They send the data to Azure Event Hub


## 38. Real Cost Monitoring with Azure Portal

-   Navigate to Cost Analysis in the Azure Portal → Under Cost Management.  
      
    
-   You can view:  
      
    

-   Total monthly cost of all resources  
      
    
-   Daily breakdown  
      
    
-   Resource-wise cost analysis  
      
    

### Example Breakdown:

-   Event Hub: $8 NZD/month — starts billing immediately and requires dedicated compute.  
      
    
-   Databricks: $1 NZD/month — was only used briefly for demos.  
      
    
-   Azure Functions: $0/month — despite being used fully for data ingestion.  
      
    
-   Average daily spend: ~60 cents NZD  
      
    

🔍 Insight: With minimal usage of premium services, the whole project can be run very cheaply.


## 39. Final Thoughts for Data Engineers

-   Cost-conscious design is a core responsibility of a data engineer.  
      
    
-   Always ask: Is this resource necessary for the task?  
    For simple, event-driven workloads, serverless options like Azure Functions are often better.  

-   Use the Azure Pricing Calculator for up-front planning.  

-   Prioritize efficient solutions, not just functional ones.


### 40. Accessing Microsoft Fabric (via Power BI portal)

-   Open app.powerbi.com to access Fabric.  
      
    
-   If you're using the trial version, it includes a temporary Fabric license.  
- Alternatively, if trial is unavailable, you can resume a Fabric compute instance created earlier and attach it to your workspace (as shown in Part 2 of the project).


### 41. Creating a Dedicated Workspace in Power BI

-   Navigate to the Workspaces section.  

-   Create a new workspace (e.g., weather-streaming-fabric-workspace) to organize all related resources.  

- Although you can use the default "My Workspace", creating a dedicated one is best practice for large projects.

### 42. Creating an Eventhouse and KQL Database

-   In Fabric, search for and create an Eventhouse (e.g., weather-eventhouse).  
-   When an Eventhouse is created, a KQL database is automatically created alongside it (with the same name).  
-   You can use this default KQL DB or create another one later.


### 43. Introducing Event Stream in Fabric

-   Event Stream is a feature in Fabric that allows pipelines to ingest streaming data from sources like Event Hub into destinations like KQL databases.  

- Navigate to the Weather Streaming workspace → click New Item → search for Event Stream → create one named weather-eventstream.

### 44. Adding the Source (Event Hub)

-   Click Add Source → choose Connect Data Sources since Event Hub is an Azure service.  
    
-   Click Connect to establish a new connection.  
-   Fill in:  

	-   Event Hub Namespace and Name (copy from Azure Event Hub resource).  
      
    
	-   Authentication Type: Only Shared Access Key is supported here.  
      

#### Creating Shared Access Policy in Event Hub:

-   Go to Event Hub settings → Shared Access Policies.  

-   Create a new policy named for-fabric.  

	-   Give it Listen permission (because Fabric is reading, not writing).

-   Copy:  
	-   Policy name (as access key name)  
	-   Primary key (as shared access key)  
 
- Paste both into the Fabric connection form and click Connect.


### 45. Preview and Validate Source Connection

-   Once connected:  

-   Use the default Consumer Group (default is fine).  

-   Choose JSON as the data format.  

After saving, Fabric shows a live preview of the data received from Event Hub, confirming successful integration.


### 46. Configuring the Destination

-   Click “Transform Events or Add Destination”.  

-   Choose Eventhouse as the destination.  
      
#### Choose Ingestion Mode:
	
	- Select: Event Processing Before Ingestion  

		-   Why? It allows automatic table creation based on incoming data.  
      
		-   If you choose direct ingestion, the table must exist in advance.  
      
    

#### Provide Target Details:

-   Destination name: e.g., weather-target  
    
-   Workspace: Weather Streaming  
      
-   Eventhouse: weather-eventhouse   
    
-   KQL Database: weather-eventhouse  
    
-   Destination Table: Create new (name it weather-table)  
    
-   Data Format: JSON (matches Event Hub input)  
    
-   Enable “Activate ingestion after adding” (enabled by default).  
    
-   Click Save to finalize destination setup.


### 47. Validating Data Flow

-   Once both source and destination are configured:  
      
	-   Click on Source and Target previews to confirm.  
 
	-   Use the Refresh button if needed.  

	-   If data appears in both, the pipeline is working correctly.


### 48. Publish the Event Stream

-   Click Publish in the top-right corner.  

-   This starts the real-time ingestion of weather data from Event Hub → Fabric KQL DB.  

-   Once published:  

	-   The source and target status change to Active.  
      
	- Live data flow begins immediately.


### 49. Run KQL Queries to Inspect Data

-   Go to the KQL Database → locate weatherTable.    
-   Click the three dots beside it → select Query Table → choose “Show any 100 records”.  
- Fabric auto-generates a KQL (Kusto Query Language) script to preview 100 rows.

You can now see real-time weather data flowing into the table
![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/19.PNG?raw=true)

### 50. Real-time Test: Observing Changes

-   You may observe condition changes in the data (e.g., from "Sunny" to "Patchy rain nearby").  
-   This proves the pipeline is handling real-time updates correctly.

### 51. Count the Total Rows Over Time

-   Modify the KQL query:  
-   Replace take 100 with count to get total rows.  
-   Click Run → note the count.  
-   Wait a few seconds → run again → count increases.  
	-   Example: from 14 to 15 rows = data ingestion is active and real-time.

### 52: Creating a Real-Time Report Using Fabric Power BI 

#### Steps to Create Report:

1.  Open Query Set and run a query that returns weather data from the table.
![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/20.PNG?raw=true)
2.  Open Power BI Desktop and Sign in with your Fabric account.      
3.  Go to Home → Get Data → More → Microsoft Fabric → KQL Database.  

3.  Enter:  

-   Cluster URI (from Fabric → Query URI)  

-   Database name (e.g., weather-eventhouse)  
- Table name (e.g., weatherTable)

### Sample Report Built in Power BI Desktop

#### Visuals Added:

-   Local time card  
      
    
-   Critical weather alert  
      
    
-   Air quality band (calculated using DAX)  
      
    
-   Weather condition icon  
      
    
-   Temperature, feels-like, humidity, pressure, UV, wind  
      
    
-   3-day forecast visual (includes dates, min/max temp, condition)  
- Recent trends line chart

![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/21.PNG?raw=true)

#### Special Features:
-   DAX was used to convert air quality index (GB DEFRA Index) to human-readable bands (e.g., High, Moderate).
- The UK DEFRA Index logic was taken from the Weather API documentation.

### 53: Manual Updates & Publishing Desktop Report

#### Notes on Refresh:

-   Import mode does not support real-time refresh.     
-   You must click "Refresh" manually to pull new data.  

#### Publish to Fabric:
1.  Save the report.  
2.  Click Publish and select the workspace (e.g., weather-streaming).  


### 54: Schedule Refresh for Desktop Report

1.  Go to Workspace → Semantic Model → Settings → Refresh.  
 
2.  Enable Scheduled Refresh.  

3.  Choose Daily or Weekly, and set time(s) (up to 48/day with premium).  
      
4. Alternatively, click “Refresh Now” to fetch latest data manually.


### 55. Verifying the Ingestion Pipeline

-   The weather data is being successfully ingested into Event Hub.  
-   The “alerts” column in the JSON payload from the API is part of the data received.
![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/22.PNG?raw=true)

### 56. Creating a New KQL Query Set for Alerts

-   Go to Microsoft Fabric → Weather Streaming workspace.  

-   Navigate to the KQL database (created earlier with the Eventhouse).  

-   Click New related item → KQL Query Set.  

-   Name it for-alerts.  

-   Rationale: Don’t modify the original query set used for the Power BI report, as it may affect visualizations.



### 57 Writing the Alert Query Logic

#### Initial Steps

-   The query filters for records where the alerts field is not empty (i.e., not []).  
      
    
-   Uses a basic where condition in KQL.

![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/23.PNG?raw=true)

```kql
// Query to find alert values that were last triggered more than 1 minute ago

  

["weather-table"]

|  where  alerts  !=  '[]'  // Only consider rows that have at least one alert

|  extend  AlertValue  =  tostring(alerts)  // Convert the alerts array into a single string

|  summarize  LastTriggered  =  max(EventProcessedUtcTime)  by  AlertValue

|  join  kind=leftanti  (

["weather-table"]

|  where  alerts  !=  '[]'  // Again, only consider rows with alerts

|  extend  AlertValue  =  tostring(alerts)  // Convert the alerts array into a string

|  summarize  LastTriggered  =  max(EventProcessedUtcTime)  by  AlertValue

|  where  LastTriggered  <  ago(5m)  // Keep only alerts whose last occurrence was over 1 minute ago

)  on  AlertValue
```

#### Logic (Explained)



-   Uses extend and tostring to extract and clean the alert message from the array.  
    
    
-   Also calculates the most recent alert time using summarize max(eventProcessedUtcTime).  
      
    
-   Output columns:  
      
    

	-   alert_value → The actual alert message  
      
    

	- last_triggered → The latest time the alert was received

#### SQL Version:

```sql  
CTE LatestAlerts  
  
WITH LatestAlerts AS (

SELECT

alerts AS AlertValue,

MAX(EventProcessedUtcTime) AS LastTriggered

FROM weather_table

WHERE alerts <> '[]'

GROUP BY alerts

)
```

-   We start by pulling every row in weather_table where alerts is not the literal string '[]'.  
      
    
-   We group by the raw alerts JSON (here renamed AlertValue), and use MAX(EventProcessedUtcTime) to determine the most recent timestamp at which each distinct alert was seen.  
      
    

- As a result, LatestAlerts has one row per unique alert‐string, with the column LastTriggered showing when that alert last appeared.

CTE OldAlerts  
```sql

  
  
OldAlerts AS (

SELECT

AlertValue,

LastTriggered

FROM LatestAlerts

WHERE LastTriggered < DATEADD(MINUTE, -5, SYSUTCDATETIME())

)
```
-   From LatestAlerts, we keep only those entries whose LastTriggered is more than 5 minute ago, compared to the current UTC timestamp (SYSUTCDATETIME()).  
      
- In effect, OldAlerts lists alert values that were last seen before the 1-minute cutoff.

Final Selection (Left Anti-Join Logic)
```sql
SELECT

la.AlertValue,

la.LastTriggered

FROM LatestAlerts AS la

LEFT JOIN OldAlerts AS oa

ON la.AlertValue = oa.AlertValue

WHERE oa.AlertValue IS NULL;
```
-   We take every row from LatestAlerts and do a LEFT JOIN to OldAlerts on AlertValue.  

-   If an AlertValue appears in OldAlerts, that means its last occurrence was older than one minute.  
      

-   By filtering WHERE oa.AlertValue IS NULL, we keep only those alerts not found in OldAlerts, i.e. those whose LastTriggered is within the last 5 minute.  
      
- Conceptually, this mirrors KQL’s join kind=leftanti, which excludes all keys that appear in the right‐side dataset.


### 58 Avoiding Duplicate Email Alerts

#### Problem:

Without additional logic, Fabric would email the same alert every 5 minutes (its default query frequency), flooding the inbox.

#### Solution:

Introduce a filter that only triggers alerts received within the last 5 minutes:

```kql
|  join  kind=leftanti  (

["weather-table"]

|  where  alerts  !=  '[]'  // Again, only consider rows with alerts

|  extend  AlertValue  =  tostring(alerts)  // Convert the alerts array into a string

|  summarize  LastTriggered  =  max(EventProcessedUtcTime)  by  AlertValue

|  where  LastTriggered  <  ago(5m)  // Keep only alerts whose last occurrence was over 1 minute ago

)  on  AlertValue
```

Since Fabric re-runs the alert query every minute, this ensures the same alert isn’t picked up repeatedly.


### 59 Configuring the Data Activator Alert
#### Steps:

1.  Click Set Alert on the query set page.  

2.  Set “Run Query Every” to 5 minutes (minimum supported frequency).  

3.  Condition: Choose “On each event”.  

	-   Every time the query returns rows, an alert is fired.  
      
4.  Action: Choose Email.  

	-   The default email recipient is the logged-in Fabric user.  

5.  Save Location: Choose the Weather Streaming workspace.  

6.  Create a new Data Activator item:  
      
	- Name it weather-alerts.


### 60 Customizing the Alert Email

-   After creating the Data Activator item:  

-   Set the Email Subject: e.g., “New Weather Warning”.  

-   Set the Headline: e.g., “Take Action Soon”.  
 
-   Save changes.  

	- Status changes to “Running”, meaning the query runs every minute to check for new alerts.



## Final Testing
-   I manually sent another test event with alerts = ["Test Alert Here"].
![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/24.PNG?raw=true)
-   Wait for 5 minutes.  
      
- Verifies that the event was detected by the Data Activator.
![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/25.PNG?raw=true)
- Checks email:  The alert email was received successfully, with correct subject and headline.
![](https://github.com/Kens3i/Weather-Streaming-Data-Engineering-Project/blob/main/Images/26.PNG?raw=true)

### Key Highlights

-   KQL Query Filtering: Crucial to detecting new alerts and preventing duplicate alerts.   
    
-   Data Activator: A powerful low-code/no-code tool for triggering actions based on real-time conditions in Fabric.  
    
-   Fully Automated: Once set up, the system:  
  
	-   Ingests real-time weather data,  

	-   Detects severe alerts,
	-  Sends real-time notifications (emails) to the user—automatically.
---

## FAQ

### 1. Why choose Azure Functions over Azure Databricks for continuous polling of a REST API?

Answer:

-   Cost Efficiency: Azure Functions on a Consumption Plan can run small tasks (e.g., calling a weather API every 30 seconds) at minimal cost. The first 1 million executions per month are free; subsequent executions are billed at a very low rate.  
      
    
-   Simplicity & Maintenance: You don’t manage infrastructure; Azure handles scaling and high availability. You simply deploy your timer-triggered function.  
      
    
-   Appropriate for Light Workloads: Polling a lightweight API and forwarding results to Event Hub is not a heavy data transformation job, so serverless compute is a perfect fit.  
      
    
-   When to Use Databricks Instead: If you need to process large batches of data (terabytes), complex Spark transformations or machine learning pipelines, Databricks (with a continuously running cluster) may be necessary despite higher cost.
    

  
  

### 2. How is cost estimated for Azure Databricks vs. Azure Functions?

Answer:

-   Databricks: You estimate VM hours (cluster up 24×7 for streaming). Include both “compute” cost (VM pricing) and “DBU” cost (Databricks Unit consumption). For a small cluster running 730 hours/month, costs can sum to several hundred dollars monthly.  
      
    
-   Functions (Consumption Plan): You estimate number of executions × execution duration × memory consumption. With a timer trigger every 30 seconds (~2 million runs/month if running consistently) but free grant covers 1M executions; usage beyond that is very cheap. In practice for one call per 30s (~2.6M runs/month), costs remain minimal.  
      
    
-   Key Takeaway: For lightweight, frequent tasks, Functions are vastly more cost-effective. Always use the Azure Pricing Calculator and monitor actual usage in Cost Analysis.
    

  
  

### 3. How do I securely store and retrieve secrets (e.g., API keys, Event Hub connection strings) in Databricks and Functions?

Answer:

-   Azure Key Vault: Store secrets (weather API key, Event Hub connection string) in Key Vault.  
      
    
-   Databricks: Create a secret scope backed by Key Vault. In notebooks, call dbutils.secrets.get(scope, key) to fetch values at runtime. Ensure Databricks service principal or MSI has the “Key Vault Secrets User” role.  
      
    
-   Azure Functions: Use Managed Identity (DefaultAzureCredential) to access Key Vault at runtime. Grant the Function’s identity appropriate Key Vault access policy (e.g., Secret Reader). In code, use SecretClient or App Settings integration to fetch secrets.  
      
    
-   Principle: Never hard-code sensitive values. Always grant least-privilege access to Key Vault.
    

  
  

### 4. How do I send events to Azure Event Hub from code?

Answer:

-   Initialize Producer: Use EventHubProducerClient.from_connection_string(...) or with managed identity: EventHubProducerClient(fully_qualified_namespace=..., eventhub_name=..., credential=DefaultAzureCredential()).  
      
    
-   Batching: Create a batch via producer.create_batch(), wrap your event payload (a Python dict) as a JSON string, then EventData(json.dumps(event_dict)), add it to the batch, and call producer.send_batch(batch).  
      
    
-   Cleanup: After sending, close the producer when the application or notebook completes (producer.close()).  
      
    
-   Test: Use Event Hub Data Explorer in the Azure Portal to confirm the arrival of messages.
    

  
  

### 5. How does Structured Streaming with a “rate” source work for driving periodic API calls?

Answer:

-   Rate Source: Spark’s built-in “rate” source emits a dummy row at a specified rate (e.g., 1 row/sec). Each micro-batch of the streaming DataFrame contains the rows arrived since last batch.  
      
    
-   foreachBatch Trigger: You attach .writeStream.foreachBatch(process_batch).start(). For each micro-batch, Spark invokes process_batch(batch_df, batch_id). You can ignore batch_df contents and use the invocation itself as a timer trigger to call your API and send to Event Hub.  
      
    
-   Throttling: Inside process_batch, track a global last_sent_time. Only fetch & send if a configured interval (e.g., ≥30 seconds) has elapsed. This prevents unnecessary API calls if micro-batches arrive more frequently.  
      
    
-   Cluster Uptime: The cluster must run continuously (auto-termination off) to keep the streaming job alive.
    

  

### 6. What is the purpose of the last_sent_time global variable in streaming code?

Answer:

-   It implements a simple throttle to ensure the API is called only once per desired interval (e.g., every 30 seconds), even though Spark may trigger micro-batches more frequently.  
      
    
-   On each process_batch call:  
      
    

1.  Compute elapsed = now - last_sent_time.  
      
    
2.  If elapsed >= interval, call the weather API, send event, update last_sent_time.  
      
    
3.  Otherwise, skip sending.  
      
    

-   This avoids redundant API calls and respects rate limits or API usage patterns.
    

  
  

### 7. How is data ingested from Event Hub into Microsoft Fabric (Kusto) in real time?

Answer:

-   Fabric Event Stream: In Fabric, use the Event Stream feature (pipeline) to connect to an external Event Hub:  
      
    

1.  Create or select an Event Hub connection: specify namespace, hub name, and authentication (e.g., Shared Access Key with Listen permission).  
      
    
2.  Verify preview: ensure JSON messages arrive.  
      
    
3.  Configure target as “Kusto (KQL) database” ingestion, choosing “Event processing before injection” to auto-create the destination table.  
      
    
4.  Publish the pipeline: it runs 24×7, continuously pulling new events and loading them into the Kusto table.  
      
    

-   Destination Table: Automatically created with schema inferred from JSON fields. Incoming events appear in near real time.
    

  

### 8. What is a Kusto (KQL) “query set” and how is it used?

Answer:

-   A query set in Fabric’s KQL database is a collection of saved queries (analogous to views in SQL) that you can run on one or more tables.  
      
    
-   Purposes:  
      
    

-   Analyze and explore ingested data.  
      
    
-   Serve as the basis for Power BI reports or Data Activator alert logic.  
      
    

-   Creating a view-like query: You write KQL queries in the query set; you can refer to tables, filter, aggregate, etc.  
      
    
-   Integration with Power BI: Fabric can generate direct-query or import-mode reports from a query set output.
    

  

### 9. RBAC – Role-Based Access Control

RBAC (Role-Based Access Control) is a security model used to manage who can access what resources and what actions they can perform.

#### How It Works:

-   You assign roles to users, groups, or service principals.  
      
    
-   Each role contains permissions that define what actions can be performed.  
      
    
-   Permissions are scoped to resource levels (e.g., subscription, resource group, or individual resource).  
      
    

#### Example:

-   Alice is assigned the Reader role on the Weather-KustoDB. She can view data but cannot modify or delete it.  
      
    
-   A Function App is assigned the Key Vault Secrets User role to read secrets from Azure Key Vault.  
      
    

#### Common Azure Roles:

-   Reader – view-only access  
      
    
-   Contributor – full control except RBAC assignments  
      
    
-   Owner – full control including RBAC  
      
    
-   Key Vault Secrets User – read secrets from Azure Key Vault  
      
    

#### Why It's Important:

-   Secure and least-privilege access.  
      
    
-   Centralized and auditable permission model.  
      
    
-   Works across all Azure services and integrates with Azure AD.  
      
    

----------

### 10 Unity Catalog Networking (in Databricks)

Unity Catalog is Databricks’ unified data governance solution for managing access to tables, views, volumes, functions, and files across all workspaces.

When talking about Unity Catalog Networking, we refer to the networking configurations and access control mechanisms needed to ensure secure access to data governed by Unity Catalog.

####  Key Concepts:

1.  Metastore:  
      
    

-   A central governance layer that stores all schema/catalog/table permissions.  
      
    
-   You typically set up one Unity Catalog metastore per region per account.  
      
    

2.  External Locations & Volumes:  
      
    

-   These are cloud storage locations (e.g., ADLS Gen2, S3) registered in Unity Catalog to allow managed access.  
      
    
-   Requires secure networking, like Private Endpoints and Storage Credentials.  
      
    

3.  Network Isolation:  
      
    

-   Unity Catalog ensures access to cloud storage is secured by allowing access only from specific IP ranges, VNETs, or managed identities.  
      
    
-   You often use Managed Identities or Service Principals to read/write to these storage locations securely.  
      
    

4.  Credential Passthrough / Token-Based Access:  
      
    

-   Unity Catalog can enforce access based on the end user’s identity, ensuring that users only access data they’re allowed to see—even when querying shared storage.
    

  
  
  

### 11. How do I configure real-time reports in Power BI off a Kusto table?

Answer:

-   Online (Fabric) Version (Direct Query):  
      
    

-   In Fabric’s query set, select “Create Power BI report.” This opens the online Power BI editor connected via Direct Query.  
      
    
-   Build visuals; then configure Auto Page Refresh or Change Detection under Format → Page refresh. You can set refresh intervals (e.g., 5s, 30s) so visuals update automatically as new data arrives.  
      
    
-   Save the report in the workspace: it remains connected to the Kusto table in real time.  
      
    

-   Power BI Desktop (Import Mode):  
      
    

-   Connect to Fabric Kusto database by specifying cluster URI, database name, table name.  
      
    
-   Choose Import Mode if you need to transform or enrich data in Power BI. Data is loaded into the PBIX file; subsequent refreshes require manual or scheduled refresh to pull new data.  
      
    
-   Publish the report back to Fabric workspace. You can configure Scheduled Refresh (daily/hourly) in Fabric to update imported data.  
      
    

-   Trade-offs:  
      
    

-   Direct Query / Online version: real-time data, no offline transformations, limited modeling flexibility.  
      
    
-   Import Mode: richer modeling and transformations but not real-time; needs refresh to get new data.
    

  
  

### 12. How to set up alerts on incoming data in Fabric?

Answer:

-   Data Activator: Fabric’s service for scheduling queries (every minute minimum) and triggering actions (email, Teams, invoke other pipelines) when conditions are met.  
      
    
-   Steps:  
      
    

1.  Create a new KQL query set specifically for alerts (do not overwrite the query set used by reports).  
      
    
2.  Write a KQL query to filter rows where the alerts column is non-empty, extract the alert value, and compute the timestamp of the latest occurrence.  
      
    
3.  Add logic to suppress duplicate notifications: compare LastTriggered timestamp to ago(1m) so that only alerts received in the last minute fire.  
      
    
4.  In the query set, click “Set alert,” choose execution frequency (e.g., every 1 minute), condition (“on each event” means if query returns any rows), and action (Email).  
      
    
5.  Define or create a Data Activator item; configure recipient email, subject, and message.  
      
    
6.  Save/publish the alert. Fabric will run the query periodically; when it returns rows, an email is sent.  
      
    

-   Deduplication Logic: Ensures the same alert value doesn’t trigger repeated notifications indefinitely.
    

  

### 13. How do I test the end-to-end pipeline?

Answer:

1.  Stop the data injection (e.g., stop the Function App) so the pipeline is idle.  
      
    
2.  Observe current state: open the real-time Power BI report to see a stable row count.  
      
    
3.  Start the Function App: it begins polling the weather API and sending events to Event Hub.  
      
    
4.  Monitor Event Hub: in Azure Portal’s Data Explorer, confirm new events arriving.  
      
    
5.  Monitor Fabric Event Stream pipeline: ensure it shows “active” and preview shows JSON records.  
      
    
6.  Check Kusto table: query row count in KQL; it should start increasing.  
      
    
7.  Refresh real-time report: row count and visuals update automatically.  
      
    
8.  Send test alert: manually send an event with an alerts value to Event Hub; verify Data Activator email arrives.  
      
    
9.  Observe logs/metrics: Check Function logs, Spark job metrics (if using Databricks), and Data Activator live feed for errors or confirmation.  
      
    
10.  Cost and performance: review Azure Cost Analysis for actual resource consumption and adjust as needed.
    

  
  

### 14. What are best practices for naming and organizing resources and code?

Answer:

-   Resource Naming: Use consistent prefixes/suffixes to indicate project and purpose (e.g., rg-weather-streaming, funcApp-weather-streaming, EventH-weather-streaming-namespace, Key-weather-streaming2).  
      
    
-   Secret Naming: Use descriptive names (weather-api-key, eventhub-connection-string-secret).  
      
    
-   Code Organization:  
      
    

-   Group helper functions together (e.g., API callers, response handlers, flatten logic, Event Hub sender).  
      
    
-   Add clear comments explaining each section.  
      
    
-   Use parameterization (e.g., location as a variable) rather than hard-coding values.  
      
    

-   Query Sets: Maintain separate query sets for reporting vs. alerting to avoid unintended impacts.  
      
    
-   Version Control: Keep notebooks or scripts under source control; document dependencies (e.g., Python package versions for Azure Event Hubs library).  
      
    
-   Configuration: Externalize configuration (Key Vault, environment variables) rather than embedding keys or connection strings.
    

  
  

### 15. How do I manage and monitor costs as the pipeline runs continuously?

Answer:

-   Azure Cost Analysis: In the resource group, use Cost Analysis to view daily/monthly spend per resource (Function App, Event Hub, Databricks, Fabric).  
      
    
-   Right-sizing:  
      
    

-   For Databricks: shut down clusters when not needed; choose minimal cluster size if using for batch.  
      
    
-   For Functions: ensure consumption plan is appropriate; monitor execution time and memory.  
      
    
-   For Event Hub: choose required throughput units and retention based on volume.  
      
    

-   Budget Alerts: Configure Azure Budgets to notify when spending approaches thresholds.  
      
    
-   Monitoring: Enable Application Insights or other logging (for moderate cost) to gather metrics; monitor API call success rates and streaming pipeline health.  
      
    
-   Retention & Purging: In Kusto database, configure data retention policies to avoid runaway storage costs; drop or archive old data if no longer needed.  
      
    
-   Optimize Frequency: Poll API at the minimal acceptable frequency (e.g., every 30s), not excessively.
    

  

### 16. How do I handle schema evolution or changes in the incoming JSON structure?

Answer:

-   Event Hub / Kusto:  
      
    

-   When ingesting JSON into Kusto, schema inference may create dynamic columns or nested structures. If the JSON shape changes (new fields), new columns may appear automatically if using dynamic ingest.  
      
    
-   For strongly typed fields, periodically review and update query sets or data mappings.  
      
    

-   Flatten Code: In the Python flatten logic, use .get(...) with default values; adding or removing fields only affects what you extract. If the API adds new fields, update flatten_data to include them as needed.  
      
    
-   Power BI Models: When the underlying table schema changes, ensure visuals/query sets reflect new columns or changed data types. For Import Mode, refresh and adjust model. For Direct Query, visuals adapt automatically if referenced fields exist.  
      
    
-   Alerts: If alert payload structure changes (e.g., different JSON key), update KQL query logic accordingly.
    

  

### 17. How do I ensure reliability and error handling in the Function or Databricks code?

Answer:

-   HTTP Calls: In weather API calls, check response.status_code; if not 200, log error details. Optionally implement retry logic with exponential backoff if transient failures occur.  
      
    
-   Exception Handling: Wrap calls in try/except, log exceptions clearly. In Databricks streaming process_batch, catching exceptions ensures Spark surfaces failures; you can also write errors to logs or a dead-letter queue.  
      
    
-   Resource Cleanup: Ensure Event Hub producer clients are closed when done; in long-running streaming, clients are reused.  
      
    
-   Monitoring & Logging:  
      
    

-   In Functions: use Application Insights or built-in logging to capture invocation success/failure.  
      
    
-   In Databricks: use notebook or cluster logs, and monitor streaming job progress in UI.  
      
    

-   Retries & Idempotency: If a send to Event Hub fails, you may retry; ensure idempotent behavior if necessary (e.g., tagging events with unique IDs).  
      
    
-   Alerting on Failures: You can extend the pipeline to send notifications (e.g., email, Teams) if repeated failures occur when calling the API or sending events.
    

  
  

### 18. How should I manage environment-specific configuration (dev/test/prod)?

Answer:

-   Separate Resource Groups or Subscriptions: Isolate environments to limit cross-impact.  
      
    
-   Parameterize Settings:  
      
    

-   Use environment variables or Azure App Settings in Functions (e.g., API endpoint, location, Kusto database URI).  
      
    
-   In Databricks: use widgets or environment-specific secret scopes.  
      
    
-   In Fabric pipelines: parameterize source/target connections if promoting across workspaces.  
      
    

-   Key Vault: Use separate Key Vault instances per environment, with appropriate secrets stored separately.  
      
    
-   CI/CD: Automate deployment using ARM templates or Terraform for resource provisioning, and pipeline definitions for Fabric/Power BI, ensuring consistent configurations across environments.  
      
    
-   Testing: In lower environments, mock or stub external dependencies (e.g., use a sandbox Event Hub or a dummy API) to validate behavior before production.
    

  
  

### 19. What troubleshooting steps can I follow if I see no data in the final report?

Answer:

1.  Check Data Injection:  
      
    

-   Is the Function App running? Review logs to see if it’s executing on schedule and whether API calls succeed.  
      
    
-   In Event Hub Data Explorer, confirm messages are arriving.  
      
    

2.  Check Fabric Event Stream:  
      
    

-   Is the pipeline active? Does the preview show incoming JSON?  
      
    
-   Any errors in the pipeline logs indicating ingestion failures?  
      
    

3.  Kusto Table:  
      
    

-   In Fabric’s KQL query window, run a simple ["weather-table"] | take 10 to see if records exist.  
      
    
-   Check the row count using | count.  
      
    

4.  Query Set / Report:  
      
    

-   Verify the query set references the correct table and fields.  
      
    
-   In Power BI report (online version), confirm visuals point to correct query set. Refresh preview.  
      
    
-   In Power BI Desktop import scenario, click Refresh to pull latest data; ensure credentials and connection details are correct.  
      
    

5.  Alerts / Other Components:  
      
    

-   If Data Activator isn’t firing, confirm query set logic returns expected rows, and frequency/configuration is correct.  
      
    

6.  Authentication Issues:  
      
    

-   If secrets can’t be retrieved, the Function or Databricks job may silently fail before sending; check Key Vault access policies and managed identity assignments.  
      
    

7.  Network/Firewall:  
      
    

-   If using Private Endpoints or restricted traffic, confirm the service has network access to Key Vault, Event Hub, and other endpoints.  
      
    

8.  Logs and Metrics:  
      
    

-   Review Application Insights (if enabled), Databricks cluster logs, Function invocation logs, Fabric pipeline health metrics. Look for exceptions or throttling.  
      
    

9.  Schema Changes:  
      
    

-   If the flatten logic or ingestion expects fields not present, resulting JSON may fail ingestion; verify the incoming event structure matches expectations.
---


### Thankyou For Spending Your Precious Time Going Through This Project!
### If You Find Any Value In This Project Or Gained Something New Please Do Give A ⭐.


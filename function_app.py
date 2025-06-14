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
def weatherApiFunction(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')


    logging.info('Python timer trigger function executed.')






    # ────────────────────────────────────────────────────────────────
    # 1. Configure Event Hub connection
    # ────────────────────────────────────────────────────────────────


    # Define the Event Hub name where the event will be published
    EVENT_HUB_NAME = "weather-streaming-event-hub"


    # Defining Event hub namespace, Event Hub->Select Namespace->Hostname
    EVENT_HUB_NAMESPACE = "EventH-weather-streaming-namespace.servicebus.windows.net"


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


    def send_event(event):
        # Create a new batch to group events before sending
        event_data_batch = producer.create_batch()
       
        # Convert the event dictionary into a JSON string and wrap it in EventData
        event_data_batch.add(EventData(json.dumps(event)))
       
        # Send the batch to the Event Hub
        producer.send_batch(event_data_batch)


    # ────────────────────────────────────────────────────────────────
    # 3. Helper function to handle HTTP response
    # ────────────────────────────────────────────────────────────────


    def handle_response(response):
        if response.status_code == 200:
            # Return parsed JSON if request was successful
            return response.json()
        else:
            # Return an error message if request failed
            return f"Error: {response.status_code}, {response.text}"


    # ────────────────────────────────────────────────────────────────
    # 4. Functions to retrieve weather data from the API
    # ────────────────────────────────────────────────────────────────


    def get_current_weather(base_url, api_key, location):
        # Call the current weather API including air quality info
        current_weather_url = f"{base_url}/current.json"
        params = {'key': api_key, 'q': location, "aqi": 'yes'}
        response = requests.get(current_weather_url, params=params)
        return handle_response(response)


    def get_forecast_weather(base_url, api_key, location, days):
        # Call the forecast API for specified number of days
        forecast_url = f"{base_url}/forecast.json"
        params = {"key": api_key, "q": location, "days": days}
        response = requests.get(forecast_url, params=params)
        return handle_response(response)


    def get_alerts(base_url, api_key, location):
        # Call the alerts API to check for severe weather
        alerts_url = f"{base_url}/alerts.json"
        params = {'key': api_key, 'q': location, "alerts": 'yes'}
        response = requests.get(alerts_url, params=params)
        return handle_response(response)


    # ────────────────────────────────────────────────────────────────
    # 5. Merge and flatten all API responses into a single dict
    # ────────────────────────────────────────────────────────────────


    def flatten_data(current_weather, forecast_weather, alerts):
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
    def get_secret_from_keyvault(vault_url, secret_name):
        credential = DefaultAzureCredential()
        secret_client = SecretClient(vault_url=vault_url, credential=credential)
        retrieved_secret = secret_client.get_secret(secret_name)
        return retrieved_secret.value




    # ────────────────────────────────────────────────────────────────
    # 8. Main function to coordinate all steps
    # ────────────────────────────────────────────────────────────────


    def fetch_weather_data():
        # Base URL of the Weather API
        base_url = "http://api.weatherapi.com/v1/"
       
        # Desired city
        location = "Kolkata"
       
        # Fetch the API key from Key Vault
        VAULT_URL = "https://key-weather-streaming2.vault.azure.net/"
        #key vault -> overview-> Vault URI
        API_KEY_SECRET_NAME = "weather-api-key"


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
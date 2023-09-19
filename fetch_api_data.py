import requests

def fetch_api_data():
    # Define start_date and end_date
    start = "2020-06-30"
    end = "2023-06-30"

    # Convert start_date and end_date to datetime objects
    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")

    # Create a folder for storing JSON files
    os.makedirs("API_Data", exist_ok=True)

    # Make API calls for each day and store data in JSON files
    current_date = start_date
    while current_date <= end_date:
        current_date_str = current_date.strftime("%Y-%m-%d")
        folder_name = os.path.join("API_Data", current_date_str)
        os.makedirs(folder_name, exist_ok=True)

        # Make the API call here and store the data in a variable
        base_url = "https://api.energidataservice.dk/dataset/ConsumptionDE35Hour"
        params = {
            'start': current_date,
            'end': current_date,
            'limit': 1824
        }
        response = requests.get(base_url,params=params)
        data = response.json()["records"]

        # Save data to a JSON file
        json_filename = os.path.join(folder_name, "data.json")
        with open(json_filename, "w") as json_file:
            json.dump(data, json_file)

        # Move to the next day
        current_date += timedelta(days=1)
        
        # Delay for one day
        time.sleep(86400)  # 86400 seconds in a day (24 hours)

fetch_api_data()

@asset(deps=[fetch_api_data])
def feature_engineering():
    pass
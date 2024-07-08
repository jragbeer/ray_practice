import os
import requests
import pyarrow.parquet as pq
import datetime


today = datetime.datetime.now()
path = os.path.abspath(os.path.dirname(__file__)).replace("\\", "/") + "/"
data_path = path + 'data/'


trip_types = ['yellow', 'green', 'fhv', 'hvfhv',]
years = list(range(2009, 2025))
months = list(range(1,13))

def download_all_nyctaxi_files():
    counter = 0
    for y in years[::-1]:
        for t in trip_types:
            for m in months:
                try:
                    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{t}_tripdata_{y}-{m:02}.parquet"
                    print(url)
                    t = download_parquet_file(url, data_path + f"nyctaxi_{t}_{y}_{m:02}.parquet")
                    if t:
                        counter = counter + 1
                    if counter >= 400:
                        return
                except Exception as ee:
                    print(ee)
                    print("Can't download file")

# @ray.remote
def download_parquet_file(url, output_file):
    # Make a GET request to fetch the file
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Write the contents of the response to the output file
        with open(output_file, 'wb') as f:
            f.write(response.content)

        print(f"Parquet file downloaded successfully as '{output_file}'")

        # Read and return the Parquet file using pyarrow
        table = pq.read_table(output_file)
        return 1
    else:
        # Print an error message if the request was not successful
        print(f"Failed to download Parquet file from '{url}'. Status code: {response.status_code}")
        return 0


download_all_nyctaxi_files()

print(datetime.datetime.now()-today)
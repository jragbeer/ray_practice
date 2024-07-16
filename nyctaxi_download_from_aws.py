from nyctaxi_data_eng import *
import requests
import pyarrow.parquet as pq
from meteostat import Hourly



def hmdxx(temp, dew_temp):
    """

    This function returns the humidex reading given the air temp and dew temp.

    :param temp: air temp in Celsius
    :param dew_temp: dewpoint in Celsius
    :return: humidex reading as a float
    """
    return temp + (
        0.5555
        * (6.11 * np.exp(5417.7530 * ((1 / 273.16) - (1 / (273.15 + dew_temp)))) - 10)
    )



today = datetime.datetime.now()
path = os.path.abspath(os.path.dirname(__file__)).replace("\\", "/") + "/"
data_path = path + 'data/'


trip_types = ['yellow', 'green', 'fhv', 'hvfhv',]
years = list(range(2009, 2025))
months = list(range(1, 13))


# Set time period
start = datetime.datetime(2010, 1, 1)
end = datetime.datetime(2025, 1, 1, 0, 0)

nyc_weather_stations = {
"John F. Kennedy Airport" : "74486",
"New York City / Yorkville" : "KNYC0",
"New York / Wall Street" : "72502",
"Newark Airport" : "KJRB0"
}


# Get hourly data
z = []
for x,y in nyc_weather_stations.items():
    data = Hourly(y, start, end, timezone="GMT",)
    data = data.fetch()
    data['station_name'] = x
    data['station_id'] = y
    data["humidex"] = pd.Series(
        [
            np.round(
                hmdxx(
                    x.temp,
                    x.dwpt,
                ),
                2,
            )
            for x in data.itertuples()
        ],
        index=data.index,
    )
    # find the date range of the demand data, used as the definitive date range throughout analysis
    date_range = pd.date_range(
        start=data.index.min(),
        end=data.index.max(),
        freq="1T",
    )
    datees = pd.DataFrame({"temp_col": np.ones(len(date_range))}, index=date_range)

    merged = pd.merge(datees, data, left_index=True, right_index=True, how="left")
    merged = merged.drop(columns=["temp_col", ])
    merged = merged.fillna(method="ffill")
    merged.index.names = ["time"]

    weather_data = merged.drop(
        columns=["snow", "tsun", "wpgt", "coco",
                 'wdir', 'wspd', 'pres']
    ).dropna(subset=["temp", "dwpt", "rhum"]).reset_index()
    weather_data = weather_data.rename(columns={"temp": "temperature",
                                                "time": "datetime_utc",
                                                "dwpt": "dewpoint_temperature",
                                                "rhum": "relative_humidity",
                                                'prcp': 'precipitation_mm',
                                                })
    for number in range(10, 24):
        weather_data[f"hdh_{number}"] = [
            number - x if x < number else 0 for x in weather_data["temperature"]
        ]
        weather_data[f"cdh_{number}"] = [
            x - number if x > number else 0 for x in weather_data["temperature"]
        ]
    weather_data.columns = [i.lower() for i in weather_data.columns]

    # get the date features
    weather_data = (
        parse_date_features(weather_data.set_index("datetime_utc"))
        .reset_index()
        .sort_values(
            [
                "datetime_utc",
            ]
        )
    )
    z.append(weather_data)

zdf = pd.concat(z)
zdf.to_parquet(data_path + "nyc_weather_data.parquet")
print(zdf.sample(15).to_string())

def download_all_nyctaxi_files():
    counter = 0
    for y in years[::-1]:
        for t in trip_types[:-1]:
            for m in months:
                try:
                    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{t}_tripdata_{y}-{m:02}.parquet"
                    print(t,y,m,url)
                    z = download_parquet_file(url, data_path + f"nyctaxi_{t}_{y}_{m:02}.parquet")
                    if z:
                        counter = counter + 1
                    # if counter >= 400:
                    #     return
                except Exception as ee:
                    print(ee)
                    print("Can't download file")

# @ray.remote
def download_parquet_file(url, output_file):
    # Make a GET request to fetch the file
    response = requests.get(url)
    time.sleep(5)
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


# download_all_nyctaxi_files()

print(datetime.datetime.now()-today)

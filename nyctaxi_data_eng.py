import sqlite3
import pickle
import holidays
import gc
import io
import dask.dataframe as dd
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool
from itertools import product
from jragbeer_common import *


switch = {"Green": "Green_Taxi_Trip_Data",
          "Yellow": "Yellow_Taxi_Trip_Data",
          "FHV": "For_Hire_Vehicles_Trip_Data",
          "HVFHV": "HV_FHV", }
buffer = io.StringIO()

# db = 'NYC_Taxi_Data.db'
# path2 = "C:/Users/Julien/PycharmProjects/NYCTaxi/data/"
# conn = sqlite3.connect(path2 + db)

# df = pd.read_sql('select PULocationID, DOLocationID, passenger_count, pickup_hour from Trip_Data where pickup_hour >= 18 and pickup_hour < 21', conn)
# print(df.sample(26).to_string())
# print(datetime.datetime.now()-timee)
# print('over')
# df_one = {}

#
ny_holidays = holidays.country_holidays("US", state='NY')
path = os.path.abspath(os.path.dirname(__file__)).replace("\\", "/") + "/"
data_path = path + 'data/'
today = datetime.datetime.today()
print(today)

load_dotenv(path + '.env')
secrets = dict(dotenv_values(path + ".env"))

# logger
dagster_logger = logging.getLogger("dagster_logger")
dagster_logger.setLevel(logging.INFO)
# create console handler
handler = logging.StreamHandler()
# create formatter and add it to the handler
formatter = logging.Formatter('%(asctime)s, %(message)s')
handler.setFormatter(formatter)
# add the handler to the logger
dagster_logger.addHandler(handler)
# create console handler
handler2 = logging.FileHandler(path + "dagster_logger.log")
# add the handler to the logger
dagster_logger.addHandler(handler2)

# Pyarrow Strings (should be faster)
pd.options.future.infer_string = True
pd.options.mode.string_storage = "pyarrow"


# Polars settings
pl.Config.set_tbl_cols(100)
pl.Config.set_tbl_rows(100)
pl.Config.set_fmt_str_lengths(1000)
pl.Config.set_tbl_dataframe_shape_below(True)
pl.Config.set_tbl_width_chars(1000)


colour = 'yellow'
all_points = np.asarray(pd.read_csv(data_path + 'zone_lookup.csv')['LocationID'])

# util and functions no longer used
def rename_file_with_pattern(folder_path, pattern = r'_tripdata_(\d{4})-(\d{2})\.parquet$'):
    """
    Rename a file in the given folder that matches the pattern with a new name
    based on the year and month extracted from the filename.

    Args:
    - folder_path (str): Path to the folder containing the file.
    - pattern (str): Regular expression pattern to match the filename. Pattern to match 'fhvhv_tripdata_2023-09.parquet'

    Returns:
    - bool: True if a file was renamed successfully, False otherwise.
    """
    # List all files in the folder
    files = os.listdir(folder_path)

    # Iterate through each file
    for filename in files:
        print(filename)
        # Check if the filename matches the pattern
        match = re.search(pattern, filename)
        if match:
            print(filename, 'xx')
            # Extract year and month from the filename
            year = match.group(1)
            month = match.group(2)

            # Construct the new file name
            new_name = f"nyctaxi_hvfhv_{year}_{month}.parquet"

            # Build the full paths
            file_path = os.path.join(folder_path, filename)
            new_file_path = os.path.join(folder_path, new_name)

            try:
                # Rename the file
                os.rename(file_path, new_file_path)
                print(f"File '{filename}' renamed to '{new_name}'")
                # return True  # Return True upon successful rename
            except OSError as e:
                print(f"Error renaming file '{filename}': {e}")
                return False  # Return False if an error occurs


def create_initial_database_from_csv(year, colour):
    # create database and add data to it
    try:
        print(year, colour)
        fp = path + f"{year}_{switch[colour]}.csv"
        dfcolumns = pd.read_csv(fp, nrows=1)
        df_list = []
        data = pd.read_csv(fp, header=None, skiprows=1, usecols=list(range(1, len(dfcolumns.columns))),
                           chunksize=15_000_000, names=dfcolumns.columns, error_bad_lines=False, engine='python')
        n = 1
        for chunk in data:
            print(n)
            df_list.append(initial_data_clean(chunk))
            n += 1

        large_df = pd.concat(df_list)
        if 'fhv' in colour.lower():
            large_df.to_sql(f'{year}_FHV', conn, if_exists='append', index=False)
        else:
            large_df.to_sql(f'{year}_{colour.upper()}', conn, if_exists='replace', index=False)
        print('done', datetime.datetime.now() - timee)

    except Exception as e:
        print(error_handling())
def read_db(tablename, conn_):
    query = "select * from {}".format(tablename)
    df = pd.read_sql(query, conn_, parse_dates=['pickup_datetime', 'dropoff_datetime'])
    return df
def make_season(series):
    array = []
    for x in series:
        new_date = datetime.datetime(2000, x.month, x.day)
        if datetime.datetime(2000, 3, 1) <= new_date < datetime.datetime(2000, 6, 1):
            array.append('spring')
        elif datetime.datetime(2000, 6, 1) <= new_date < datetime.datetime(2000, 9, 1):
            array.append('summer')
        elif datetime.datetime(2000, 9, 1) <= new_date < datetime.datetime(2000, 12, 1):
            array.append('fall')
        else:
            array.append('winter')
    return array
def thing(weekend, season, day_of_week, hr, m, yr, h, each, data, right):
    # bad function, aiming to cut down on number of tasks to do at the end for post_transform
    for d in day_of_week:
        data = data[data['day_of_week'] == d]
        for s in season:
            data = data[data['season'] == s]
            for w in weekend:
                data = data[data['weekend'] == w]
                value_counts = pd.DataFrame(data[each].value_counts())
                value_counts.columns = ['value']
                new_vals_df = pd.concat([pd.DataFrame(data={'value': 1}, index=[i]) for i in all_points if i not in list(value_counts.index)])
                tmp = pd.concat([new_vals_df, value_counts], )
                right['Pick-Up'][str('green')][yr][m][hr][h][d][s][w] = tmp
    return right
# part one (read from csv, initial clean, save as parquet) | use create_parquet_by_colour to start
def initial_data_clean(df):
    """
    Make the numeric columns smaller and drop any bad lines.
    :param df: initial NYC Taxi csv data - raw (but column names are changed)
    :return: a df with a few rows dropped and downcasted
    """
    df['dolocationid'] = pd.to_numeric(df['dolocationid'], errors='coerce', downcast='float')
    df['pulocationid'] = pd.to_numeric(df['pulocationid'], errors='coerce', downcast='float')
    df = df.replace([np.inf, -np.inf], np.nan)
    try:
        df = df.dropna(how='any')
    except ValueError as ve:
        print(df.sample(5).to_string())
        print(df[df.isna().any(axis=1)].to_string())
    df['dolocationid'] = pd.to_numeric(df['dolocationid'], errors='coerce', downcast='integer')
    df['pulocationid'] = pd.to_numeric(df['pulocationid'], errors='coerce', downcast='integer')
    df.reset_index(inplace=True, drop=True)
    return df
def readcsv(file):
    """
    This function cleans the column names of the csv file and drops bad rows (via a *initial_data_clean* call).

    :param file: NYC Taxi csv file
    :return: cleaned df
    """
    try:
        dfcolumns = pd.read_csv(file, nrows=1)
        # make all of the different col names across files the same
        a = dfcolumns.columns
        cols_to_use = [x for x in a if 'datetime' in x.lower()] + [x for x in a if 'locationid' in x.lower()]
        data = pd.read_csv(file, usecols=cols_to_use, error_bad_lines=False, )
        data.columns = [x.replace(' ', '_').lower().replace('lpep_', '').replace("tpep_", '') for x in data.columns]
        # print(len(data.index))
        return initial_data_clean(data)
    except:
        print(file)
def create_parquet_by_colour(colour='green'):
    global timee
    csv_path = path2 + f'/{colour}/'
    files = os.listdir(csv_path)
    files = [csv_path + x for x in files]
    file_sizes = 0
    for each in files:
        file_sizes += os.stat(each).st_size / 1024**3
    logging.info(f"{colour.capitalize()} total file size as CSV: {file_sizes:.2f} GB")
    # set up your pool
    # with Pool(processes=4) as pool:
    #     # have your pool map the file names to dataframes
    #     df_list = pool.map_async(readcsv, files,)
    #     df_list.wait()
    #     df = pd.concat(df_list.get(), ignore_index=True)
    df = pd.concat((readcsv(each) for each in files[::-1]), ignore_index=True)
    df.info()
    df = initial_data_clean(df)
    cols = [i for i in df.columns if i not in ['dolocationid', 'pulocationid']]
    for i in cols:
        try:
            df[i] = df[i].astype(str)
        except:
            pass
    df.to_parquet(path + f'{colour}_newdata_oct2020.parquet')
    logging.info(f'{colour} | initial clean finished for all processes: {datetime.datetime.now() - timee}')
def create_parquet_by_colour_pipe(colour='green'):
    global timee
    csv_path = path2 + f'/{colour}/'
    files = os.listdir(csv_path)
    files = [csv_path + x for x in files]
    file_sizes = 0
    for each in files:
        file_sizes += os.stat(each).st_size / 1024**3
    logging.info(f"{colour.capitalize()} total file size as CSV: {file_sizes:.2f} GB")
    # set up your pool
    # with Pool(processes=4) as pool:
    #     # have your pool map the file names to dataframes
    #     df_list = pool.map_async(readcsv, files,)
    #     df_list.wait()
    #     df = pd.concat(df_list.get(), ignore_index=True)
    df = pd.concat((readcsv(each) for each in files[::-1]), ignore_index=True)
    df.info()
    df = initial_data_clean(df)
    cols = [i for i in df.columns if i not in ['dolocationid', 'pulocationid']]
    for i in cols:
        try:
            df[i] = df[i].astype(str)
        except:
            pass
    logging.info(f'{colour} | initial clean finished for all processes: {datetime.datetime.now() - timee}')
    return df
    # df.to_parquet(path + f'{colour}_data_oct2020.parquet')
# part two (transformation functions, from datelike-strings to features for filtering) | use a data_transforms function
@dask.delayed
def inner_trans(df):
    df['dolocationid'] = df['dolocationid'].astype(np.int16)
    df['pulocationid'] = df['pulocationid'].astype(np.int16)
    df.reset_index(inplace=True, drop=True)
    # logging.info(df.sample(5).to_string())
    # df.info()
    # logging.info(datetime.datetime.now() - timee)
    try:
        df['dropoff_date'] = pd.to_datetime(df['dropoff_datetime'], errors='coerce')
    except:
        df['dropoff_date'] = df['dropoff_datetime']
    try:
        df['pickup_date'] = pd.to_datetime(df['pickup_datetime'], errors='coerce')
    except:
        df['pickup_date'] = df['pickup_datetime']
    if df['dropoff_date'].equals(df['dropoff_datetime']):
        pass
    else:
        df['dropoff_date'] = df['pickup_date']
    df = df.dropna(how='any')
    df = df.drop(['dropoff_datetime', 'pickup_datetime'], 1, )
    tmp = {'weekend': [],
           "day_of_week": [],
           'holiday': []}
    df['month'] = df['pickup_date'].dt.month
    df['p_Hour'] = df['pickup_date'].dt.hour
    df['day'] = df['pickup_date'].dt.day
    df['year'] = df['pickup_date'].dt.year
    df['d_Hour'] = df['dropoff_date'].dt.hour
    for x in df['pickup_date']:
        p = x.isoweekday()
        tmp["day_of_week"].append(p)
        if p in [6, 7]:
            tmp['weekend'].append(1)
        else:
            tmp['weekend'].append(0)
        if x.date() in ny_holidays:
            tmp["holiday"].append(1)
        else:
            tmp["holiday"].append(0)
    for k, v in tmp.items():
        df[k] = pd.Series(v).astype(np.int16)
    df['season'] = pd.Series(make_season(df['pickup_date'])).astype('category')
    return df
# @dask.delayed
def inner_trans2(df):
    df['dolocationid'] = df['dolocationid'].astype(np.int16)
    df['pulocationid'] = df['pulocationid'].astype(np.int16)
    logging.info(f"0, {datetime.datetime.now() - timee}")
    df.reset_index(inplace=True, drop=True)
    try:
        df['dropoff_date'] = pd.to_datetime(df['dropoff_datetime'], errors='coerce', format='%m/%d/%Y %I:%M:%S %p')
    except Exception as e:
        df['dropoff_date'] = df['dropoff_datetime']
    logging.info(f"1, {datetime.datetime.now() - timee}")
    try:
        df['pickup_date'] = pd.to_datetime(df['pickup_datetime'], errors='coerce', format='%m/%d/%Y %I:%M:%S %p')
    except:
        df['pickup_date'] = df['pickup_datetime']
    # years = pick_up_date.astype('datetime64[Y]').astype(int) + 1970
    # months = pick_up_date.astype('datetime64[M]').astype(int) % 12 + 1
    # days = (pick_up_date - pick_up_date.astype('datetime64[M]')).astype(int) + 1
    # hours = pick_up_date.astype('datetime64[H]').astype(int)
    # for i, each in enumerate(pick_up_date):
    #     print(each, years[i], months[i], days[i], hours[i])
    logging.info(f"2, {datetime.datetime.now() - timee}")
    df = df.drop(['dropoff_datetime', 'pickup_datetime'], 1, )
    df = df.replace([np.inf, -np.inf,'NaN', 'NaT'], np.nan)
    try:
        df = df.dropna(how='any')
    except ValueError as ve:
        print(df.sample(15).to_string())
        print('you suck')
        # print(df[df.isna().any(axis=1)].to_string())
    df['month'] = df['pickup_date'].dt.month
    df['p_Hour'] = df['pickup_date'].dt.hour
    # df['day'] = df['pickup_date'].dt.day
    df['year'] = df['pickup_date'].dt.year
    df['d_Hour'] = df['dropoff_date'].dt.hour
    df['day_of_week'] = df['pickup_date'].dt.weekday + 1
    # df['weekend'] = (df['day_of_week'] < 6).astype(int)
    df['holiday'] = df['pickup_date'].isin(ny_holidays)
    df.drop(columns=['dropoff_date', 'pickup_date'], inplace=True)
    # seasons = ['winter', 'winter', 'winter', 'spring', 'spring', 'spring', 'summer', 'summer', 'summer', 'fall', 'fall', 'fall',]
    # df['season'] = df['month'].map(dict(zip(range(1, 13), seasons))).astype('category')
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col], downcast='integer')
        except:
            pass
    logging.info(f"vectorize done: {datetime.datetime.now() - timee}")
    gc.collect()
    return df

def data_transforms_dask(colour='green', size=2_200_000):
    # dfs = []
    # counter = 0
    # for yr in range(2016, 2020):
    #     for chunk in pd.read_sql(f"""select * from "{yr}_{colour.upper()}" """, conn, chunksize=size):
    #         counter += len(chunk)
    #         print(f"# of rows: {counter}")
    #         dfs.append(inner_trans(chunk))

    df = pd.read_parquet(path + f'{colour}_data.parquet')
    logging.info(datetime.datetime.now() - timee)
    # df = pd.concat(dask.compute(*dfs))
    df['colour'] = colour
    df['colour'] = df['colour'].astype('category')
    df.columns = [x.lower() for x in df.columns]
    # df.info(buf=buffer)
    # with open(path + "log.txt", "a",
    #           encoding="utf-8") as f:
    #     f.write(buffer.getvalue())
    df.info()
    logging.info(datetime.datetime.now() - timee)
    pickle_out = open(path + f"data_{colour}_all.pickle", "wb")
    pickle.dump(df, pickle_out, protocol=4)
    pickle_out.close()
    gc.collect()
    logging.info(datetime.datetime.now() - timee)
def data_transforms_orig_vector(colour='green', ):
    # dfs = (pd.read_sql(f"""# select * from "{yr}_{colour.upper()}" """, conn, ) for yr in range(2016, 2020))
    # df = pd.concat(dfs)
    df = pd.read_parquet(path + f'{colour}_newdata_oct2020.parquet')
    print(len(df.index))
    df = inner_trans2(df)
    df['colour'] = colour
    df['colour'] = df['colour'].astype('category')
    df.columns = [x.lower() for x in df.columns]
    file_name = f'df_done_{colour}_oct2020.parquet'
    done_time = datetime.datetime.now() - timee
    df.to_parquet(path + file_name)
    logging.info(f"Done in {done_time} ({done_time.seconds} seconds) | {colour} file available at: {str(path)}{file_name}")
def data_transforms_mp_vector(colour='green', size=3_000_000, ):
    dfs=[]
    pool = Pool()
    df = pd.read_parquet(path + f'{colour}_data_oct2020.parquet')
    logging.info(datetime.datetime.now() - timee)
    for x in tqdm(np.split(df, np.arange(size, len(df), size))):
        dfs.append(pool.apply_async(inner_trans2, args=(x,),))
    df = pd.concat([i.get() for i in dfs])
    df.columns = [x.lower() for x in df.columns]
    df['colour'] = colour
    df['colour'] = df['colour'].astype('category')
    logging.info(df.sample(5).to_string())
    df.info(buf=buffer)
    with open(path + "log.txt", "a",
              encoding="utf-8") as f:
        f.write(buffer.getvalue())
    logging.info(datetime.datetime.now() - timee)
    gc.collect()
    file_name = f'df_done_{colour}_oct2020.parquet'
    df.to_parquet(path + file_name)
    done_time = datetime.datetime.now() - timee
    logging.info(f"Done in {done_time} ({done_time.seconds} seconds) | {colour} file available at: {str(path)}{file_name}")
def data_transforms_tp_vector(colour='green', size=1_500_000, ):
    dfs=[]
    pool = ThreadPool()
    df = pd.read_parquet(path + f'{colour}_data_oct2020.parquet')
    logging.info(datetime.datetime.now() - timee)
    for x in tqdm(np.split(df, np.arange(size, len(df), size))):
        dfs.append(pool.apply_async(inner_trans2, args=(x,),))
    df = pd.concat([i.get() for i in dfs])
    df.columns = [x.lower() for x in df.columns]
    df['colour'] = colour
    df['colour'] = df['colour'].astype('category')
    logging.info(df.sample(5).to_string())
    df.info(buf=buffer)
    with open(path + "log.txt", "a",
              encoding="utf-8") as f:
        f.write(buffer.getvalue())
    logging.info(datetime.datetime.now() - timee)
    gc.collect()
    file_name = f'df_done_{colour}_oct2020.parquet'
    df.to_parquet(path + file_name)
    done_time = datetime.datetime.now() - timee
    logging.info(f"Done in {done_time} ({done_time.seconds} seconds) | {colour} file available at: {str(path)}{file_name}")
def data_transforms_mp_vector_half_pipe(df, colour='green', size=6_000_000, ):
    dfs = []
    pool = Pool()
    # df = pd.read_parquet(path + f'{colour}_data_oct2020.parquet')
    logging.info(datetime.datetime.now() - timee)
    for x in tqdm(np.split(df, np.arange(size, len(df), size))):
        dfs.append(pool.apply_async(inner_trans2, args=(x,), ))
    df = pd.concat([i.get() for i in dfs])
    df.columns = [x.lower() for x in df.columns]
    df['colour'] = colour
    df['colour'] = df['colour'].astype('category')
    logging.info(df.sample(5).to_string())
    df.info(buf=buffer)
    with open(path + "log.txt", "a",
              encoding="utf-8") as f:
        f.write(buffer.getvalue())
    logging.info(datetime.datetime.now() - timee)
    gc.collect()
    file_name = f'df_done_{colour}_oct2020.parquet'
    df.to_parquet(path + file_name)
    done_time = datetime.datetime.now() - timee
    logging.info(f"Done in {done_time} ({done_time.seconds} seconds) | {colour} file available at: {str(path)}{file_name}")
def data_transforms_mp_vector_full_pipe(df, colour='green', size=6_000_000, ):
    dfs = []
    pool = Pool()
    # df = pd.read_parquet(path + f'{colour}_data_oct2020.parquet')
    logging.info(datetime.datetime.now() - timee)
    for x in tqdm(np.split(df, np.arange(size, len(df), size))):
        dfs.append(pool.apply_async(inner_trans2, args=(x,), ))
    df = pd.concat([i.get() for i in dfs])
    df.columns = [x.lower() for x in df.columns]
    df['colour'] = colour
    df['colour'] = df['colour'].astype('category')
    try:
        logging.info(df.sample(5).to_string())
        df.info(buf=buffer)
        with open(path + "log.txt", "a",
                  encoding="utf-8") as f:
            f.write(buffer.getvalue())
        logging.info(datetime.datetime.now() - timee)
    except:
        print(df)
    gc.collect()
    file_name = f'df_done_{colour}_oct2020.parquet'
    # df.to_parquet(path + file_name)
    done_time = datetime.datetime.now() - timee
    logging.info(f"Done in {done_time} ({done_time.seconds} seconds) | {colour} file available at: {str(path)}{file_name}")
    return df
def data_transforms_dask_vector(colour='green', size=1_500_000, ):
    dfs=[]
    df = pd.read_parquet(path + f'{colour}_data_oct2020.parquet')
    # df = dask.delayed(pd.read_parquet)(f'{colour}_data_oct2020.parquet')  # Let Dask build object
    logging.info(datetime.datetime.now() - timee)
    for x in np.split(df, np.arange(size, len(df), size)):
        dfs.append(inner_trans2(x))
    df = pd.concat(dask.compute(*dfs))
    df.columns = [x.lower() for x in df.columns]
    df['colour'] = colour
    df['colour'] = df['colour'].astype('category')
    logging.info(df.sample(5).to_string())
    df.info(buf=buffer)
    with open(path + "log.txt", "a",
              encoding="utf-8") as f:
        f.write(buffer.getvalue())
    logging.info(datetime.datetime.now() - timee)
    gc.collect()
    file_name = f'df_done_{colour}_oct2020.parquet'
    df.to_parquet(path + file_name)
    done_time = datetime.datetime.now() - timee
    logging.info(f"Done in {done_time} ({done_time.seconds} seconds) | {colour} file available at: {str(path)}{file_name}")
def data_transforms_dask_vector_npartitions(colour='green', size=int(5_700_000/4)+1, ):
    ddf = pd.read_parquet(path + f'{colour}_data_oct2020.parquet',)
    df = dd.from_pandas(ddf, npartitions=8*10)
    logging.info(datetime.datetime.now() - timee)
    df = df.map_partitions(inner_trans2)
    df.columns = [x.lower() for x in df.columns]
    df = df.persist()
    print(type(df))
    df['colour'] = colour
    df['colour'] = df['colour'].astype('category')
    # logging.info(df.sample(5).to_string())
    # df.info(buf=buffer)
    # with open(path + "log.txt", "a",
    #           encoding="utf-8") as f:
    #     f.write(buffer.getvalue())
    logging.info(datetime.datetime.now() - timee)
    gc.collect()
    file_name = f'df_done_{colour}_oct20201.parquet'
    df = df.compute()
    df.to_parquet(path + file_name, single_file = True)
    print(type(df))
    done_time = datetime.datetime.now() - timee
    logging.info(f"Done in {done_time} ({done_time.seconds} seconds) | {colour} file available at: {str(path)}{file_name}")
def data_transforms(colour='green', size=2_200_000):
    dfs = pd.concat(pd.read_sql(f"""select * from "{yr}_{colour.upper()}" """, conn, ) for yr in [2016, 2017, 2018, 2019])
    df = pd.concat([inner_trans(x) for x in np.split(dfs, np.arange(size, len(dfs), size))])
    df['colour'] = colour
    df.columns = [x.lower() for x in df.columns]
    logging.info(df.sample(5).to_string())
    df.info(buf=buffer)
    with open(path + "log.txt", "a",
              encoding="utf-8") as f:
        f.write(buffer.getvalue())
    logging.info(datetime.datetime.now() - timee)
    pickle_out = open(path + f"data_{colour}_all.pickle", "wb")
    pickle.dump(df, pickle_out, protocol=4)
    pickle_out.close()
    gc.collect()
    logging.info(datetime.datetime.now() - timee)
def data_transforms_orig(colour='green', ):
    dfs = (pd.read_sql(f"""select * from "{yr}_{colour.upper()}" """, conn, ) for yr in [2016, 2017, 2018, 2019])
    df = pd.concat(dfs)
    df = inner_trans(df)
    df['colour'] = pd.Series([colour for x in range(len(df.index))]).astype('category')
    df.columns = [x.lower() for x in df.columns]
    logging.info(df.sample(5).to_string())
    df.info(buf=buffer)
    with open(path + "log.txt", "a",
              encoding="utf-8") as f:
        f.write(buffer.getvalue())
    logging.info(datetime.datetime.now() - timee)
    pickle_out = open(path + f"data_{colour}_all_orig.pickle", "wb")
    pickle.dump(df, pickle_out, protocol=4)
    pickle_out.close()
    gc.collect()
    logging.info(datetime.datetime.now() - timee)

# part three (convert from dataframe to nested dictionary, so bokeh can read it easier?) | use post_transform
def inner_post_transform(holi, hr, d,each, data_):
    # filter down the data
    data = data_[(data_['day_of_week'] == d) & (data_[each['hour']] == hr) & (data_['holiday'] == holi)]
    value_counts = pd.DataFrame({'value':data[each['col']].value_counts()})
    # create df of all 1's for each locationID not in the resultant df
    new_vals_df = pd.concat([pd.DataFrame(data={'value': 1}, index=[i]) for i in all_points if i not in list(value_counts.index)])
    return  pd.concat([value_counts, new_vals_df, ],)# concat the two dfs
def post_parallel_function(data_, params, each,input_dict):
        config = {'Pick-Up':{'col':'pulocationid', 'name':'Pick-Up', 'hour':'p_hour'},
                  'Drop-Off':{'col':'dolocationid', 'name':'Drop-Off', 'hour':'d_hour' }}
        dat = data_[(data_['month'] == params[1]) & (data_['year'] == params[0])].copy()
        day_of_week = range(1, 8)
        hour = range(0, 24)
        holiday = [False, True]
        for d in day_of_week:
            for hr in hour:
                for h in holiday:
                # print(params, h, d, hr)
                # print(dat)
                    input_dict[params[0]][params[1]][hr][d][h] = inner_post_transform(h, hr, d,config[each], dat)
        # pprint(input_dict)
        return input_dict
def post_transform_serial(data, color):
    years = range(2016, 2020)
    months = range(1,13)
    day_of_week = range(1, 8)
    hour = range(0, 24)
    holiday = [False, True]
    inner_dict = {y:
                      {m:
                           {hr:
                                {d:
                                     {h: {} for h in list(holiday)}
                                 for d in list(day_of_week)}
                            for hr in list(hour)}
                       for m in list(months)}
                  for y in list(years)}
    year_month_combos = [i for i in list(product(years, months)) if not (i[0] == 2016 and i[1] < 7)]
    data.info()
    print(data.memory_usage())
    print(data.sample(100).to_string())
    pu_do_data = {t:post_parallel_function(data.copy(), param, t, inner_dict) for t in ['Pick-Up', 'Drop-Off'] for param in tqdm(year_month_combos)}
    pickle_out = open(f"{color}_output_oct2020.pickle", "wb")
    pickle.dump(pu_do_data, pickle_out)
    pickle_out.close()
    return pu_do_data


def post_transform_mp(data, ):
    years = range(2016, 2020)
    months = range(1, 13)
    day_of_week = range(1, 8)
    hour = range(0, 24)
    holiday = [False, True]
    inner_dict = {y:
                      {m:
                           {hr:
                                {d:
                                     {h: {} for h in list(holiday)}
                                 for d in list(day_of_week)}
                            for hr in list(hour)}
                       for m in list(months)}
                  for y in list(years)}

    new_dict = {hr: {d: {h: {} for h in list(holiday)} for d in list(day_of_week)} for hr in list(hour)}
    year_month_combos = [i for i in list(product(years, months)) if not (i[0] == 2016 and i[1] < 7)]
    print(year_month_combos)
    huh = {t: post_parallel_function(data.copy(), param, t, inner_dict) for t in ['Pick-Up', 'Drop-Off'] for param in tqdm(year_month_combos)}
    print(huh['Pick-Up'][2017])
    # data.info()
    # print(data.memory_usage())
    # print(data.sample(100).to_string())
    # for param in tqdm(year_month_combos):
    #     for t in ['Pick-Up', 'Drop-Off']:
    #         pu_do_data[t] =
    #     for t in ['p', 'd']:
    #         dx = data.copy()
    #         stuff[str(param) + str(thing)] = Process(post_parallel_function, args=(dx, param, t, ))
    #         stuff[str(param) + str(thing)].start()
    # for each in stuff.keys():
    #     stuff[each].join()
    # pickle_out = open(f"{color}_output_oct2020.pickle", "wb")
    #     # pickle.dump(pu_do_data, pickle_out)
    #     # pickle_out.close()
    return huh
# create_parquet_by_colour('yellow')

if __name__ == '__main__':
    # client = Client(n_workers=8, threads_per_worker=1, memory_limit='7.75GB')
    out = {}
    colour = 'green'
    cols = ['green', 'yellow']
    timee = datetime.datetime.now()
    # data_transforms_mp_vector(colour)
    # post_transform(data_transforms_mp_vector_full_pipe(create_parquet_by_colour_pipe(colour), colour),)
    # data_transforms_mp_vector_half_pipe(create_parquet_by_colour_pipe(colour), colour)
    # create_parquet_by_colour(colour)
    t = post_transform_serial(data_transforms_mp_vector_full_pipe(create_parquet_by_colour_pipe(colour), colour), colour)
    # for a in cols:
    #     out[a] = post_transform(data_transforms_mp_vector_full_pipe(create_parquet_by_colour_pipe(a), a),)
    # pickle_out = open(f"output2_oct2020.pickle", "wb")
    # pickle.dump(out, pickle_out)
    # pickle_out.close()
    # aa = post_transform_serial(pd.read_parquet(path + f"df_done_{colour}_oct2020.parquet"), colour)
    # print(aa)
    # pickle_in = open(f"output_oct2020.pickle", "rb")
    # example_dict = pickle.load(pickle_in)
    # print(example_dict.keys())
    # for i,v in example_dict.items():
    #     print(i)
    # print(example_dict['green']['green'].keys())
    qq = datetime.datetime.now()-timee
    print(qq, qq.seconds)

    #
    pass
# #     # client = Client()
# #     # print(client)
#     # for col in ['yellow']:
# #     #     # create_parquet_by_colour(col)
# #     #     # logging.info(datetime.datetime.now() - timee)
# #     #     data_transforms_dask_vector(col)
# #     #     # func = data_transforms_orig_vector
# #     #     # logging.info(func.__name__)
# #     #     # func()
# #     #     gc.collect()
# #     #     logging.info(datetime.datetime.now() - timee)

#     post_transform()

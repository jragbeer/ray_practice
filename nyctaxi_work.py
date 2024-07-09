from nyctaxi_data import *
import pyarrow as pa



# 513mB on disk
# 3.6GB in memory to start

# rename_file_with_pattern(data_path, )

mapper = {"N":0, "Y":1, "None":pa.null()}
dfs = []

def clean_data():
    for num in ['02', '03', '04',
                # '05', '06', '07',
                # '08', '09', '10',
                # '11', '12',
                ]:
        idf = pd.read_parquet(data_path + f"nyctaxi_hvfhv_2019_{num}.parquet",
                              dtype_backend = 'pyarrow',
                              )
        print(idf.memory_usage(deep=True))
        idf.info(verbose=True, memory_usage='deep')
        print(idf.head().to_string())
        print(datetime.datetime.now()-today)

        for x in ["shared_request_flag", "wav_match_flag", "wav_request_flag", "access_a_ride_flag", "shared_match_flag"]:
            idf[x] = pd.to_numeric(idf[x].map(mapper), downcast='integer', dtype_backend = 'pyarrow')

        for col, dt in zip(idf.columns, idf.dtypes):
            if 'int' in str(dt):
                idf[x] = pd.to_numeric(idf[x], downcast='integer', dtype_backend = 'pyarrow')
            if 'double' in str(dt):
                idf[x] = (idf[x] * 100).astype('int')
                idf[x] = pd.to_numeric(idf[x], downcast='integer', dtype_backend = 'pyarrow')


        print(idf.memory_usage(deep=True))
        idf.info()
        print(idf.head().to_string())
        dfs.append(idf)
        print(f'fhvhv_tripdata_2019-{num}.parquet DONE IN: ', datetime.datetime.now() - today)

    done = pd.concat(dfs)
    print(done.memory_usage(deep=True))
    done.info()
    print(done.head().to_string())

clean_data()

print('FINISHED: ',datetime.datetime.now()-today)

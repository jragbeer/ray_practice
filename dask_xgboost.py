import dask.dataframe as dd
import dask.array as da
import xgboost
from dask_ml.metrics import mean_squared_error
from distributed import Client
from dask_cuda import LocalCUDACluster
from nyctaxi_data_eng import *
import dask_cudf
import cudf


def make_cv_splits(n_folds):
    frac = [1 / n_folds] * n_folds
    splits = df.random_split(frac, shuffle=True)
    for i in range(n_folds):
        train = [splits[j] for j in range(n_folds) if j != i]
        test = splits[i]
        yield dd.concat(train), test

if __name__ == '__main__':

    s = cudf.Series([1, 2, 3, None, 4])
    print(s)

    # cluster = LocalCUDACluster()
    # client = Client(cluster)
    client = Client(n_workers=1, threads_per_worker=8)
    list_of_dfs = []
    for x in [f"nyctaxi_hvfhv_2023_{i:02}.parquet" for i in range(1, 13)]:
        try:
            print(x)
            iidf = dd.read_parquet(data_path + x)
            list_of_dfs.append(iidf)
        except:
            pass
    for x in [f"nyctaxi_hvfhv_2022_{i:02}.parquet" for i in range(1, 13)]:
        try:
            print(x)
            iidf = dd.read_parquet(data_path + x)
            list_of_dfs.append(iidf)
        except:
            pass
    df = dd.concat(list_of_dfs)
    df.info(memory_usage = 'deep')
    df = df[["trip_miles" , "trip_time"]]
    # df = df.categorize(columns=df.select_dtypes(include="category").columns.tolist()) # Categorize
    print(df.head().to_string())

    df = df.persist()
    scores = []
    for i, (train, test) in enumerate(make_cv_splits(10)):
        print(f"Split #{i + 1} / 5 ...")
        y_train = train["trip_time"]
        X_train = train.drop(columns=["trip_time"])
        y_test = test["trip_time"]
        X_test = test.drop(columns=["trip_time"])

        d_train = xgboost.dask.DaskDMatrix(client, X_train, y_train, enable_categorical=True)
        model = xgboost.dask.train(
            client,
            {"tree_method": "hist"},
            d_train,
            num_boost_round=4,
            evals=[(d_train, "train")],
        )
        predictions = xgboost.dask.predict(client, model, X_test)

        score = mean_squared_error(
            y_test.to_dask_array(),
            predictions.to_dask_array(),
            squared=False,
            compute=False,
        )
        scores.append(score.reshape(1).persist())

    scores = da.concatenate(scores).compute()
    print(f"MSE score = {scores.mean()} +/- {scores.std()}")
    print(datetime.datetime.now()-today)
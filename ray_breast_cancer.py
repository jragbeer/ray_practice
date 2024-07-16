from xgboost_ray import RayDMatrix, RayParams, train, predict
from sklearn.datasets import load_breast_cancer
import xgboost as xgb
from nyctaxi_data_eng import *

train_x, train_y = load_breast_cancer(return_X_y=True)
train_set = RayDMatrix(train_x, train_y)

evals_result = {}
bst = train(
    {
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
    },
    train_set,
    evals_result=evals_result,
    evals=[(train_set, "train")],
    verbose_eval=False,
    ray_params=RayParams(num_actors=2, cpus_per_actor=1))

bst.save_model(data_path + "model.xgb")
print(f"""Final training error: {evals_result["train"]["error"][-1]:.4f}""")

## simple prediction example

data, labels = load_breast_cancer(return_X_y=True)

dpred = RayDMatrix(data, labels)

bst = xgb.Booster(model_file=data_path + "model.xgb")
pred_ray = predict(bst, dpred, ray_params=RayParams(num_actors=2))

print(pred_ray)
print(datetime.datetime.now()-today)

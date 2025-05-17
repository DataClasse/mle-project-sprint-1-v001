# mle_projects/mle-project-sprint-1-v001/part2_dvc/scripts/evaluate.py
import pandas as pd
from sklearn.model_selection import cross_validate, KFold
from joblib import load
import yaml
import os
import json
import numpy as np

def convert_to_list(arr):
    if isinstance(arr, np.ndarray):
        return arr.tolist()
    return arr

def evaluate_model():
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)
    data = pd.read_csv('data/initial_data.csv', index_col=params['index_col'])
    pipeline = load('models/fitted_model.pkl')
    if params['use_kfold']:
        cv = KFold(n_splits=params['n_splits'], shuffle=True, random_state=params['seed'])
        scores = cross_validate(
            pipeline,
            data.drop(params['target_col'], axis=1),
            data[params['target_col']],
            scoring=['neg_mean_absolute_error', 'neg_mean_squared_error', 'r2'],
            cv=cv,
            return_train_score=True
        )
    else:
        scores = cross_validate(
            pipeline,
            data.drop(params['target_col'], axis=1),
            data[params['target_col']],
            scoring=['neg_mean_absolute_error', 'neg_mean_squared_error', 'r2'],
            cv=params['n_splits'],
            return_train_score=True
        )

    serializable_scores = {k: convert_to_list(v) for k, v in scores.items()}

    os.makedirs('cv_results', exist_ok=True)
    with open('cv_results/cv_res.json', 'w') as fd:
        json.dump(serializable_scores, fd)

if __name__ == '__main__':
    evaluate_model()
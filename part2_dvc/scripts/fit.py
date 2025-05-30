# mle_projects/mle-project-sprint-1-v001/part2_dvc/scripts/fit.py
import pandas as pd
import yaml
import os
from joblib import dump
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.ensemble import GradientBoostingRegressor

def fit_model():
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)
    data = pd.read_csv('data/initial_data.csv', index_col=params['index_col'])
    
    numerical_features = [
        'total_area', 'living_area', 'kitchen_area', 'rooms', 'floor', 
        'build_year', 'ceiling_height', 'flats_count', 'floors_total', 
        'latitude', 'longitude'
    ]
    
    categorical_features = [
        'building_type', 'has_elevator', 'is_apartment', 'studio'
    ]
    
    # т.к. GradientBoostingRegressor нечувствителен к масштабированию
    preprocessor = ColumnTransformer([
     ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features)
    ], remainder='passthrough')  
    
    pipeline = Pipeline([
        ('preprocessing', preprocessor),
        ('regression', GradientBoostingRegressor(random_state=params['seed']))
    ])
    
    pipeline.fit(
        data.drop(params['target_col'], axis=1), 
        data[params['target_col']]
    )
    
    os.makedirs('models', exist_ok=True)
    dump(pipeline, 'models/fitted_model.pkl')

if __name__ == '__main__':
    fit_model()
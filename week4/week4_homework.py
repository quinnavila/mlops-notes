#!/usr/bin/env python
import sys
from datetime import datetime
import pickle
import pandas as pd


with open('model.bin', 'rb') as f_in:
    dv, model = pickle.load(f_in)


categorical = ['PULocationID', 'DOLocationID']

def read_data(filename):
    df = pd.read_parquet(filename)
   
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df



def save_results(df, y_pred, output_file):
    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred

    df_result.to_parquet(
    output_file,
    engine='pyarrow',
    compression=None,
    index=False
    )
    return df_result

def apply_model(input_file, output_file, run_date):
    df = read_data(input_file)
    # print(df.head())
    year = run_date.year
    month = run_date.month
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    # print(df.head())
    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = model.predict(X_val)

    df_result = save_results(df, y_pred, output_file)
    return df_result


def get_paths(run_date):
    year = run_date.year
    month = run_date.month 

    input_file = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    output_file = f'output/yellow_{year:04d}-{month:02d}.parquet'
    return input_file, output_file


def ride_duration_prediction(run_date: datetime):
    input_file, output_file = get_paths(run_date)

    predictions = apply_model(
        input_file=input_file,
        output_file=output_file,
        run_date=run_date
    )
    return predictions

def run():
    month = int(sys.argv[1])
    year = int(sys.argv[2])  
    
    df_result = ride_duration_prediction(
        run_date=datetime(year=year, month=month, day=1)
    )
    print(df_result['predicted_duration'].mean())

if __name__ == '__main__':
    run()
import pandas as pd
from datetime import datetime

import batch

def dt(hour, minute, second=0):
    return datetime(2022, 1, 1, hour, minute, second)

def test_prepare_data():
    data = [
    (None, None, dt(1, 2), dt(1, 10)),
    (1, None, dt(1, 2), dt(1, 10)),
    (1, 2, dt(2, 2), dt(2, 3)),
    (None, 1, dt(1, 2, 0), dt(1, 2, 50)),
    (2, 3, dt(1, 2, 0), dt(1, 2, 59)),
    (3, 4, dt(1, 2, 0), dt(2, 2, 1)),     
    ]

    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    input_df = pd.DataFrame(data, columns=columns)

    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'duration']
    expected_data = [
    ('-1', '-1', dt(1, 2), dt(1, 10), 8.0),
    ('1', '-1', dt(1, 2), dt(1, 10), 8.0),
    ('1', '2', dt(2, 2), dt(2, 3), 1.0),   
    ]
    expected_output = pd.DataFrame(expected_data, columns=columns)

    categorical = ['PULocationID', 'DOLocationID']
    actual_output = batch.prepare_data(input_df, categorical)
    assert actual_output.shape[0] == expected_output.shape[0]
    assert actual_output.equals(expected_output)
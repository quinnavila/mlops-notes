import os
import pandas as pd
from datetime import datetime

def dt(hour, minute, second=0):
    return datetime(2022, 1, 1, hour, minute, second)


data = [
(None, None, dt(1, 2), dt(1, 10)),
(1, None, dt(1, 2), dt(1, 10)),
(1, 2, dt(2, 2), dt(2, 3)),
(None, 1, dt(1, 2, 0), dt(1, 2, 50)),
(2, 3, dt(1, 2, 0), dt(1, 2, 59)),
(3, 4, dt(1, 2, 0), dt(2, 2, 1)),     
]

columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
df_input = pd.DataFrame(data, columns=columns)

# S3_ENDPOINT_URL = "http://localhost:4566"
S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')

options = {
    'client_kwargs': {
        'endpoint_url': S3_ENDPOINT_URL
    }
}

input_file = "s3://nyc-duration/in/2022-01.parquet"

df_input.to_parquet(
    input_file,
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=options
)
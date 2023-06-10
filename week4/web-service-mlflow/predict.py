import os
import pickle

import mlflow
from flask import Flask, request, jsonify


# RUN_ID = os.getenv('RUN_ID')
RUN_ID = 'dc53d5f5644143c5817a33f7cb2097f3'

# mlflow.set_tracking_uri("http://127.0.0.1:5000")
# mlflow.set_experiment("green-taxi-duration")

logged_model = f's3://mlflow-models-6-6/1/{RUN_ID}/artifacts/model'
# logged_model = f'runs:/{RUN_ID}/model'
model = mlflow.pyfunc.load_model(logged_model)


def prepare_features(ride):
    features = {}
    features['PU_DO'] = '%s_%s' % (ride['PULocationID'], ride['DOLocationID'])
    features['trip_distance'] = ride['trip_distance']
    return features


def predict(features):
    preds = model.predict(features)
    return float(preds[0])


app = Flask('duration-prediction')


@app.route('/predict', methods=['POST'])
def predict_endpoint():
    ride = request.get_json()

    features = prepare_features(ride)
    pred = predict(features)

    result = {
        'duration': pred,
        'model_version': RUN_ID
    }

    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=9696)

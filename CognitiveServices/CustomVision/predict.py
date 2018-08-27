import os
import sys

from azure.cognitiveservices.vision.customvision.training import training_api
from azure.cognitiveservices.vision.customvision.prediction import prediction_endpoint

CUSTOMVISION_TRAINING_KEY = ''      # Get training key from your Custom Vision portal.
CUSTOMVISION_PREDICTION_KEY = ''    # Get prediction key from your Custom Vision portal.
PROJECT_NAME = ''

def main():
    trainer = training_api.TrainingApi(CUSTOMVISION_TRAINING_KEY)
    predictor = prediction_endpoint.PredictionEndpoint(CUSTOMVISION_PREDICTION_KEY)

    # find project
    print('Get project {} info...'.format(PROJECT_NAME))
    project_id = None
    for p in trainer.get_projects():
        if p.name == PROJECT_NAME:
            project_id = p.id
            break

    if project_id is None:
        print('[ERROR] Project {} not found!'.format(PROJECT_NAME))
        return

    print('Predicting...')
    with open('test_image.jpg', 'rb') as image_data:
        results = predictor.predict_image(project_id, image_data.read())

    print('Result:')
    for prediction in results.predictions:
        print("{0}: {1:.2f}%".format(prediction.tag_name, prediction.probability * 100))


if __name__ == '__main__':
    main()
import os
from datetime import date
import time

from azure.cognitiveservices.vision.customvision import training

CUSTOMVISION_TRAINING_KEY = '' # Get training key from your Custom Vision portal.
PROJECT_NAME = 'my-customvision-{}'.format(date.today().strftime('%y%m%d'))
IMG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'images')

def main():
    trainer = training.training_api.TrainingApi(CUSTOMVISION_TRAINING_KEY)

    print('Creating project...{}'.format(PROJECT_NAME))
    project = trainer.create_project(PROJECT_NAME)

    print('Creating tags...')
    for subdir in os.listdir(IMG_DIR):
        print('Creating tag: {}...'.format(subdir))
        tag = trainer.create_tag(project.id, subdir)
        print ("> Adding images...")
        TAG_DIR = os.path.join(IMG_DIR, subdir)
        for image in os.listdir(TAG_DIR):
            with open(os.path.join(TAG_DIR, image), 'rb') as image_data:
                trainer.create_images_from_data(project.id, image_data.read(), [ tag.id ])
                print('> > Image: {} added.'.format(image))

    print ("Training...")
    iteration = trainer.train_project(project.id)
    while (iteration.status == "Training"):
        iteration = trainer.get_iteration(project.id, iteration.id)
        print ("Training status: " + iteration.status)
        time.sleep(1)

    # The iteration is now trained. Make it the default project endpoint
    trainer.update_iteration(project.id, iteration.id, is_default=True)
    print ("Done!")


if __name__ == '__main__':
    main()
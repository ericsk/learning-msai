import os.path

from azure.cognitiveservices.vision.computervision import ComputerVisionAPI
from azure.cognitiveservices.vision.computervision.models import VisualFeatureTypes
from msrest.authentication import CognitiveServicesCredentials

COMPUTER_VISION_API_LOCATION = 'southeastasia' # TODO: You might use 'westus' or 'southeastasia'
COMPUTER_VISION_API_KEY = '' # TODO: Get API key from Azure Portal.

def main():
    client = ComputerVisionAPI(COMPUTER_VISION_API_LOCATION, CognitiveServicesCredentials(COMPUTER_VISION_API_KEY))

    with open('image.jpg', 'rb') as image_stream:
        image_analysis = client.analyze_image_in_stream(
            image_stream,
            visual_features=[
                VisualFeatureTypes.tags,
                VisualFeatureTypes.description
            ]
        )
    
    print("Image description: {}".format(image_analysis.description.captions[0].text))
    print("Tags:\nTag\t\tConfidence")
    for tag in image_analysis.tags:
        print("{}\t\t{}".format(tag.name, tag.confidence))

if __name__ == '__main__':
    main()
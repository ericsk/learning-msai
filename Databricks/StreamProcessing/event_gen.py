import sys
import uuid
import random
import json
import time
import logging

from azure.eventhub import EventHubClient, Sender, EventData

DELAY = 1

ADDRESS = 'amqps://<NAMESPACE>.servicebus.windows.net/<HUBNAME>'
USER = ''
KEY = ''

def get_logger(level):
    azure_logger = logging.getLogger("azure")
    azure_logger.setLevel(level)
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s'))
    azure_logger.addHandler(handler)

    uamqp_logger = logging.getLogger("uamqp")
    uamqp_logger.setLevel(logging.INFO)
    uamqp_logger.addHandler(handler)
    return azure_logger

logger = get_logger(logging.INFO)

class FakeUser(object):
    platforms = ('ios','android')
    app_versions = ('1.0.0','1.1.0','1.2.1','1.3.0','2.0.0','2.0.1')
    games = ('minigame1','minigame2','minigame3','minigame4','minigame5','minigame6')
    packages = { 'cheap':0.99, 'mid' : 9.99, 'expensive' : 19.99 }
    client = EventHubClient(ADDRESS, debug=False, username=USER, password=KEY)
    sender = client.add_sender(partition="0")
    client.run()
  
    def __init__(self):
        self.app_name = 'Fake Application'
        self.platform = random.choice(self.platforms)
        self.app_version = random.choice(self.app_versions)
        self.game_keyword = random.choice(self.games)
        self.device_id = uuid.uuid4().hex
        self.install()

    def tick(self):
        if random.randint(1,10) == 1:
            self.change_game()
        if random.randint(1,100) == 1:
            self.purchase()
        else:
            self.playGame()
        if random.randint(1,10) == 1:
            self.__init__()
  
    def create_base_event(self):
        params = {
            'device_id' : self.device_id,
            'platform' : self.platform,
            'app_version' : self.app_version,
            'app_name' : self.app_name,
            'game_keyword' : self.game_keyword
        }
        return params
  
    def change_game(self):
        self.game_keyword = random.choice(self.games)

    def install(self):
        params = self.create_base_event()
        params['client_event_time'] = int(time.time())
        payload = {
            'eventName' : 'installEvent',
            'eventParams' : params
        }
        self.send_event(payload)
    
    def purchase(self):
        params = self.create_base_event()
        params['bundleId'] = random.choice(self.packages.keys())
        params['amount'] = self.packages[params['bundleId']]
        params['client_event_time'] = int(time.time())
        payload = {
            'eventName' : 'purchaseEvent',
            'eventParams' : params
        }
        self.send_event(payload)
  
    def playGame(self):
        params = self.create_base_event()
        params['game_keyword'] = self.game_keyword
        params['scoreAdjustment'] = random.randint(-1000,1000)
        params['client_event_time'] = int(time.time())
        payload = {
            'eventName': 'scoreAdjustment',
            'eventParams' : params
        }
        self.send_event(payload)
  
    def send_event(self, payload):
        data = json.dumps(payload)
        logger.info("Sending message: {}".format(data))
        self.sender.send(EventData(data))

user = FakeUser()
while True:
    user.tick()
    time.sleep(DELAY)
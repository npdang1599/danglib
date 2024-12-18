import requests
import danglib.configs as confs

# Notify to viber
class ViberBot:
    viber_members = dict()

    def __init__(self, viber_token):
        self.viber_token = viber_token
        self.get_viber_members()

    def get_viber_members(self):
        url = "https://chatapi.viber.com/pa/get_account_info"
        req = requests.get(url=url, headers={'X-Viber-Auth-Token': self.viber_token})
        data = req.json()
        members = data['members']
        self.viber_members = {i['name']:i['id'] for i in members}

    def send_viber(self, msg, member_ids=None):
        if member_ids is None:
            member_ids = ['OdXaNu3AJOThLDH57sHhAg==']
        try: 
            req = requests.post(
                url = "https://chatapi.viber.com/pa/broadcast_message",\
                headers={'X-Viber-Auth-Token':self.viber_token}, 
                json= {
                    "broadcast_list":member_ids,
                    "min_api_version":2,
                    "sender":{
                        "name":"F5Backup",
                    },
                    "type": "text",
                    "text": f"{confs.SERVER_NAME}: {msg}"
                })
        except:
            pass

# F5bot = ViberBot(viber_token="4fd90f053967df59-d02a6f0d532cf9f6-ec99a115cfcfd19")

def create_f5bot():
    return ViberBot(viber_token="4fd90f053967df59-d02a6f0d532cf9f6-ec99a115cfcfd19")
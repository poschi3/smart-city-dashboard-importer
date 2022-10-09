import json

def config():
    with open("config.json", 'r') as fin:
        return json.load(fin)

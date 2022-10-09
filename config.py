import json
from types import SimpleNamespace

with open("config.json", 'r') as fin:
    config = json.load(fin, object_hook=lambda d: SimpleNamespace(**d))

import json
from pathlib import Path


root_path = Path(__file__).parent.parent

config_file= str(root_path/"config"/"config.json")

print(config_file)

with open(config_file) as json_file:
    config = json.load(json_file)


path_timezones = str(Path(__file__).parent/ "resources"/"timezones.csv")

print(path_timezones)

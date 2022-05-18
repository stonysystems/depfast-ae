import yaml
import json

def convert_yml_json(name):
  with open(name, 'r') as file:
      configuration = yaml.safe_load(file)
  
  with open(name+".json", 'w+') as json_file:
      json.dump(configuration, json_file)

  return json.load(open(name+".json"))
  

if __name__ == "__main__":
    print(convert_yml_json("aaa.yml"))

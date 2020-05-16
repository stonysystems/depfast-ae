import json

serverMapping = {
        "andrew-server1:27017": "10.128.0.28",
        "andrew-server2:27017": "10.128.0.14",
        "andrew-server3:27017": "10.128.0.15",
    }

def main():
    with open('result.json', 'r') as f:
        my_dict = json.load(f)

    primary, secondary = "", ""
    for d in my_dict:
        if d['stateStr'] == "PRIMARY":
            primary = serverMapping[d["name"]]
        if d['stateStr'] == "SECONDARY":
            secondary = serverMapping[d["name"]]

    print "primary", primary
    print "secondary", secondary

if __name__== "__main__":
    main()

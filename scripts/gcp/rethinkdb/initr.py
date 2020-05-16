from rethinkdb import r

def init():
    r.connect('10.128.0.25', 28015).repl()
    # Connection established
    try:
        r.db('ycsb').table_drop('usertable').run()
    except Exception as e:
        print("Could not delete table")
    try:
        r.db_drop('ycsb').run()
    except Exception as e:
        print("Could not delete db")

    try:
       	r.db_create('ycsb').run()
       	r.db('ycsb').table_create('usertable', replicas=1,primary_key='__pk__').run()
    except Exception as e:
       	print("Could not create table")

    # Print the primary name
    b = list(r.db('rethinkdb').table('table_status').run())

    print("Primary Replica=", b[0]['shards'][0]['primary_replicas'])

    res = list(r.db('rethinkdb').table('server_status').run())
    pids = [(n['name'],n['process']['pid'],n['network']['cluster_port']) for n in res]
    print("Process Ids=", pids)

def main():
    # Initialising RethinkDB
    init()

if __name__== "__main__":
    main()


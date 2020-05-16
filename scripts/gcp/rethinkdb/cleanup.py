from rethinkdb import r

def init():
    r.connect('10.128.0.25', 28015).repl()
    # Connection established
    print("connected to 10.128.0.25: 28015")
    try:
        r.db('ycsb').table_drop('usertable').run()
    except Exception as e:
        print("Could not delete table")
    try:
        r.db_drop('ycsb').run()
    except Exception as e:
        print("Could not delete db")
    print("DB and table deleted")

def main():
    # Initialising RethinkDB
    print("Cleanup RethinkDB")
    init()

if __name__== "__main__":
    main()


import pymongo
import math

## creates Mongodb client connection

def connection():
    try:
        client = pymongo.MongoClient('REMOVED') # Removed for publication purposes
        print("Connection successful")
    except:
        print("Error connecting to database")
        
##    db = client['data']
##    collection = db['tokens']

    db = client['data_sandy'] # 'data'
    collection = db['tokens_sandy'] # 'tokens'
    
    return collection


def get_posting(collection, term):
    cursor = collection.find_one({"token": term})
    if cursor not in [None]:
        return dict(cursor)
    else:
        print("No documents contain " + term)


def get_df(collection, term):
    #https://docs.mongodb.com/manual/reference/method/db.collection.find/
    #https://stackoverflow.com/questions/38594616/python-query-with-mongo
    cursor = collection.find_one({"token": term})
    if cursor not in [None]:
        return len(cursor) - 2
    else:
        return 1


def get_tf(collection, term, name):
    n = name + ".freq"
    cursor = collection.find_one({"token": term}, {n})
    return cursor[name]["freq"]

    
def get_tf_idf(n, tf, df):
    return (1+math.log(tf)) * math.log(n/df)


def add_tf_idf(collection, term, name):
    n = name + ".tf-idf"
    file = 37497
    df = get_df(collection, term)
    tf = get_tf(collection, term, name)
    tf_idf = get_tf_idf(file, tf, df)
    collection.update_one({"token": term}, {"$set": {n: str(tf_idf)}})



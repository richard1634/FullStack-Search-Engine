import preprocess
import mongo
import os
from pymongo import UpdateOne
from operator import itemgetter
from collections import defaultdict
import math
import json


def create0(link):
    '''Populates mongoDB database with our index'''
    print("############### Starting storage #####################")

##    root = 'C:/Users/richa/Desktop/webpages/WEBPAGES_RAW'
    root = "C:/Users/spsan/OneDrive/Documents/UCI 2018-19/CS 121/Project 3/webpages/WEBPAGES_RAW"
    counter = 0
    bulk_update_list = []
    # https://www.tutorialspoint.com/python3/os_walk.htm
    for root, dirs, files in sorted(os.walk(root)):
        for f in files:
            if not f.endswith('.json') and not f.endswith('.tsv') and not f.endswith('.DS_Store'):
                path = os.path.join(root, f)
##                doc_id = path[33:] # Richard's path
                doc_id = path[85:] # Sandy's path
                tokenDict = preprocess.tokenizer(path)
                #========== insert here ========
                for token, freq in tokenDict.items():
                    append_list = {"token": token}
                    append_list2 = {"$set": {"posting" + str(counter): {"docID": doc_id, "freq": freq, "tf-idf": "None"}}}
                    bulk_update_list.append(append_list)
                    bulk_update_list.append(append_list2)
                    counter += 1
                    print(counter)
                    if (counter % 10 == 0):
                        link.bulk_write([
                            UpdateOne(bulk_update_list[0], bulk_update_list[1],True),
                            UpdateOne(bulk_update_list[2], bulk_update_list[3],True),
                            UpdateOne(bulk_update_list[4], bulk_update_list[5],True),
                            UpdateOne(bulk_update_list[6], bulk_update_list[7],True),
                            UpdateOne(bulk_update_list[8], bulk_update_list[9],True),
                            UpdateOne(bulk_update_list[10], bulk_update_list[11],True),
                            UpdateOne(bulk_update_list[12], bulk_update_list[13],True),
                            UpdateOne(bulk_update_list[14], bulk_update_list[15],True),
                            UpdateOne(bulk_update_list[16], bulk_update_list[17],True),
                            UpdateOne(bulk_update_list[18], bulk_update_list[19],True)
                            ])
                        bulk_update_list = []
                        
                #=========== end here==========

    print("############### Finished storage #####################")

def create1(link):
    '''Adds tf-idf for every term and document'''
    append_list = []
    append_list2 = []
    bulk_update_list= []
    counter = 0
    for document in link.find():
        term = document['token']
        for name in document:
            if 'posting' in name:
                n = name + ".tf-idf"
                file = 37497
                df = mongo.get_df(link, term)
                tf = mongo.get_tf(link, term, name)
                tf_idf = mongo.get_tf_idf(file, tf, df)
                
                append_list = {"token": term}
                append_list2 = {"$set": {n:str(tf_idf)}}
                
                bulk_update_list.append(append_list)
                bulk_update_list.append(append_list2)
                counter += 1
                if (counter % 10 == 0):
                    link.bulk_write([
                        UpdateOne(bulk_update_list[0], bulk_update_list[1],True),
                        UpdateOne(bulk_update_list[2], bulk_update_list[3],True),
                        UpdateOne(bulk_update_list[4], bulk_update_list[5],True),
                        UpdateOne(bulk_update_list[6], bulk_update_list[7],True),
                        UpdateOne(bulk_update_list[8], bulk_update_list[9],True),
                        UpdateOne(bulk_update_list[10], bulk_update_list[11],True),
                        UpdateOne(bulk_update_list[12], bulk_update_list[13],True),
                        UpdateOne(bulk_update_list[14], bulk_update_list[15],True),
                        UpdateOne(bulk_update_list[16], bulk_update_list[17],True),
                        UpdateOne(bulk_update_list[18], bulk_update_list[19],True)
                        ])
                    bulk_update_list = []
                    print('.')
                
    print("################### Finished tf-idf #################")


def query(link,term) -> list:
    '''Returns posting list for a single term query'''
    keys = set();
    result_list = []
    term = preprocess.stemList(term)
    posting = mongo.get_posting(link, term)

    for x in posting:
        if "posting" in x:
            keys.add(x)
    for k in keys:
        result_list.append(posting[k]) # appending docid,freq,tf-idf
    for x in result_list:
        str_tf_idf =  x['tf-idf']
        x['tf-idf'] = float(str_tf_idf) # turn tf-idf into a float
        
    return result_list

# Used for cosine similarity, but removed
##def wtf(term, input_query) -> float:
##    '''Calculates weighted term frequency for cosine similarity'''
##    return 1 + math.log(input_query.count(term) / len(input_query))


def ranked(link) -> None:
    '''Takes in single / multi-term query then returns top 10 ranked results'''
##    We tried cosine similarity but ended up with incorrect results.
##    We then just used unweighted tf-idf to calculate scores, but left the old
##    code commented above and below for reader's reference.
    
    input_query = str(input("Enter query: "))
    input_query = input_query.split(" ")

    termDict = {} # {term:[{docID:0, freq:0, tf-idf:0}]}
    scoreDict = defaultdict(float) # {docID: score}
    
    for term in input_query:
        termDict[term] = (query(link,term))

    for k,v in termDict.items():
        for i in v:
            scoreDict[i['docID']] += i['tf-idf']
# CSS impl
##            n = wtf(k,input_query)*(i['tf-idf']/(1+math.log(i['freq'])))*i['tf-idf']
##            scoreDict[i['docID']] += n/math.sqrt(i['tf-idf']**2)

# CSS impl
##    for k,v in scoreDict.items():
##        finalDict[k] = v/math.sqrt(magDict[k])
    
    ranked_results = sorted(scoreDict.keys(),key=lambda x:scoreDict[x], reverse=True)
    ranked_ten = ranked_results[:10]

##    print("DOC IDs AND SCORES")
##    for i in ranked_ten:
##        print(i, scoreDict[i], '\n')

    print("RESULTS")
    URL_list = getURL(ranked_ten)
    for i in range(len(URL_list)):
        print(i+1, URL_list[i])


# https://stackoverflow.com/questions/2835559/parsing-values-from-a-json-file
def getURL(ranked_ten) -> list:
    '''Reads bookkeeping.json to pull URLs from docIDs'''
    result_list = []

    with open('webpages/WEBPAGES_RAW/bookkeeping.json') as f:
        data = json.load(f)

    for i in ranked_ten:
        i = i.replace('\\', '/')
        result_list.append(data[i])

    return result_list


if __name__ == '__main__':
    link = mongo.connection()
    is_created = 1
    # 0 if index and idf not calculated
    # 1 otherwise
    
    if is_created == 0:
       create0(link) #makes index
       create1(link)#calculates idf
    
    ranked(link)

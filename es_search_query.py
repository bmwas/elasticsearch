"""
A small python function to index CSV data onto Elasticsearch.
This function also drops a text file with the same data but in JSON format.
This function will perform the following tasks
1) Load a CSV and convert column data into JSON format
2) Index data into an ElasticSearch Server. (NB: Assumes this server is using port 9200).
This function was used to index a list of optometrists from the Houston metropolitan area
This list was generated through a web crawler (www.importio.io)
Works with Elasticsearch 2.4.0.
@author: Benson Mwangi (benson.mwangi@gmail.com)
September 2016
"""
#Import relevant packages.
import json
import requests
from elasticsearch import Elasticsearch
import pandas as pd

## Load csv file and read using pandas.
pt="/home/Documents/" ## Change this accordingly!
df=pd.read_csv(pt+'Houston_optometrists.csv') 
es = Elasticsearch()
count=1;
#The csv contains business name, category, neighborhood, latitude, longitude and street name.
li=[]
for i in range(0,len(df.index)):
    doc = {'business':df['business'][i],'category':df['category'][i],'neighborhood':df['neighborhood'][i],'latitude':df['latitude'][i],'longitude':df['longitude'][i],'street':df['street'][i]}
    res = es.index(index="optobiz", doc_type='categories', id=count, body=doc)
    li.append({
           "business": df['business'][i],
           "category": df['category'][i],
           "neighborhood": df['neighborhood'][i],
           "latitude": df['latitude'][i],
           "longitude": df['longitude'][i],
           "street": df['street'][i],
        })
    print(res['created'],count)
    count=count+1
# Use json.dump to drop a json file with information gathered from the csv file.
json.dump(li,open('Houston_Optometrist_2.json','w'),indent=4,sort_keys=False)

## --------------END OF ELASTICSEARCH INDEXING---------------------------------------------------------
##------------ An example of how to Query indexed data!--------------------------------------------------------
## Assuming we would like to query any business with a word "Eyewear".

address='Eyewear'
es = Elasticsearch()
doc={
    "query" : {
        "match" : {"category" : json.dumps(address)}
      }
    }
res = es.search(index="optobiz", doc_type="categories", body=doc)
vendors = set([x["_source"]["business"] for x in res["hits"]["hits"]])
latitudes = list(([x["_source"]["latitude"] for x in res["hits"]["hits"]]))
longitudes = list(([x["_source"]["longitude"] for x in res["hits"]["hits"]]))
temp = {v: [] for v in vendors}
temp2= {v: [] for v in vendors}
itemss = {v: "" for v in vendors}
for r in res["hits"]["hits"]:
    applicant = r["_source"]["business"]
    if "business" in r["_source"]:
        bulk = {
                "business"    : r["_source"].get("business", "NA"),
                "category" : r["_source"].get("category", "NA"),
                "neighborhood"  : r["_source"].get("neighborhood", "NA"),
                "geocode"  : r["_source"].get("geocode", "NA"),
                "street" : r["_source"]["street"]}
        bgeocode = {"geocode"  : r["_source"].get("geocode", "NA")}
        itemss[applicant] = r["_source"]["neighborhood"]
        #Temp contains all queried 
        temp[applicant].append(bulk)
#Generate longitudes and latitudes fo the queried businesses. 
longitudes=[dc["_source"]["longitude"] for dc in res["hits"]["hits"]]
latitudes=[dc["_source"]["latitude"] for dc in res["hits"]["hits"]]



count=0
both=list()
for i in range(0,len(longitudes)):
    avc=['Test',latitudes[count],longitudes[count],count]
    both.append(avc)
    count=count+1

        
        

#A small function will input server address, categories and search terms.
def elastic_search(url,category, term):
    query = json.dumps({
        "query": {
            "match": {
                category: term
            }
        }
    })
    response = requests.get(url, data=query) ## Use http requests
    results = json.loads(response.text) 
    return results


from neo4j import GraphDatabase
import json
from tqdm import tqdm
import time
import gc
import os

uri = "neo4j://10.195.2.227:7687"
username = "neo4j"
password = "neo4j-connect"

def add_node(session, node):
    result = session.run("CREATE (a:Wiki $node)", node=node)
        
def add_edge(session, docid1, docid2, edge):
    result = session.run("MATCH (a:Wiki {docid: $docid1}) MATCH (b:Wiki {docid: $docid2}) MERGE (a)-[r:Link]->(b) SET r += $edge", docid1=docid1, docid2=docid2, edge=edge)

predicate_names = {}
def get_predicate_name(triple):
    uri = triple['predicate']['uri']
    prop_id = uri.split('/')[-1]
    if prop_id in predicate_names:
        return predicate_names[prop_id]['label'], predicate_names[prop_id]['description']
    
    url = f"https://www.wikidata.org/w/api.php?action=wbgetentities&ids={prop_id}&format=json"
    response = requests.get(url)
    try:
        label = response.json()['entities'][prop_id]['labels']['en']['value']
        description = response.json()['entities'][prop_id]['descriptions']['en']['value']
    except:
        label = triple['predicate']['surfaceform']
        description = ""
        
    predicate_names[prop_id] = {"label": label, "description": description}
    return label, description
    
#Expected number: 20877472
data_dir = "/app/T-Rex"
for file in os.listdir(data_dir):
    if '.json' in file:
        s_time = time.time()
        driver = GraphDatabase.driver(uri, auth=(username, password))
        data = json.load(open(f"{data_dir}/{file}", "r+"))
        
        with driver.session() as session:
            for d in tqdm(data):
                for t in d['triples']:
                    count += 1
                    try:
                        predicate_name, predicate_description = get_predicate_name(t)
                        entry = {
                            'predicate_surfaceform': t['predicate']['surfaceform'],
                            'predicate_uri': t['predicate']['uri'],
                            'predicate_name': predicate_name,
                            'predicate_description': predicate_description,
                            'object_surfaceform': t['object']['surfaceform'],
                            'subject_surfaceform': t['subject']['surfaceform'],  
                        }
                        add_edge(session, t['subject']['uri'], t['object']['uri'], entry)
                    except Exception as e:
                        print(e)
                        print(t)
                        print()

        print(f"Time for file {file} is {int(time.time() - s_time)}sec")
        del data
        gc.collect()

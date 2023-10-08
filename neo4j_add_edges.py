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
    edge_name = edge['predicate_name'].replace(' ', '_')
    result = session.run("MATCH (a:Wiki {docid: $docid1}) MATCH (b:Wiki {docid: $docid2}) MERGE (a)-[r:$edge_name]->(b) SET r += $edge", docid1=docid1, docid2=docid2, edge=edge, edge_name=edge_name)
    
predicate_names = {}
def get_predicate_name(uri):
    pred_id = uri.split('/')[-1]
    if pred_id in predicate_names:
        return predicate_names[pred_id]
    
    url = f"https://www.wikidata.org/w/api.php?action=wbgetentities&ids={prop_id}&format=json"
    response = requests.get(url)
    label = response.json()['entities'][prop_id]['labels']['en']['value']
    predicate_names[pred_id] = label
    return label
    
    
data_dir = "/app/T-Rex"
for file in os.listdir(data_dir):
    if '.json' in file:
        s_time = time.time()
        driver = GraphDatabase.driver(uri, auth=(username, password))
        data = json.load(open(f"{data_dir}/{file}", "r+"))
        
        with driver.session() as session:
            for d in tqdm(data):
                for t in d['triples']:
                    predicate_name = get_predicate_name(t['predicate']['uri'])
                    entry = {
                        'predicate_surfaceform': t['predicate']['surfaceform'],
                        'predicate_uri': t['predicate']['uri'],
                        'predicate_name': predicate_name
                        'object_surfaceform': t['object']['surfaceform'],
                        'subject_surfaceform': t['subject']['surfaceform'],  
                    }
                    add_edge(session, t['subject']['uri'], t['object']['uri'], entry)

        print(f"Time for file {file} is {int(time.time() - s_time)}sec")
        del data
        gc.collect()

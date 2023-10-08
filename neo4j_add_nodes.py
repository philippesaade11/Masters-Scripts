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
    result = session.run("MATCH (a:Wiki {docid: $docid1}) MATCH (b:Wiki {docid: $docid2}) MERGE (a)-[:$edge]->(b)", docid1=docid1, docid2=docid2, edge=edge)
    
#Expected number: 4645090
data_dir = "/app/T-Rex"
for file in os.listdir(data_dir):
    if '.json' in file and found:
        s_time = time.time()
        driver = GraphDatabase.driver(uri, auth=(username, password))
        data = json.load(open(f"{data_dir}/{file}", "r+"))
        
        with driver.session() as session:
            for d in tqdm(data):
                try:
                    entry = {
                        'docid': d['docid'],
                        'title': d['title'],
                        'text': d['text'],
                        'uri': d['uri']
                    }
                    add_node(session, entry)
                except Exception as e:
                    print(e)

        print(f"Time for file {file} is {int(time.time() - s_time)}sec")
        del data
        gc.collect()

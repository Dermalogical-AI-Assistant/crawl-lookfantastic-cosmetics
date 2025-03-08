from neo4j import GraphDatabase
import pandas as pd
import numpy as np
import json
from common import save_current_process, dir_fieldnames

# DB Neo4j Connection
uri = "bolt://localhost:7687" 
username = "neo4j"
password = "Ngoctram123"
driver = GraphDatabase.driver(uri, auth=(username, password))

def neo4j_write(func, params):
    with driver.session() as session:
        return session.write_transaction(func, *params)

def neo4j_read(func):
    with driver.session() as session:
        result = session.read_transaction(func)
        return result
    
def create_notable_relationship(tx, product_id, ingre_title, title, type):
    query = """
    MATCH (i:Ingredient {title: $ingre_title})
    MATCH (p:Product {id: $product_id})
    MERGE (p)-[:NOTABLE {title: $title, type: $type}]->(i)
    """
    tx.run(query, product_id=product_id, ingre_title=ingre_title, 
           title=title, type=type)

notable = pd.read_csv(r'D:\Study\DATN\KG_Cosmetics_Jasmine\data\ingredients\notable.csv', names=dir_fieldnames['notable'])
notable = notable.drop_duplicates()

# Create relationships to Product
for index, row in notable[1:].iterrows():
    save_current_process(str(index))
    
    product_id = row['product_id']
    title = row['title']
    type = row['type']
    list=row['list'].encode('utf-8').decode('unicode_escape')
    list = eval(list.replace("\\xad", "").replace("\\", "\\\\"))
    
    for ingr in list:
        ingre_title = ingr['title']    
        neo4j_write(create_notable_relationship, (
            product_id, ingre_title, title, type
        ))
    
# Close the driver
driver.close()
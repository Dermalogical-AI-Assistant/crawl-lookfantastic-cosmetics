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
    
def create_harmful_relationship(tx, product_id, ingre_title, harmful_title, harmful_type, harmful_description):
    query = """
    MATCH (i:Ingredient {title: $ingre_title})
    MATCH (p:Product {id: $product_id})
    MERGE (p)-[:HARMFUL {title: $harmful_title, type: $harmful_type, description: $harmful_description}]->(i)
    """
    tx.run(query, product_id=product_id, ingre_title=ingre_title, 
           harmful_title=harmful_title, harmful_type=harmful_type, harmful_description=harmful_description)

harmful = pd.read_csv(r'D:\Study\DATN\KG_Cosmetics_Jasmine\data\ingredients\harmful.csv', names=dir_fieldnames['harmful'])
harmful = harmful.drop_duplicates()

# Create relationships to Product
for index, row in harmful[6334:].iterrows():
    save_current_process(str(index))
    
    product_id = row['product_id']
    harmful_title = row['title']
    harmful_type = row['type']
    harmful_description = row['description']
    
    harmful_list=row['list'].encode('utf-8').decode('unicode_escape')
    # harmful_list = json.loads(harmful_list.replace("'", '"').replace("\\xad", "").replace('/', '//'))
    harmful_list = eval(harmful_list.replace("\\xad", "").replace("\\", "\\\\"))

    print(harmful_list)
    for ingr in harmful_list:
        ingre_title = ingr['title']    
        neo4j_write(create_harmful_relationship, (
            product_id, ingre_title, harmful_title, harmful_type, harmful_description
        ))
    
# Close the driver
driver.close()
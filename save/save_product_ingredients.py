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
    
def edit_product(tx, product_id, total_ingredients, product_ingredients_description):
    query = """
    MATCH (p:Product)
    WHERE p.id = $product_id
    SET p.total_ingredients = $total_ingredients,
        p.product_ingredients_description = $product_ingredients_description
    """
    tx.run(query, product_id=product_id, total_ingredients=total_ingredients, product_ingredients_description=product_ingredients_description)


product_ingredients = pd.read_csv(r'D:\Study\DATN\KG_Cosmetics_Jasmine\data\ingredients\product_ingredients.csv', names=dir_fieldnames['product_ingredients'])
product_ingredients = product_ingredients.drop_duplicates()
for index, row in product_ingredients.iterrows():
    save_current_process(str(index))
    
    product_id = row['product_id']
    total_ingredients = row['total_ingredients']
    product_ingredients_description = row['product_ingredients_description']
    # ewg = row['ewg']
    # natural = row['natural']
    print(product_id)
    print(total_ingredients)
    print(product_ingredients_description)
    print()
    
    neo4j_write(edit_product, (
        product_id, total_ingredients, product_ingredients_description
    ))
    
# Close the driver
driver.close()
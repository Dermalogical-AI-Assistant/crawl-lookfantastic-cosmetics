from neo4j import GraphDatabase
import pandas as pd
import numpy as np
from save.common import save_current_process

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
    
def create_product(tx, id, url, description, how_to_use, ingredient_benefits, preprocessed_ingredients):
    query = """
    MERGE (p:Product {id: $id})
    SET p.url = $url,
        p.description = $description,
        p.how_to_use = $how_to_use,
        p.ingredient_benefits = $ingredient_benefits,
        p.preprocessed_ingredients = $preprocessed_ingredients
    """
    tx.run(query, id=id, url=url, description=description, how_to_use=how_to_use, 
           ingredient_benefits=ingredient_benefits, preprocessed_ingredients=preprocessed_ingredients)

product_details = pd.read_csv(r'./data/product_details.csv')

for index, row in product_details.iterrows():
    save_current_process(str(index))
    id = row['id']
    url = row['page_url']
    description = row['description']
    how_to_use = row['how_to_use']
    ingredient_benefits = row['ingredient_benefits']
    preprocessed_ingredients = row['preprocessed_ingredients']
    
    neo4j_write(create_product, (
        id, url, description, how_to_use, ingredient_benefits, preprocessed_ingredients
    ))
    
    
# Close the driver
driver.close()
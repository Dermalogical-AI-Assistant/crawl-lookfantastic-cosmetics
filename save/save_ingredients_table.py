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
    
def create_ingredient(tx, title, preprocessed_introtext, preprocessed_ewg_ingre, cir_rating, categories, properties):
    query = """
    MERGE (p:Ingredient {title: $title})
    SET p.preprocessed_introtext = $preprocessed_introtext,
        p.preprocessed_ewg_ingre = $preprocessed_ewg_ingre,
        p.cir_rating = $cir_rating,
        p.categories = $categories,
        p.properties = $properties
    """
    tx.run(query, title=title, preprocessed_introtext=preprocessed_introtext, 
           preprocessed_ewg_ingre=preprocessed_ewg_ingre, cir_rating=cir_rating, 
           categories=categories, properties=properties)

def create_ingredient_to_product_relationship(tx, product_id, ingre_title):
    query = """
    MATCH (i:Ingredient {title: $ingre_title})
    MATCH (p:Product {id: $product_id})
    MERGE (p)-[:HAS]->(i)
    """
    tx.run(query, product_id=product_id, ingre_title=ingre_title)

ingredients_table = pd.read_csv(r'D:\Study\DATN\KG_Cosmetics_Jasmine\data\ingredients\ingredients_table.csv')
ingredients_table = ingredients_table.drop_duplicates()

# Create nodes of node_type Ingredient
# list_ingredients = ingredients_table.drop(columns=['product_id']).drop_duplicates()

# for index, row in list_ingredients[26:].iterrows():
#     save_current_process(str(index))
    
#     title = row['title']
#     preprocessed_introtext = row['preprocessed_introtext']
#     preprocessed_ewg_ingre = row['preprocessed_ewg_ingre']
#     cir_rating = row['cir_rating']
#     categories = row['categories']
#     properties = row['properties']
    
#     neo4j_write(create_ingredient, (
#         title, preprocessed_introtext, preprocessed_ewg_ingre, cir_rating, categories, properties
#     ))
    
# Create relationships to Product
for index, row in ingredients_table[1:].iterrows():
    save_current_process(str(index))
    product_id = row['product_id']
    ingre_title = row['title']
    neo4j_write(create_ingredient_to_product_relationship, (product_id, ingre_title)   )
    
# Close the driver
driver.close()
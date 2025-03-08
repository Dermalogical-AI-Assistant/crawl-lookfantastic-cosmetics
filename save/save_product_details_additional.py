from neo4j import GraphDatabase
import pandas as pd
import numpy as np
from common import save_current_process

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
    
def edit_product(tx, id, img, title, price, skincare_concern):
    query = """
    MERGE (p:Product {id: $id})
    SET p.img = CASE WHEN p.img IS NULL THEN $img ELSE p.img END,
        p.title = CASE WHEN p.title IS NULL THEN $title ELSE p.title END,
        p.price = CASE WHEN p.price IS NULL THEN $price ELSE p.price END,
        p.skincare_concern = COALESCE(p.skincare_concern, []) + $skincare_concern
    """
    tx.run(query, id=id, img=img, title=title, price=price, skincare_concern=skincare_concern)

df_products = pd.read_csv(r'./data/df_products.csv')
print(len(df_products))
# for index, row in df_products.iterrows():
#     save_current_process(str(index))
    
#     id = row['id']
#     img = row['img']
#     title = row['title']
#     price = row['price']
#     skincare_concern = row['skincare_concern']
    
#     print(id)
#     print(img)
#     print(title)
#     print(price)
#     print(skincare_concern)
    
#     neo4j_write(edit_product, (
#         id, img, title, price, skincare_concern
#     ))
    
# # Close the driver
# driver.close()
import requests
import pandas as pd
import numpy as np
import json
import csv
from utils import save_current_process

product_details = pd.read_csv(r'D:\Study\DATN\KG_Cosmetics_Jasmine\data\product_details.csv')
product_details
COSMILY_ANALYZE_URL = 'https://api.cosmily.com/api/v1/analyze/ingredient_list'

dir_fieldnames = {
    'product_ingredients': ['product_id', 'total_ingredients', 'description', 'ewg', 'natural'],
    'ingredients_table': ['product_id', 'title', 'introtext', 'cir_rating', 'categories', 'ewg_ingre', 'properties'],
    'harmful': ['product_id', 'type', 'title', 'description', 'list', 'slug'],
    'positive': ['product_id', 'type', 'title', 'description', 'list'],
    'notable': ['product_id', 'type', 'title', 'list'],
}

def write_csv(file_name, fieldnames, data):
    with open(f'./data/ingredients/{file_name}.csv', mode='a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writerow(data)

for i, row in product_details[9:].iterrows():
    product_id = row['id']
    list_ingre = row['preprocessed_ingredients']
    
    if isinstance(list_ingre, str) == False or len(list_ingre) == 0:
        continue
    
    data = {
        'ingredients': list_ingre
    }
    result = requests.post(COSMILY_ANALYZE_URL, data=data)
    result = result.json()
    result = result['analysis']
    
    # total_ingredients, desc
    total_ingredients = result['total_ingredients']
    description = result['description']
    
    # ewg, natural
    ewg_product = result['ewg']
    natural = result['natural']
    
    data = {
        'product_id': product_id, 
        'total_ingredients': total_ingredients,
        'description': description,
        'ewg': ewg_product,
        'natural': natural,
    }
    write_csv(file_name='product_ingredients', fieldnames=dir_fieldnames['product_ingredients'], data=data)
    
    # ingredients_table
    ingredients_table = result['ingredients_table']

    for ingre in ingredients_table:
        if ingre['alias'] is None:
            continue
        
        title = ingre.get('title')
        introtext = ingre.get('introtext')
        cir_rating = ingre.get('cir_rating')
        categories = ingre.get('categories')
        ewg_ingre = ingre.get('ewg')
        properties = [key for key, value in ingre.get('boolean_properties').items() if value is True]
        
        data = {
            'product_id': product_id, 
            'title': title,
            'introtext': introtext,
            'cir_rating': cir_rating,
            'categories': categories,
            'ewg_ingre': ewg_ingre,
            'properties': properties,
        }
        write_csv(file_name='ingredients_table', fieldnames=dir_fieldnames['ingredients_table'], data=data)

    # harmful
    harmful = result['harmful']
    
    for key, value in harmful.items():
        harmful_type = key
        harmful_title = value['title']
        harmful_description = value['description']
        harmful_list = value['list']
        harmful_slug = value['slug']
        
        data = {
            'product_id': product_id, 
            'type': harmful_type,
            'title': harmful_title,
            'description': harmful_description,
            'list': harmful_list,
            'slug': harmful_slug
        }
        write_csv(file_name='harmful', fieldnames=dir_fieldnames['harmful'], data=data)

    
    # positive
    positive = result['positive']    
    for key, value in positive.items():
        positive_type = key
        positive_title = value['title']
        positive_description = value['description']
        positive_list = value['list']
        
        data = {
            'product_id': product_id, 
            'type': positive_type, 
            'title': positive_title,
            'description': positive_description,
            'list': positive_list
        }
        write_csv(file_name='positive', fieldnames=dir_fieldnames['positive'], data=data)
        
    # notable
    notable = result['notable']    
    for key, value in notable.items():
        notable_count = value['count']
        if notable_count > 0:
            notable_type = key
            notable_title = value['title']
            notable_list = value['list']
            
            data = {
                'product_id': product_id, 
                'type': notable_type, 
                'title': notable_title,
                'list': notable_list
            }
            write_csv(file_name='notable', fieldnames=dir_fieldnames['notable'], data=data)
            
    save_current_process(f'{i}')

def save_current_process(data):
    with open('./data/process.txt', 'w') as file:
        file.write(data)
    print(f'Done {data}')
    
dir_fieldnames = {
    'product_ingredients': ['product_id', 'total_ingredients', 'product_ingredients_description', 'ewg', 'natural'],
    'ingredients_table': ['product_id', 'title', 'introtext', 'cir_rating', 'categories', 'ewg_ingre', 'properties', 'preprocessed_introtext', 'preprocessed_ewg_ingre'],
    'harmful': ['product_id', 'type', 'title', 'description', 'list', 'slug'],
    'positive': ['product_id', 'type', 'title', 'description', 'list'],
    'notable': ['product_id', 'type', 'title', 'list'],
}
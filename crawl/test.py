import pandas as pd

product_list = pd.read_csv('./data/product_list.csv', names = ['img', 'title', 'price', 'url', 'skincare_concern'])
product_list = product_list.drop_duplicates()
all_urls = list(product_list['url'].unique())
print(len(all_urls))
import requests
from bs4 import BeautifulSoup
import csv
import json
from utils import save_current_process
    
def get_list_products(page_soup, skincare_concern):
    list_products = page_soup.find_all('product-card-wrapper', {'data-e2e': lambda x: x and x.startswith('search_list-item')}) # len=32
    for product in list_products:
        img_element = product.find('picture').find('img') 
        
        if img_element:
            img = img_element.get('src')
        else:
            img = None
            
        title = product.find('a', {'class': 'product-item-title'}).text.strip()
        url = product.find('a', {'class': 'product-item-title'}).get('href')
        price = product.find('p', {'class': 'price'}).text.strip()
        
        product = {
            'img' : img,
            'title': title,
            'price': price,
            'url': url,
            'skincare_concern': skincare_concern
        }
        
        with open('./data/product_list.csv', mode='a', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=['img', 'title', 'price', 'url', 'skincare_concern'])
            writer.writerow(product)
            
def get_list_skincare_concern_urls():
    # get URLs of list products from LOOKFANTASTIC.COM based on skin concerns
    with open('./data/list_skincare_concerns.txt', 'r') as file:
        list_skincare_concern_urls = file.readlines()
        
    list_skincare_concern_urls = [skin_concern_url.replace('\n', '') for skin_concern_url in  list_skincare_concern_urls]
    return list_skincare_concern_urls

def crawl_a_page(skincare_concern_url, skincare_concern, crawl_first_page=True):
    if crawl_first_page:
        # fetch page of list of skincare products 
        first_page = requests.get(skincare_concern_url)
        first_page = first_page.content
        first_page = BeautifulSoup(first_page, 'html.parser') 
                
        get_list_products(page_soup=first_page, skincare_concern=skincare_concern)
        save_current_process(url=skincare_concern_url)
        
        # get max_page_number
        pagination_wrapper = first_page.find('pagination-wrapper').find('span').text # e.g. 'Page 1 of 30'
        max_page_number = int(pagination_wrapper.split()[-1])
        print(max_page_number)
        
        # save url and max_page_number of url
        with open('./data/page_info.txt', 'w') as file:
            json.dump({
                'skincare_concern_url': skincare_concern_url,
                'max_page_number': max_page_number
            }, file, indent=4)
    else:
        with open('./data/page_info.txt', 'r') as file:
            page_info = json.loads(file)
        max_page_number = int(page_info['max_page_number'])
    
    for page_number in range(2, max_page_number + 1):
        page_url = f'{skincare_concern_url}?pageNumber={page_number}'
        page = requests.get(page_url)
        page = page.content
        page = BeautifulSoup(page, 'html.parser')

        save_current_process(url=page_url)
        
        get_list_products(page_soup=page, skincare_concern=skincare_concern)

#=================================BAT DAU THOYYYY=========================================#
list_skincare_concern_urls = get_list_skincare_concern_urls()

for skincare_concern_url in list_skincare_concern_urls[2:]:
    skincare_concern = skincare_concern_url.split('/')[-2]
    crawl_a_page(skincare_concern_url=skincare_concern_url, skincare_concern=skincare_concern)
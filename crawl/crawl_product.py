import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
import csv
from utils import save_current_process

# drop duplicates
product_list = pd.read_csv('./data/product_list.csv', names = ['img', 'title', 'price', 'url', 'skincare_concern'])
product_list = product_list.drop_duplicates()
product_list.to_csv('./data/product_list.csv', index=False, header=False)

def get_component_need_scrolling(selenium_driver, data_tracking_push, aria_labelledby):
    try:
        button = selenium_driver.find_element(By.CSS_SELECTOR, f'[data-tracking-push="{data_tracking_push}"]')
        
        # scroll before clicking
        selenium_driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
        time.sleep(1) 
        
        # wait for the button to be clickable
        WebDriverWait(selenium_driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, f'[data-tracking-push="{data_tracking_push}"]'))
        )
        
        # click
        selenium_driver.execute_script("arguments[0].click();", button)

        # after clicking, get that element
        component_text = selenium_driver.find_element(By.CSS_SELECTOR, f'[aria-labelledby="{aria_labelledby}"]').text
        return component_text
    except NoSuchElementException:
        return None

all_urls = list(product_list['url'].unique())
index = all_urls.index('https://lookfantastic.com/p/avant-skincare-anti-ageing-glycolic-lifting-face-and-neck-mask-50ml/11592214/')
print(f'index = {index}')
for page_url in all_urls[index:]:
    selenium_driver = webdriver.Chrome()
    selenium_driver.get(page_url)
    print(f'\n{page_url}')
    
    save_current_process(url=page_url)
    
    try:
        description=selenium_driver.find_element(By.CSS_SELECTOR, '[aria-labelledby="Description"]').text
    except NoSuchElementException:
        description=None
    
    try:
        how_to_use = get_component_need_scrolling(
            selenium_driver=selenium_driver, 
            data_tracking_push="How to Use",
            aria_labelledby="How-to-Use"
        )
        
        ingredient_benefits = get_component_need_scrolling(
            selenium_driver=selenium_driver, 
            data_tracking_push="Ingredient Benefits",
            aria_labelledby="Ingredient-Benefits"
        )
        
        full_igredients_list = get_component_need_scrolling(
            selenium_driver=selenium_driver,
            data_tracking_push="Full Ingredients List",
            aria_labelledby="Full-Ingredients-List"
        )
        
        data = {
            'page_url': page_url,
            'description': description,
            'how_to_use': how_to_use,
            'ingredient_benefits': ingredient_benefits,
            'full_igredients_list': full_igredients_list
        }
        
        with open('./data/product_details.csv', mode='a', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=['page_url', 'description', 'how_to_use', 'ingredient_benefits', 'full_igredients_list'])
            writer.writerow(data)
        
    finally:
        # 4. Close the browser
        selenium_driver.quit()
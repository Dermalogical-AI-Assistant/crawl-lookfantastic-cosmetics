def save_current_process(url):
    with open('./data/process.txt', 'w') as file:
        file.write(url)
    print(f'Done {url}')
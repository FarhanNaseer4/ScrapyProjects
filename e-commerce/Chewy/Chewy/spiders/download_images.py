import csv
import os
import re

import requests


def read_csv():
    try:
        with open('input/chewy.csv', encoding='utf-8', errors='ignore') as csv_file:
            return list(csv.DictReader(csv_file))
    except Exception as ex:
        print(f'Error While Reading File {ex}')


def Folder_Manager():
    csv_data = read_csv()
    images_path = ''
    parent_sku = ''
    product_name = ''
    for data in csv_data:
        categories = data.get('Categories', '')
        cate_list = categories.split('>')
        sku_ = data.get('SKU_2', '')
        child_parent = data.get('Parent_2', '')
        type_ = data.get('Type', '')
        if 'Variable' in type_:
            if any(cate_list):
                start_folder_path = "images_folder_path"
                parent_sku = sku_
                product_name = data.get('Name', '')
                for cate in cate_list:
                    exist = check_folder_exists(cate, start_folder_path)
                    if not exist:
                        create_new_folder(cate, start_folder_path)
                        start_folder_path = os.path.join(start_folder_path, cate)
                    else:
                        start_folder_path = os.path.join(start_folder_path, cate)
                images_path = start_folder_path
        else:
            if parent_sku == child_parent:
                create_new_folder(product_name, images_path)
                images_path = os.path.join(images_path, product_name)
                variation_name = remove_special_characters(data.get('Name', ''))
                attr_1 = data.get('Attribute 1 name')
                attr_2 = data.get('Attribute 2 name')
                create_new_folder(variation_name, images_path)
                image_new_path = os.path.join(images_path, variation_name)
                images = data.get('Images', '')
                get_url(images, image_new_path, variation_name + '_' + attr_1 + '_' + attr_2)


def create_new_folder(folder_name, start_folder_path):
    try:
        new_folder_path = os.path.join(start_folder_path, folder_name)
        os.makedirs(new_folder_path)
    except Exception as ex:
        print(f'Error While Creating Folder {ex}')


def check_folder_exists(folder_name, start_folder_path):
    try:
        new_folder_path = os.path.join(start_folder_path, folder_name)
        if os.path.exists(new_folder_path):
            check = True
        else:
            check = False
        return check
    except Exception as ex:
        print(f'Error While Checking Exists Of Folder {ex}')


def download_image(url, file_path, name):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            with open(file_path, 'wb') as file:
                file.write(response.content)
            print(f"{name} Image downloaded successfully.")
        else:
            print("Failed to download the image. Status code:", response.status_code)
    except requests.exceptions.RequestException as e:
        print("An error occurred While downloading:", e)


def get_url(urls, new_path, name):
    try:
        i = 1
        clean_name = remove_special_characters(name)
        for url in urls.split(' | '):
            image_url = url.replace(' ', '').strip()
            file_name = f'{clean_name}_' + str(i)
            save_path = f"{new_path}/{file_name}.jpg"
            download_image(image_url, save_path, file_name)
            i = i + 1
    except Exception as ex:
        print(f'Error While Creating Image Urls {ex}')


def remove_special_characters(string):
    pattern = r'[^a-zA-Z0-9\s]'
    cleaned_string = re.sub(pattern, '_', string)

    return cleaned_string


if __name__ == '__main__':
    Folder_Manager()

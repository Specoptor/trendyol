import pandas as pd
import requests
import xml.etree.ElementTree as ET
import re
import json
import logging

product_pattern = re.compile(r'p-(\d+)')


def get_links():
    """
    Get the Product links from the sitemap.
    Include only english based urls.
    :return: a list of containing product links.
    """
    links = []
    for counter in range(1, 4):
        target_link = f'https://www.trendyol.com/en/sitemap_products{counter}.xml'
        response = requests.get(target_link)
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            for child in root:
                links.append(child[0].text)
    logging.info(f'extracted {len(links)} links from sitemap')
    return links


def get_product_id(link: str) -> str:
    """
    Extract the product id from the link
    :param link:
    :return:
    """

    match = product_pattern.search(link)
    if match:
        return match.group(1)
    else:
        return None


def grab_product_data(product_id: str) -> dict:
    """
    Grab the product data from the product id by calling the v2 content api
    :param product_id:
    :return:
    """
    API_URL = (f'https://public-mdc.trendyol.com/discovery-sfint-productgw-service/api/product-detail'
               f'/getProductDetailContentV2?contentId={product_id}&culture=en-GB')
    response = requests.get(API_URL, headers={
        'cookie': 'storefrontId=34; countryCode=GB; language=en;'
    })

    if response.status_code == 200:
        logging.info(f'successfully downloaded the title: {response.json["name"]}')
        return response.json()
    else:
        logging.info(f'status code: {response.status_code}, url: {API_URL}', 'product_id: ', product_id)
        return None


def get_product_data_from_link(link: str) -> dict:
    """
    Grab the product data from the link
    :param link:
    :return:
    """
    product_id = get_product_id(link)
    if product_id:
        return grab_product_data(product_id)
    else:
        return None


def required_product_data(response_dict, link):
    """
    Extract the required product data from the response dict
    :param response_dict:
    :param link:
    :return:
    """
    required_data = {}
    required_data['url'] = link
    required_data['description'] = response_dict.get('brand', {}).get('description')
    required_data['images'] = response_dict['images']
    required_data['name'] = response_dict['name']
    required_data['brand_name'] = response_dict.get('brand', {}).get('name')
    required_data['barcode'] = response_dict['allVariants'][0]['barcode']
    required_data['price'] = response_dict['allVariants'][0]['price']

    return required_data


def run_scraper() -> list[dict]:
    """ download product data
    1) Grab urls from sitemap
    2) Parse each url to get the product id
    3) Use the product id to fetch product response from the v2 content api
    4) Save the content response to a json file
    :return:
    """
    links = get_links()
    product_data = []
    try:
        for i, link in enumerate(links):
            data_dict = {'link': link}
            data_dict['product_id'] = get_product_id(link)
            data_dict['response'] = get_product_data_from_link(link)
            product_data.append(data_dict)
            logging.info(f' {i} out of {len(links)} completed')
    except Exception as e:
        print(e)
    finally:
        with open('trendyol_products.json', 'w') as f:
            json.dump(product_data, f)
    return product_data


def aggregate_product_data(response_dicts_list: list) -> list:
    """
    Aggregate the product data from the responses
    :param responses:
    :return:
    """
    results = []
    for data in response_dicts_list:
        results.append(required_product_data(data['response'], data['link']))


def aggregrate_product_data_from_json_list(fp: str):
    """
    Aggregate the product data from the responses
    :param responses:
    :return:
    """
    results = []
    with open(fp, 'r') as f:
        data = json.load(f)
        for product in data:
            if product['response']:
                results.append(required_product_data(product['response'], product['link']))
    return results


if __name__ == '__main__':
    # products = run_scraper()
    # req_data = aggregate_product_data(products)
    #
    # with open('trendyol_products_required_data.json', 'w') as f:
    #     json.dump(req_data, f, indent=4)

    ################# from json #################

    req_data = aggregrate_product_data_from_json_list('trendyol_products.json')

    ################ to csv #####################
    df = pd.DataFrame(req_data)
    df.to_csv('trendyol_products_required_data.csv')
    print

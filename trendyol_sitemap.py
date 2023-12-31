import pandas as pd
import requests
import xml.etree.ElementTree as ET
import re
import json
import logging

product_pattern = re.compile(r'p-(\d+)')

logging.basicConfig(filename='trendyol.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

logging.info('starting the scraper')
def get_links():
    """
    Get the Product links from the sitemap.
    Include only english based urls.
    :return: a list of containing product links.
    """
    links = []
    for counter in range(1, 7):
        target_link = f'https://www.trendyol.com/de/sitemap_products{counter}.xml'
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
        if response.json():
            logging.info(f'successfully downloaded the title: {response.json()["name"]}')
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
    required_data['in_stock'] = response_dict.get('inStock')
    if required_data['in_stock'] and response_dict.get('allVariants'):
        required_data['barcode'] = response_dict['allVariants'][0]['barcode']
        required_data['price'] = response_dict['allVariants'][0]['price']
        required_data['size'] = response_dict['allVariants'][0].get('size') or response_dict['allVariants'][0].get(
            'value')

    return required_data


def aggregate_product_data(response_dicts_list: list) -> list:
    """
    Aggregate the product data from the responses
    :param response_dicts_list:
    :param responses:
    :return:
    """
    results = []
    try:
        for data in response_dicts_list:
            if data['response']:
                results.append(required_product_data(data['response'], data['link']))
    except Exception as e:
        print(e)
    finally:
        df = pd.DataFrame(results)
        df.to_csv('trendyol_products_required_data_de.csv')
    return results


def run_scraper() -> list[dict]:
    """ download product data

    1) Grab urls from sitemap
    2) Parse each url to get the product id
    3) Use the product id to fetch product response from the v2 content api
    4) Once all products are downloaded, generate required_data for each by parsing the response
    4) Save the required_data products response to a json file
    5) Save the required_data as a csv by instantiating a pandas dataframe
    :return:
    """
    links = get_links()
    product_data = []
    try:
        for i, link in enumerate(links):
            data_dict = {
                'link': link,
                'product_id': get_product_id(link),
                'response': get_product_data_from_link(link)}
            product_data.append(data_dict)
            logging.info(f' {i} out of {len(links)} completed')
    except Exception as e:
        print(e)
    finally:
        with open('trendyol_products_de.json', 'w') as f:
            json.dump(product_data, f)
            aggregate_product_data(product_data)

    return product_data


if __name__ == '__main__':
    # products = run_scraper()
    # req_data = aggregate_product_data(products)
    #
    # with open('trendyol_products_required_data.json', 'w') as f:
    #     json.dump(req_data, f, indent=4)

    ################# from json #################
    req_data = run_scraper()
    print(len(req_data))
    #
    # ################ to csv #####################
    # df = pd.DataFrame(req_data)
    # df.to_csv('trendyol_products_required_data.csv')
    # print

"""
Get the brand and title of the product
"""
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.common.by import By
import threading

import json
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_brand_and_title(driver):
    """
    Get the brand and title of the product by first finding the brand and title element using the class name.
    :param driver: driver with the product page loaded.
    :return: a string containing the brand and title of the product.
    """
    try:
        brand_and_title_element = driver.find_element(By.CLASS_NAME, 'pr-new-br')
        brand_and_title = brand_and_title_element.text
        return brand_and_title
    except:
        logging.critical(f'Error getting brand and title: {driver.current_url}')
        return None


# %%
def get_price(driver):
    """
    Get the price of the product by first finding the price element using the class name
     and then getting the text of the element.
    :param driver: driver with the product page loaded.
    :return: a string containing the price of the product.
    """
    try:
        price_element = driver.find_element(By.CLASS_NAME, 'prc-dsc')
        price = price_element.text
        return price
    except:
        logging.debug(f'Price not found: {driver.current_url}')
        return None


# %%
def get_attributes(driver):
    """
    Get the attributes of the product by first finding the attributes list element
    and then splitting the text and then pairing adjacent tokens as key-value pairs.
    :param driver: driver with the product page loaded.
    :return: a dictionary containing the attributes of the product.
    """
    attributes = {}
    try:
        attributes_list_element = driver.find_element(By.CLASS_NAME, 'detail-attr-container')
        for li in attributes_list_element.find_elements(By.TAG_NAME, 'li'):
            key, value = li.text.split('\n')
            attributes[key] = value
    except:
        logging.debug(f'Attributes not found: {driver.current_url}')
        pass
    finally:
        return attributes


def get_images(driver):
    images = []
    try:
        main_image = driver.find_element(By.CLASS_NAME, 'base-product-image').find_element(By.TAG_NAME,
                                                                                           'img').get_attribute('src')
        images.append(main_image)
        for other_image in driver.find_element(By.CLASS_NAME, 'styles-module_slider__o0fqa').find_elements(By.TAG_NAME,
                                                                                                           'img'):
            images.append(other_image.get_attribute('src'))
    except:
        if not images:
            logging.debug(f'No images found: {driver.current_url}')
        else:
            logging.debug(f'Only main image found: {driver.current_url}')
    finally:
        return images


def get_description(driver):
    """
    Get the description of the product by first finding the description list element and then iterating over the list items.
    Initialize an empty string and append the text of each list item to the string using a new line seperator.
    :param driver: driver with the product page loaded.
    :return: a string containing the description of the product.
    """
    description = None
    try:
        description_list_element = driver.find_element(By.CLASS_NAME, 'detail-desc-list')
        description = description_list_element.text
    except:
        logging.debug(f'Description not found: {driver.current_url}')
    finally:
        return description


def split_urls(urls, num_splits):
    avg = len(urls) // num_splits
    chunks = [urls[i:i + avg] for i in range(0, len(urls), avg)]
    return chunks


def multi_threaded_scraper(links, num_threads=10):
    """
    Scrape the product details from the product pages using multiple threads.
    :param links: list of product page urls to scrape.
    :return: a list of dictionaries containing the product details.
    """

    def worker_task(worker, url_queue):
        results = {}
        while not url_queue.empty():
            try:
                url = url_queue.get_nowait()
            except queue.Empty:
                break
            try:
                data = worker.scrape(url)
                results[url] = data
            except Exception as exc:
                print(f'URL {url} generated an exception: {exc}')
        return results

    url_queue = queue.Queue()
    for url in links:
        url_queue.put(url)

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Initialize workers
        workers = [Worker() for _ in range(num_threads)]

        # Associate each URL with a worker
        futures = [executor.submit(worker_task, worker, url_queue) for worker in workers]
        all_results = {}
        try:
            for prod_data in as_completed(futures):
                try:
                    data = prod_data.result()
                except:
                    pass
                else:
                    all_results.update(data)
        except:
            logging.critical('Error in scraper')
        finally:
            # ensure results found
            if not all_results:
                logging.critical('No data scraped')

            # store the required data
            else:
                with open('required_product_data_turkish.json', 'w') as f:
                    json.dump(all_results, f, indent=4)
                    logging.info('Required data stored')

            for worker in workers:
                worker.close()

            return all_results


class Worker:
    def __init__(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        self.driver = webdriver.Chrome(options=options)  # or any other browser

    def scrape(self, url):
        self.driver.get(url)
        product_details = {'link': url,
                           'brand_and_title': None,
                           'price': None,
                           'attributes': None,
                           'description': None,
                           'images': None,
                           }

        if self.driver.title == 'trendyol.com':
            logging.info(f'Product does not exit anymore: {url}')
            return product_details

        product_details['brand_and_title'] = get_brand_and_title(self.driver)
        product_details['price'] = get_price(self.driver)
        product_details['attributes'] = get_attributes(self.driver)
        product_details['description'] = get_description(self.driver)
        product_details['images'] = get_images(self.driver)
        logging.info(f'Extracted product details: {url}')

        return product_details

    def close(self):
        self.driver.quit()


if __name__ == '__main__':
    # load links
    with open(os.path.join(os.getcwd(), 'turkish_product_links.json'), 'r') as f:
        urls = json.load(f)
    required_data_response = multi_threaded_scraper(urls[:100])
    print(len(required_data_response))
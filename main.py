import pickle
import re

import requests
from requests.exceptions import MissingSchema, ConnectTimeout
from urlextract import URLExtract

from loguru import logger


def urls_handling(urls: list):

    logger.info('Start of work urls_handling.')
    urls_status_code = {}
    urls_unshorted = {}

    for url in urls:

        valid_url = re.sub(r'(\')(,)', r'', str(url))

        try:

            url_info = requests.head(valid_url, timeout=2)

            urls_status_code[valid_url] = url_info.status_code
            urls_unshorted[valid_url] = url_info.headers.get("Location")

            if urls_unshorted[valid_url] is None:
                urls_unshorted[valid_url] = 'This url is already full or unavailable.'

        except (MissingSchema, ConnectTimeout):
            logger.exception('Something went wrong!')
            continue

    logger.info('The formation of dictionaries is completed.')
    return urls_status_code, urls_unshorted


def data_parsing(file_to_parse: str):

    extractor = URLExtract()

    with open(file_to_parse, 'rb') as file:

        logger.info('file_to_parse is open')
        data = pickle.load(file)

        extracted_urls = extractor.find_urls(str(data))

        urls_status_code, urls_unshorted = urls_handling(extracted_urls)
        logger.info('file_to_parse is closed')

    return urls_status_code, urls_unshorted


if __name__ == '__main__':

    logger.add('logs.log', rotation='5min')

    completed_urls_status_code, completed_urls_unshorted = data_parsing('messages_to_parse.dat')

    logger.add('logs.log', retention='20min')

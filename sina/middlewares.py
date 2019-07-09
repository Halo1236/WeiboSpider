# encoding: utf-8
import random

import pymongo
import requests

from sina.settings import LOCAL_MONGO_PORT, LOCAL_MONGO_HOST, DB_NAME


class CookieMiddleware(object):
    """
    每次请求都随机从账号池中选择一个账号去访问
    """

    def __init__(self):
        client = pymongo.MongoClient(LOCAL_MONGO_HOST, LOCAL_MONGO_PORT)
        self.account_collection = client[DB_NAME]['account']

    def process_request(self, request, spider):
        all_count = self.account_collection.find({'status': 'success'}).count()
        if all_count == 0:
            raise Exception('当前账号池为空')
        random_index = random.randint(0, all_count - 1)
        random_account = self.account_collection.find({'status': 'success'})[random_index]
        request.headers.setdefault('Cookie', random_account['cookie'])
        request.meta['account'] = random_account


class RedirectMiddleware(object):
    """
    检测账号是否正常
    302 / 403,说明账号cookie失效/账号被封，状态标记为error
    418,偶尔产生,需要再次请求
    """

    def __init__(self):
        client = pymongo.MongoClient(LOCAL_MONGO_HOST, LOCAL_MONGO_PORT)
        self.account_collection = client[DB_NAME]['account']

    def process_response(self, request, response, spider):
        http_code = response.status
        if http_code == 302 or http_code == 403:
            spider.logger.debug('http_code:' + http_code)
            self.account_collection.find_one_and_update({'_id': request.meta['account']['_id']},
                                                        {'$set': {'status': 'error'}}, )
            return request
        elif http_code == 418:
            spider.logger.error('ip 被封了!!!请更换ip,或者停止程序...')
            self.delete_proxy(request.meta['proxy'])
            return request
        else:
            return response

    def delete_proxy(self, proxy):
        requests.get("http://172.16.1.65:5010/delete/?proxy={}".format(proxy))


class IPProxyMiddleware(object):

    def get_proxy(self):
        """
        'ip:port'的格式，如'12.34.1.4:9090'
        :return:
        """
        return requests.get("http://172.16.1.65:5010/get/").text

    def process_request(self, request, spider):
        proxy_data = self.get_proxy()
        if proxy_data:
            current_proxy = 'http://' + proxy_data
            spider.logger.debug(f"当前代理IP:{current_proxy}")
            request.meta['proxy'] = current_proxy

# encoding: utf-8
import base64
import random

import pymongo
import requests
from scrapy import signals
from twisted.internet import task

from sina.settings import LOCAL_MONGO_PORT, LOCAL_MONGO_HOST, DB_NAME
from sina.settings import USER_AGENTS


class SpeedMiddleware(object):
    def __init__(self, stats):
        self.stats = stats
        # 每隔多少秒监控一次已抓取数量
        self.time = 10.0

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        instance = cls(crawler.stats)
        crawler.signals.connect(instance.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(instance.spider_closed, signal=signals.spider_closed)
        return instance

    def spider_opened(self):
        self.tsk = task.LoopingCall(self.collect)
        self.tsk.start(self.time, now=True)

    def spider_closed(self):
        scrapy_count = self.stats.get_value('item_scraped_count')
        print(scrapy_count)
        if self.tsk.running:
            self.tsk.stop()

    def collect(self):
        # 这里收集stats并写入相关的储存。
        # 目前展示是输出到终端
        scrapy_count = self.stats.get_value('item_scraped_count')
        if scrapy_count:
            print(scrapy_count)

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
            spider.logger.debug('http_code:%s' % http_code)
            self.account_collection.find_one_and_update({'_id': request.meta['account']['_id']},
                                                        {'$set': {'status': 'error'}}, )
            return request
        elif http_code == 418:
            spider.logger.error('ip 被封了!!!请更换ip,或者停止程序...')
            # self.delete_proxy(request.meta['proxy'])
            return request
        else:
            return response

    # def delete_proxy(self, proxy):
    #     requests.get("http://172.16.1.65:5010/delete/?proxy={}".format(proxy))


class IPProxyMiddleware(object):
    proxyServer = "http://http-dyn.abuyun.com:9020"
    proxyUser = "H80Y6E38XC1M20XD"
    proxyPass = "B8AFEE45CF44DDB2"
    # 代理隧道验证信息
    proxyAuth = "Basic " + base64.urlsafe_b64encode(bytes((proxyUser + ":" + proxyPass), "ascii")).decode("utf8")

    def get_proxy(self):
        """
        'ip:port'的格式，如'12.34.1.4:9090'
        :return:
        """
        return requests.get("http://172.16.1.65:5010/get/").text

    def process_request(self, request, spider):
        # proxy_data = self.get_proxy()
        # if proxy_data:
        # current_proxy = 'http://' + proxy_data
        # spider.logger.debug(f"当前代理IP:{current_proxy}")
        # request.meta['proxy'] = current_proxy

        user_agents = random.choice(USER_AGENTS)
        request.headers.setdefault('User-Agent', user_agents)
        request.meta["proxy"] = self.proxyServer
        request.headers["Proxy-Authorization"] = self.proxyAuth

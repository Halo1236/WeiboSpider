# -*- coding: utf-8 -*-
import json
import re

import scrapy
from lxml import etree
from scrapy import Request


class IdspiderSpider(scrapy.Spider):
    name = 'idSpider'
    uid_list = []
    allowed_domains = ['weibo.cn']
    start_urls = ['http://weibo.cn/']
    base_url = 'https://weibo.cn/search/user/?keyword=天津&page=%s'

    def start_requests(self):
        self.uid_list = []
        for uid in range(1, 51):
            yield Request(url=self.base_url % uid, callback=self.parse)

    def parse(self, response):
        tree_node = etree.HTML(response.body)
        nodes = tree_node.xpath('//form[@method="post"]/@action')
        for url in nodes:
            uid = re.search(r'/attention/add\?uid=(\d+)', url)
            if uid:
                print(uid.group(1))
                self.uid_list.append(uid.group(1))
        with open("D:\halo\py\WeiboSpider\sina\spiders\\record.json", "w") as f:
            json.dump(self.uid_list, f)

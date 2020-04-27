#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
created by Halo 2020/4/27 10:35
"""
import scrapy
from redis import Redis
from scrapy.utils.project import get_project_settings
from scrapy_redis_bloomfilter.bloomfilter import BloomFilter


class CrawlerSpider(scrapy.Spider):
    name = 'crawler'
    settings = get_project_settings()
    server = Redis.from_url(settings.get("REDIS_URL"))

    def start_requests(self):
        url_list = []
        for url in url_list:
            # 使用布隆过滤器进行去重
            if not self.dul_url_bf(url, 'url_finger'):
                yield scrapy.Request(url, callback=self.parse_detail)

    def parse_detail(self, response):
        content = response.xpath().extract_first()
        # 使用布隆过滤器进行去重
        if not self.dul_url_bf(content, 'content_finger'):
            print(content)

    # 此处传入url,或者文本正文，key可以传递指纹名称
    def dul_url_bf(self, url, key):
        '''
        url去重，如果url已经存在返回True,反之把url写入bloomfilter,并返回False(bf组件的exist()存在的时候返回1, 不存在返回false)
        :param url:
        :return: 如果存在返回True,不存在返回False
        '''
        bf = BloomFilter(server=self.server, key=key,
                         hash_number=self.settings.get("BLOOMFILTER_HASH_NUMBER_URL"),
                         bit=self.settings.get("BLOOMFILTER_BIT_URL"))
        if bf.exists(url):
            print(f'dupeurl:{url}')
            return True
        else:
            bf.insert(url)
            return False

#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
created by Halo 2019/7/9 11:56
"""

from scrapy.cmdline import execute

# execute(['scrapy', 'crawl', 'weibo_spider', '-s', 'JOBDIR=crawls/appWeibo-1'])
execute(['scrapy', 'crawl', 'weibo_spider'])

#!/usr/bin/env python
# encoding: utf-8
import datetime
import re
from lxml import etree
from scrapy import Spider
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.utils.project import get_project_settings
from sina.items import TweetsItem, InformationItem, RelationshipsItem, CommentItem
from sina.utils import time_fix, extract_weibo_content, extract_comment_content
import time


class WeiboSpider(Spider):
    name = "weibo_spider"
    base_url = "https://weibo.cn"

    def start_requests(self):
        start_uids = [
            "2489610225",
            "3163782211",
            "5018637328",
            "2085794347",
            "2358203003",
            "5582151150",
            "2490830195",
            "3156358147",
            "3009277592",
            "2085621483",
            "3250739191",
            "2321517984",
            "3286928372",
            "1274689160",
            "3316876605",
            "1986142787",
            "2323162571",
            "3142524405",
            "1414521997",
            "3888882491",
            "3237896304",
            "3304713765",
            "5897816720",
            "3162485697",
            "2542584687",
            "3009368284",
            "3237977132",
            "2062579281",
            "1972092827",
            "1780626511",
            "3014669221",
            "3618356673",
            "3720234243",
            "3416886302",
            "3919910570",
            "3483877775",
            "3237864304",
            "5720846370",
            "2783300235",
            "3026791217",
            "5651373179",
            "6008253025",
            "3774510130",
            "1846613483",
            "5611252299",
            "2020674845",
            "5332411930",
            "2408563290",
            "5349861361",
            "3497419227",
            "3241456580",
            "3436542520",
            "3605641842",
            "3241019570",
            "3263995385",
            "3278128471",
            "3473434090",
            "2013377617",
            "6170256671",
            "2268861017",
            "1867064437",
            "3276879583",
            "3244253572",
            "2628634973",
            "2561971013",
            "5732746604",
            "2619139711",
            "5350117449",
            "3952245756",
            "3672564242",
            "2638070263",
            "1154463150",
            "5663873844",
            "3029042875",
            "2931783897",
            "5329742278",
            "3959246898",
            "3241360764",
            "3463433664",
            "3280815121",
            "6176076827",
            "3144192467",
            "2922963852",
            "2294063531",
            "3248625825",
            "1764668035",
            "2747069655",
            "5143937592",
            "3232962800",
            "3360788612",
            "5385431433",
            "2871597060",
            "3846634455",
            "2744330960",
            "3962603810",
            "3971635987",
            "3483563760",
            "3316316505",
            "3926650990",
            "1617488410",
            "3365906364",
            "3373832900",
            "3250216315",
            "2811946900",
            "5995639141",
            "3278155445",
            "5271683381",
            "5177153131",
            "5182140450",
            "5171271494",
            "3483563587",
            "5095946997",
            "2089066941",
            "5330003322",
            "1896820725",
            "2288486705",
            "2040119035",
            "5092540901",
            "2081474535",
            "2804827393",
            "1871729063",
            "6601557815",
            "3514938052",
            "3546332963",
            "2435011495",
            "1863933484",
            "1927355635",
            "2459182322",
            "3314280193",
            "2459176692",
            "2166205181",
            "1905859287",
            "1768875897",
            "2755757383",
            "2561292283",
            "1672636564",
            "2093930331",
            "1743931961",
            "2707705292",
            "2853967810",
            "1764903565",
            "1967542335",
            "5146731838",
            "1971261335",
            "5361355153",
            "1787417547",
            "1915018060",
            "6693924555",
            "5388908261",
            "2286759285",
            "3284359355",
            "5332219646",
            "5355973082",
            "3489450883",
            "2482301641",
            "3319593617",
            "3976442236",
            "3277702241",
            "5334171024",
            "6362674659",
            "5184485824",
            "1796341451",
            "1150252302",
            "5744672955",
            "5712005627",
            "1688101107",
            "6072764798",
            "5109600385",
            "5242156955",
            "6274644284",
            "5774465112",
            "3867358682",
            "6657266877",
            "6460994699",
            "1879375752",
            "1070566025",
            "2898383271",
            "2531728090",
            "2054367281",
            "1661365924",
            "5876822629",
            "1595045493",
            "2002571163",
            "2714768693",
            "3816098407",
            "2117838117",
            "1674532845",
            "1986959921",
            "5339664769",
            "2809952652",
            "2736763727",
            "5597064956",
            "2060075827",
            "5650277713",
            "1075732170",
            "1916530792",
            "5976024275",
            "2484089162",
            "2690554612",
            "2696020393",
            "1584787070",
            "3246220454",
            "2530417354",
            "2697339551",
            "5562281050",
            "3266669812",
            "2988441331",
            "5893983063",
            "2026841804",
            "2116532322",
            "1376176022",
            "2411277125",
            "6658307729",
            "5333767263",
            "3099215743",
            "1204602151",
            "1863931504",
            "2019390825",
            "2248568893",
            "2030022153",
            "5288647434",
            "3717931952",
            "1367439987",
            "5109202031",
            "6079195070",
            "1722425473",
            "2281233221",
            "1749602275",
            "3096944547",
            "3263146791",
            "5939115345",
            "3618448637",
            "5658243669",
            "1065099995",
            "1779995870",
            "5196277838",
            "6810959115",
            "2473986835",
            "2609421507",
            "1872882507",
            "3825099977",
            "1898567185",
            "5067532524",
            "2249922565",
            "2109462577",
            "3947120103",
            "1980684173",
            "3964595614",
            "1708295524",
            "3766719145",
            "2822847512",
            "2146589541",
            "2692716451",
            "2424869197",
            "2722566443",
            "1747399132",
            "1439660697",
            "1829976535",
            "5033939253",
            "2659560092",
            "6439735830",
            "2241178801",
            "5614573825",
            "1926024235",
            "1668717603",
            "3165280783",
            "2878075622",
            "2040669523",
            "2659218371",
            "2796530090",
            "2079707781",
            "1923210015",
            "1289113135",
            "2042725697",
            "5587674818",
            "1677750677",
            "2280069477",
            "3883267363",
            "2881070844",
            "1661892365",
            "2775865581",
            "1392817397",
            "3910380770",
            "1828359933",
            "2438629673",
            "2241178821",
            "1948881450",
            "2524139534",
            "2440177801",
            "3970032975",
            "5352817390",
            "2742132041",
            "3165341937",
            "2660065772",
            "2097985730",
            "2435407091",
            "2449248062",
            "1799305173",
            "2177810835",
            "1902253474",
            "3316214875",
            "2848077540",
            "5096848072",
            "2987284933",
            "2650116405",
            "1977969234",
            "2170519533",
            "2338933643",
            "5307929539",
            "1905730485",
            "3057284334",
            "1896745891",
            "6124442690",
            "1790441385",
            "2527321991",
            "1325740631",
            "2131466825",
            "1968621521",
            "1918916835",
            "5889219623",
            "6201084646",
            "2341782777",
            "5321724315",
            "2544303155",
            "5885044766",
            "2109341913",
            "2241178835",
            "5741805096",
            "2138031547",
            "2392773183",
            "5743928514",
            "2115467654",
            "1822604321",
            "1764723463",
            "5834779294",
            "1730319991",
            "2986945202",
            "6625692270",
            "1865059941",
            "2884512574",
            "2131410367",
            "2006922583",
            "1931818505",
            "2168231361",
            "2608580372",
            "5866735329",
            "1827321240",
            "5974707214",
            "2714365361",
            "3137442014",
            "2137794041",
            "5137242466",
            "5994384106",
            "2510060155",
            "2971176983",
            "2553426347",
            "1728660253",
            "3965323451",
            "2643951560",
            "1808591274",
            "6043699292",
            "1192500647",
            "3196787374",
            "6397923886",
            "3987441781",
            "3274086951",
            "2486217140",
            "5260961794",
            "2364649532",
            "2960403711",
            "2261504755",
            "1800902481",
            "1947323864",
            "3242931773",
            "2806214634",
            "1256699602",
            "1906778127",
            "3293770963",
            "3883542433",
            "2161959110",
            "1921565421",
            "1578461595",
            "5390296204",
            "1933108375",
            "2483036187",
            "3886393258",
            "1781327082",
            "3814047993",
            "5043632915",
            "1078469762",
            "6709440104",
            "1580033962",
            "2304851674",
            "2073166785",
            "2637458551",
            "2753434965",
            "2633883440",
            "3196326742",
            "6930524432",
            "2705425747",
            "3703861257",
            "1866301442",
            "2016276913",
            "1872704570",
            "3708743890",
            "2860615970",
            "3120886924",
            "2816817224",
            "1953500547",
            "2576081991",
            "3829331418",
            "2843281283",
            "3924767762",
            "2060518123",
            "2504598293",
            "5175864213",
            "2559300834",
            "1498259771",
            "3260123625",
            "1682210634",
            "2152686560",
            "2404144105",
            "2370964771",
            "2006225450",
            "2180809653",
            "1568118331",
            "2217895261",
            "2940966314",
            "2109914795",
            "2609607303",
            "2240439787",
            "5122163863",
            "2710620054",
            "2203018152",
            "6109445571",
            "2466424232",
            "2061837283",
            "5141890951",
            "1980387010",
            "2877289602",
            "2808525104",
            "1829970097",
            "2610773833",
            "2491388260",
            "5576596017",
            "1879078611",
            "6449299615",
            "5058806286",
            "2109964153",
            "1904287977",
            "2337547167",
            "2034988235",
            "2099030081",
            "3884994904",
            "5871443933",
            "2316270273",
            "3902134828",
            "5171795319",
            "5640416325",
            "6068807698",
            "5121471246",
            "1719187823",
            "5842631975",
            "3239206937",
            "3659818285",
            "3184521314",
            "5261535972",
            "1797375501",
            "2719135743",
            "3147834173",
            "1727161784",
            "2109032945",
            "5593881502",
            "2244554142",
            "1932886857",
            "2287232535",
            "3847270184",
            "2451547662",
            "2384060720",
            "6424614554",
            "6061074864",
            "3883227202",
            "5661117180",
            "1835702133",
            "3265533444",
            "1982371847",
            "1736279574",
            "1921344763",
            "5859310123"
        ]
        for uid in reversed(start_uids):
            yield Request(url="https://weibo.cn/%s/info" % uid, callback=self.parse_information)

    def parse_information(self, response):
        """ 抓取个人信息 """
        information_item = InformationItem()
        now_time = datetime.datetime.now()
        information_item['crawl_time'] = now_time.strftime('%Y-%m-%d %H:%M:%S')
        selector = Selector(response)
        information_item['_id'] = re.findall('(\d+)/info', response.url)[0]
        text1 = ";".join(selector.xpath('body/div[@class="c"]//text()').extract())  # 获取标签里的所有text()
        nick_name = re.findall('昵称;?[：:]?(.*?);', text1)
        gender = re.findall('性别;?[：:]?(.*?);', text1)
        place = re.findall('地区;?[：:]?(.*?);', text1)
        briefIntroduction = re.findall('简介;?[：:]?(.*?);', text1)
        birthday = re.findall('生日;?[：:]?(.*?);', text1)
        sex_orientation = re.findall('性取向;?[：:]?(.*?);', text1)
        sentiment = re.findall('感情状况;?[：:]?(.*?);', text1)
        vip_level = re.findall('会员等级;?[：:]?(.*?);', text1)
        authentication = re.findall('认证;?[：:]?(.*?);', text1)
        labels = re.findall('标签;?[：:]?(.*?)更多>>', text1)
        if nick_name and nick_name[0]:
            information_item["nick_name"] = nick_name[0].replace(u"\xa0", "")
        if gender and gender[0]:
            information_item["gender"] = gender[0].replace(u"\xa0", "")
        if place and place[0]:
            place = place[0].replace(u"\xa0", "").split(" ")
            information_item["province"] = place[0]
            if len(place) > 1:
                information_item["city"] = place[1]
        if briefIntroduction and briefIntroduction[0]:
            information_item["brief_introduction"] = briefIntroduction[0].replace(u"\xa0", "")
        if birthday and birthday[0]:
            information_item['birthday'] = birthday[0]
        if sex_orientation and sex_orientation[0]:
            if sex_orientation[0].replace(u"\xa0", "") == gender[0]:
                information_item["sex_orientation"] = "同性恋"
            else:
                information_item["sex_orientation"] = "异性恋"
        if sentiment and sentiment[0]:
            information_item["sentiment"] = sentiment[0].replace(u"\xa0", "")
        if vip_level and vip_level[0]:
            information_item["vip_level"] = vip_level[0].replace(u"\xa0", "")
        if authentication and authentication[0]:
            information_item["authentication"] = authentication[0].replace(u"\xa0", "")
        if labels and labels[0]:
            information_item["labels"] = labels[0].replace(u"\xa0", ",").replace(';', '').strip(',')
        request_meta = response.meta
        request_meta['item'] = information_item
        yield Request(self.base_url + '/u/{}'.format(information_item['_id']),
                      callback=self.parse_further_information,
                      meta=request_meta, dont_filter=True, priority=1)

    def parse_further_information(self, response):
        text = response.text
        information_item = response.meta['item']
        tweets_num = re.findall('微博\[(\d+)\]', text)
        if tweets_num:
            information_item['tweets_num'] = int(tweets_num[0])
        follows_num = re.findall('关注\[(\d+)\]', text)
        if follows_num:
            information_item['follows_num'] = int(follows_num[0])
        fans_num = re.findall('粉丝\[(\d+)\]', text)
        if fans_num:
            information_item['fans_num'] = int(fans_num[0])
        yield information_item

        # 获取该用户微博
        yield Request(url=self.base_url + '/{}/profile?page=1'.format(information_item['_id']),
                      callback=self.parse_tweet, meta=information_item, priority=1)

        # # 获取关注列表
        # yield Request(url=self.base_url + '/{}/follow?page=1'.format(information_item['_id']),
        #               callback=self.parse_follow,
        #               dont_filter=True)
        # # 获取粉丝列表
        # yield Request(url=self.base_url + '/{}/fans?page=1'.format(information_item['_id']),
        #               callback=self.parse_fans,
        #               dont_filter=True)

    def parse_tweet(self, response):
        information_item = response.meta
        if response.url.endswith('page=1'):
            # 如果是第1页，一次性获取后面的所有页
            all_page = re.search(r'/>&nbsp;1/(\d+)页</div>', response.text)
            if all_page:
                all_page = all_page.group(1)
                all_page = int(all_page)
                if all_page > 100:
                    all_page = 100
                for page_num in range(2, all_page + 1):
                    page_url = response.url.replace('page=1', 'page={}'.format(page_num))
                    yield Request(page_url, self.parse_tweet, dont_filter=True, meta=response.meta)
        """
        解析本页的数据
        """
        tree_node = etree.HTML(response.body)
        tweet_nodes = tree_node.xpath('//div[@class="c" and @id]')
        for tweet_node in tweet_nodes:
            try:
                tweet_item = TweetsItem()
                tweet_item['nick_name'] = information_item["nick_name"]
                now_time = datetime.datetime.now()
                tweet_item['crawl_time'] = now_time.strftime('%Y-%m-%d %H:%M:%S')
                tweet_repost_url = tweet_node.xpath('.//a[contains(text(),"转发[")]/@href')[0]
                user_tweet_id = re.search(r'/repost/(.*?)\?uid=(\d+)', tweet_repost_url)
                tweet_item['weibo_url'] = 'https://weibo.com/{}/{}'.format(user_tweet_id.group(2),
                                                                           user_tweet_id.group(1))
                tweet_item['user_id'] = user_tweet_id.group(2)
                tweet_item['_id'] = '{}_{}'.format(user_tweet_id.group(2), user_tweet_id.group(1))
                create_time_info_node = tweet_node.xpath('.//span[@class="ct"]')[-1]
                create_time_info = create_time_info_node.xpath('string(.)')
                if "来自" in create_time_info:
                    tweet_item['created_at'] = time_fix(create_time_info.split('来自')[0].strip())
                    tweet_item['tool'] = create_time_info.split('来自')[1].strip()
                else:
                    tweet_item['created_at'] = time_fix(create_time_info.strip())

                like_num = tweet_node.xpath('.//a[contains(text(),"赞[")]/text()')[-1]
                tweet_item['like_num'] = int(re.search('\d+', like_num).group())

                repost_num = tweet_node.xpath('.//a[contains(text(),"转发[")]/text()')[-1]
                tweet_item['repost_num'] = int(re.search('\d+', repost_num).group())

                comment_num = tweet_node.xpath(
                    './/a[contains(text(),"评论[") and not(contains(text(),"原文"))]/text()')[-1]
                tweet_item['comment_num'] = int(re.search('\d+', comment_num).group())

                images = tweet_node.xpath('.//img[@alt="图片"]/@src')
                if images:
                    tweet_item['image_url'] = images[0]

                videos = tweet_node.xpath('.//a[contains(@href,"https://m.weibo.cn/s/video/show?object_id=")]/@href')
                if videos:
                    tweet_item['video_url'] = videos[0]

                map_node = tweet_node.xpath('.//a[contains(text(),"显示地图")]')
                if map_node:
                    map_node = map_node[0]
                    map_node_url = map_node.xpath('./@href')[0]
                    map_info = re.search(r'xy=(.*?)&', map_node_url).group(1)
                    tweet_item['location_map_info'] = map_info
                    tweet_item['location'] = map_node.xpath('./preceding-sibling::a/text()')[0]

                repost_node = tweet_node.xpath('.//a[contains(text(),"原文评论[")]/@href')
                if repost_node:
                    tweet_item['origin_weibo'] = repost_node[0]

                # 检测由没有阅读全文:
                all_content_link = tweet_node.xpath('.//a[text()="全文" and contains(@href,"ckAll=1")]')
                if all_content_link:
                    all_content_url = self.base_url + all_content_link[0].xpath('./@href')[0]
                    yield Request(all_content_url, callback=self.parse_all_content, meta={'item': tweet_item},
                                  priority=1)

                else:
                    tweet_html = etree.tostring(tweet_node, encoding='unicode')
                    tweet_item['content'] = extract_weibo_content(tweet_html)
                    yield tweet_item

                # 抓取该微博的评论信息
                # comment_url = self.base_url + '/comment/' + tweet_item['weibo_url'].split('/')[-1] + '?page=1'
                # yield Request(url=comment_url, callback=self.parse_comment, meta={'weibo_url': tweet_item['weibo_url']})

            except Exception as e:
                self.logger.error(e)

    def parse_all_content(self, response):
        # 有阅读全文的情况，获取全文
        tree_node = etree.HTML(response.body)
        tweet_item = response.meta['item']
        content_node = tree_node.xpath('//*[@id="M_"]/div[1]')[0]
        tweet_html = etree.tostring(content_node, encoding='unicode')
        tweet_item['content'] = extract_weibo_content(tweet_html)
        yield tweet_item

    def parse_follow(self, response):
        """
        抓取关注列表
        """
        # 如果是第1页，一次性获取后面的所有页
        if response.url.endswith('page=1'):
            all_page = re.search(r'/>&nbsp;1/(\d+)页</div>', response.text)
            if all_page:
                all_page = all_page.group(1)
                all_page = int(all_page)
                for page_num in range(2, all_page + 1):
                    page_url = response.url.replace('page=1', 'page={}'.format(page_num))
                    yield Request(page_url, self.parse_follow, dont_filter=True, meta=response.meta)
        selector = Selector(response)
        urls = selector.xpath('//a[text()="关注他" or text()="关注她" or text()="取消关注"]/@href').extract()
        uids = re.findall('uid=(\d+)', ";".join(urls), re.S)
        ID = re.findall('(\d+)/follow', response.url)[0]
        for uid in uids:
            relationships_item = RelationshipsItem()
            relationships_item['crawl_time'] = int(time.time())
            relationships_item["fan_id"] = ID
            relationships_item["followed_id"] = uid
            relationships_item["_id"] = ID + '-' + uid
            yield relationships_item

    def parse_fans(self, response):
        """
        抓取粉丝列表
        """
        # 如果是第1页，一次性获取后面的所有页
        if response.url.endswith('page=1'):
            all_page = re.search(r'/>&nbsp;1/(\d+)页</div>', response.text)
            if all_page:
                all_page = all_page.group(1)
                all_page = int(all_page)
                for page_num in range(2, all_page + 1):
                    page_url = response.url.replace('page=1', 'page={}'.format(page_num))
                    yield Request(page_url, self.parse_fans, dont_filter=True, meta=response.meta)
        selector = Selector(response)
        urls = selector.xpath('//a[text()="关注他" or text()="关注她" or text()="移除"]/@href').extract()
        uids = re.findall('uid=(\d+)', ";".join(urls), re.S)
        ID = re.findall('(\d+)/fans', response.url)[0]
        for uid in uids:
            relationships_item = RelationshipsItem()
            relationships_item['crawl_time'] = int(time.time())
            relationships_item["fan_id"] = uid
            relationships_item["followed_id"] = ID
            relationships_item["_id"] = uid + '-' + ID
            yield relationships_item

    def parse_comment(self, response):
        # 如果是第1页，一次性获取后面的所有页
        if response.url.endswith('page=1'):
            all_page = re.search(r'/>&nbsp;1/(\d+)页</div>', response.text)
            if all_page:
                all_page = all_page.group(1)
                all_page = int(all_page)
                for page_num in range(2, all_page + 1):
                    page_url = response.url.replace('page=1', 'page={}'.format(page_num))
                    yield Request(page_url, self.parse_comment, dont_filter=True, meta=response.meta)
        tree_node = etree.HTML(response.body)
        comment_nodes = tree_node.xpath('//div[@class="c" and contains(@id,"C_")]')
        for comment_node in comment_nodes:
            comment_user_url = comment_node.xpath('.//a[contains(@href,"/u/")]/@href')
            if not comment_user_url:
                continue
            comment_item = CommentItem()
            comment_item['crawl_time'] = int(time.time())
            comment_item['weibo_url'] = response.meta['weibo_url']
            comment_item['comment_user_id'] = re.search(r'/u/(\d+)', comment_user_url[0]).group(1)
            comment_item['content'] = extract_comment_content(etree.tostring(comment_node, encoding='unicode'))
            comment_item['_id'] = comment_node.xpath('./@id')[0]
            created_at_info = comment_node.xpath('.//span[@class="ct"]/text()')[0]
            like_num = comment_node.xpath('.//a[contains(text(),"赞[")]/text()')[-1]
            comment_item['like_num'] = int(re.search('\d+', like_num).group())
            comment_item['created_at'] = time_fix(created_at_info.split('\xa0')[0])
            yield comment_item


if __name__ == "__main__":
    process = CrawlerProcess(get_project_settings())
    process.crawl('weibo_spider')
    process.start()

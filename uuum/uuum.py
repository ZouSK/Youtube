import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from pathlib import Path

import openpyxl
import requests
from pyquery import PyQuery


class Crawler:
    """
    - Category：就是菜单栏上的那7个
    - Order：即位于列表中第几个
    - ID：这个不一定有，我自己是没找到，看运气吧
    - Name：creator__name
    - Caption：caption__text
    - Link

    - Channel Name：频道名称
    - Subscribers：订阅这个频道的人数
    - Total Video Views：频道节目累计播放量
    """

    # IMPORTANT!!!
    # youtube用不同地区的ip访问返回页面不同，请保证使用美国代理
    proxies = {
        'http': 'http://127.0.0.1:1080',
        'https': 'http://127.0.0.1:1080'
    }

    url = 'https://www.uuum.jp/creator/'

    category = {
        'multi': 'マルチクリエイター',
        'game': 'ゲーム実況',
        'beauty': 'ファッション&ビューティー',
        'creative': '映像クリエイティブ',
        'hobby': 'ホビー&カルチャー',
        'toy': 'トイ&キッズ',
        'global': '海外'
    }

    match = {
        'name': '.creator__name',
        'caption': '.caption__text',
        'youtube': '.creator__links__youtube > a',
        'profile': '.creator__links__profile > a'
    }

    youtube_match = {
        # 动态加载匹配
        # 'channel': '#channel-title',
        # 'subscriber': '#subscriber-count',
        # 'view': '#right-column > yt-formatted-string:nth-child(3)'

        # 静态加载匹配
        'channel': '#c4-primary-header-contents > div > div > div:nth-child(1) > h1 > span > span > span > a',
        'subscriber': '#browse-items-primary > li > div > div.about-metadata-stats.branded-page-box-padding > '
                      'div > span:nth-child(1)',
        'view': '#browse-items-primary > li > div > div.about-metadata-stats.branded-page-box-padding > '
                'div > span:nth-child(2)'
    }

    uuum_id_match = re.compile('/creator/([\w-]+)')
    youtube_url_user_match = re.compile('http[s]?://www.youtube.com/user/[\w-]+')
    youtube_url_channel_match = re.compile('http[s]?://www.youtube.com/channel/[\w-]+')
    youtube_url_custom_match = re.compile('http[s]?://www.youtube.com/c/[\w-]+')

    def get_uuum(self):
        cache = Path('cache/uuum.json')
        if cache.exists():
            return json.loads(cache.read_text())

        data = requests.get(self.url, timeout=15)
        print(f'first_url:{self.url}')
        data.encoding = 'utf-8'

        pq = PyQuery(data.text)

        result = []
        order = 1

        for key, value in self.category.items():
            names = [_.text.strip() for _ in pq(
                '#{} {}'.format(key, self.match['name']))]
            captions = [_.text.strip() for _ in pq(
                '#{} {}'.format(key, self.match['caption']))]
            links = [_.get('href') for _ in pq(
                '#{} {}'.format(key, self.match['youtube']))]
            ids = [self.get_id(_.get('href')) for _ in pq(
                '#{} {}'.format(key, self.match['profile']))]

            assert len(names) == len(captions) == len(links) == len(ids), '{}的结果数量不匹配'.format(key)

            for _id, name, caption, link in zip(ids, names, captions, links):
                result.append({
                    'category': key,
                    'order': order,
                    'id': _id,
                    'name': name,
                    'caption': caption,
                    'link': link
                })

                order += 1

        cache.parent.mkdir(exist_ok=True)
        cache.write_text(json.dumps(result))
        return result

    def get_id(self, _id):
        match = re.search(self.uuum_id_match, _id)
        assert match, f'No id match: {_id}'
        return match.group(1)

    def get_youtube_url(self, link):
        """当前会取到4种url，需要进行过滤"""
        match = re.search(self.youtube_url_user_match, link)
        if match:
            url = match.group(0)
        else:
            match = re.search(self.youtube_url_channel_match, link)
            if match:
                url = match.group(0)
            else:
                match = re.search(self.youtube_url_custom_match, link)
                assert match, f'未知类型:{link}'

                # req = requests.get(link, proxies=self.proxies)
                req = requests.get(link, timeout=15)
                print(f'second url: {link}')
                req.encoding = 'utf-8'

                user_match = re.search(self.youtube_url_user_match, req.text)
                assert user_match, f'用户未匹配到:{link}'
                url = user_match.group(0)

        return f'{url}/about'

    def get_youtube(self, creator):
        cache = Path(f'cache/youtube/{creator["id"]}.json')
        if cache.exists():
            return json.loads(cache.read_text())

        url = self.get_youtube_url(creator['link'])
        # data = requests.get(url, proxies=self.proxies)
        data = requests.get(url, timeout=15)
        print(f'third url: {url}')
        data.encoding = 'utf-8'

        pq = PyQuery(data.text)

        channel = pq(self.youtube_match['channel']).text().strip()
        assert channel, f'{url} channel匹配不到'

        subscriber = pq(self.youtube_match['subscriber']).text()
        assert 'subscribers' in subscriber, f'{url} subscriber匹配不到'
        subscriber = int(''.join(_ for _ in subscriber if _.isdigit()))

        view = pq(self.youtube_match['view']).text()
        assert 'views' in view, f'{url} view匹配不到'
        view = int(''.join(_ for _ in view if _.isdigit()))

        result = {
            'channel': channel,
            'subscriber': subscriber,
            'view': view
        }

        cache.parent.mkdir(exist_ok=True)
        cache.write_text(json.dumps(result))
        return result

    def run(self):
        creators = self.get_uuum()
        executor = ThreadPoolExecutor(max_workers=10)
        all_task = []

        for creator in creators:
            all_task.append(executor.submit(self.get_youtube, creator))

        for future in as_completed(all_task):
            print(future.result())

    def save2excel(self):
        wb = openpyxl.Workbook()
        ws = wb.create_sheet('creator', 0)

        ws.append([
            'ID', 'Category', 'Order', 'Name', 'Caption', 'Channel Name', 'Subscribers', 'Total Video Views', 'Link'
        ])

        for creator in self.get_uuum():
            youtube = self.get_youtube(creator)

            ws.append([
                creator['id'],
                self.category[creator['category']],
                creator['order'],
                creator['name'],
                creator['caption'],
                youtube['channel'],
                youtube['subscriber'],
                youtube['view'],
                creator['link']
            ])

        wb.save('uuum.xlsx')

    def save2mongo(self):
        from pymongo import MongoClient
    
        table = MongoClient().youtube.uuum
    
        data = []
    
        keys = ['id', 'category', 'order', 'name', 'caption', 'channel', 'subscriber', 'view', 'link']
    
        for creator in self.get_uuum():
            youtube = self.get_youtube(creator)
    
            data.append(dict(zip(keys, [
                creator['id'],
                self.category[creator['category']],
                creator['order'],
                creator['name'],
                creator['caption'],
                youtube['channel'],
                youtube['subscriber'],
                youtube['view'],
                creator['link']
            ])))
    
        table.insert_many(data)

    def save2mysql(self):
        from models import session, Uuum

        uuum_list = []
        for creator in self.get_uuum():
            youtube = self.get_youtube(creator)

            u = Uuum(
                id=creator['id'],
                category=self.category[creator['category']],
                order=creator['order'],
                name=creator['name'],
                caption=creator['caption'],
                channel=youtube['channel'],
                subscriber=youtube['subscriber'],
                views=youtube['view'],
                link=creator['link']
            )
            uuum_list.append(u)
        session.add_all(uuum_list)
        session.commit()


    @staticmethod
    def clear_cache():
        for file in Path('cache').rglob('*.json'):
            file.unlink()


if __name__ == '__main__':
    crawler = Crawler()

    # 清除缓存
    crawler.clear_cache()
    crawler.run()
    crawler.save2excel()
    # crawler.save2mysql()
    # crawler.save2mongo()

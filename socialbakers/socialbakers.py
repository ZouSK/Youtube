import json
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urljoin

import openpyxl
import requests
from pyquery import PyQuery


class Crawler:
    url = 'https://www.socialbakers.com/statistics/youtube/channels/japan/page-{}/'  # 1-100

    rank = 'td.item-count-td.brand-table-first-nr > div'
    link = 'td.name > div > a'
    channel = 'td.name > div > a > h2 > span'
    subscriber = 'td:nth-child(3) > div'
    view = 'td:nth-child(4) > div > strong'

    video = 'div.account-detail > ul > li:nth-child(3) > span > strong'
    youtube_link = 'div.account-detail > ul > li:nth-child(1) > span > a'
    youtube_link_match = re.compile('http[s]?://www.youtube.com/channel/([^/]+)')

    def get_creator(self, page):
        cache = Path(f'cache/{page}.json')
        if cache.exists():
            return json.loads(cache.read_text())

        req = requests.get(self.url.format(page))
        # print(f'firsturl: {self.url.format(page)}')
        req.encoding = 'utf-8'

        pq = PyQuery(req.text)('table tr')

        ranks = [_.text.strip() for _ in pq(self.rank)]
        links = [urljoin(self.url, _.get('href')) for _ in pq(self.link)]
        channels = [_.text.strip() for _ in pq(self.channel)]
        subscribers = [int(''.join(_ for _ in subscriber.text_content() if _.isdigit())) for subscriber in
                       pq(self.subscriber)]
        views = [int(''.join(_ for _ in view.text if _.isdigit())) for view in pq(self.view)]

        assert 10 == len(ranks) == len(links) == len(channels) == len(subscribers) == len(views), f'page{page}匹配失败'

        result = []
        keys = ['rank', 'channel', 'subscriber', 'view', 'link']
        for values in zip(ranks, channels, subscribers, views, links):
            result.append(dict(zip(keys, values)))

        cache.parent.mkdir(exist_ok=True)
        cache.write_text(json.dumps(result))
        return result

    def get_video(self, page):
        cache = Path(f'cache/{page}.json')
        creators = json.loads(cache.read_text())
        for creator in creators:
            if 'video' in creator:
                return creators

            req = requests.get(creator['link'])
            # print(f'secondurl:{creator["link"]}')
            req.encoding = 'utf-8'

            pq = PyQuery(req.text)

            creator['video'] = int(''.join(_ for _ in pq(self.video)[0].text if _.isdigit()))
            creator['youtube'] = pq(self.youtube_link)[0].get('href')

        cache.write_text(json.dumps(creators))
        return creators

    def run(self):
        executor = ThreadPoolExecutor(max_workers=10)

        all_task = []

        for page in range(1, 101):
            all_task.append(executor.submit(self.get_creator, page))

        for future in as_completed(all_task):
            for result in future.result():
                print(result)

        print('-' * 50)

        all_task = []

        for page in range(1, 101):
            all_task.append(executor.submit(self.get_video, page))

        for future in as_completed(all_task):
            for result in future.result():
                print(result)

    def get_match_link(self, link):
        match = re.search(self.youtube_link_match, link)
        assert match, f'Not match: {link}'
        return match.group(1)

    def save2excel(self):
        wb = openpyxl.Workbook()
        ws = wb.create_sheet('creator', 0)

        ws.append([
            'ID', 'Rank', 'Channel Name', 'Subscribers', 'Total Video Views', 'Videos', 'Link'
        ])

        for page in range(1, 101):
            cache = Path(f'cache/{page}.json')
            creators = json.loads(cache.read_text())
            for creator in creators:

                ws.append([
                    self.get_match_link(creator['youtube']),
                    creator['rank'],
                    creator['channel'],
                    creator['subscriber'],
                    creator['view'],
                    creator['video'],
                    creator['youtube']
                ])

        wb.save('socialbackers.xlsx')

    def save2mongo(self):
        from pymongo import MongoClient

        table = MongoClient().youtube.sbs

        data = []

        keys = ['id', 'rank', 'channel', 'subscriber', 'view', 'video', 'link']

        for page in range(1, 101):
            cache = Path(f'cache/{page}.json')
            creators = json.loads(cache.read_text())
            for creator in creators:

                data.append(dict(zip(keys, [
                    self.get_match_link(creator['youtube']),
                    creator['rank'],
                    creator['channel'],
                    creator['subscriber'],
                    creator['view'],
                    creator['video'],
                    creator['youtube']
                ])))

        table.insert_many(data)
    
    def save2mysql(self):

        from models import Socialbacker, session

        sblist = []

        for page in range(1, 101):
            cache = Path(f'cache/{page}.json')
            creators = json.loads(cache.read_text())
            for creator in creators:

                sb = Socialbacker(
                    id=self.get_match_link(creator['youtube']),
                    rank=creator['rank'],
                    channel=creator['channel'],
                    subscriber=creator['subscriber'],
                    views=creator['view'],
                    video=creator['video'],
                    link=creator['youtube']
                )
                sblist.append(sb)
        session.add_all(sblist)
        session.commit()

    @staticmethod
    def clear_cache():
        for file in Path('cache').rglob('*.json'):
            file.unlink()


if __name__ == '__main__':
    crawler = Crawler()

    # 清除缓存
    crawler.clear_cache()
    print('清除缓存成功')
    time.sleep(10)
    print('crawl starting!')
    crawler.run()
    crawler.save2mysql()
    crawler.save2excel()
    # crawler.save2mongo()

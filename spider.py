import requests
import re
import unicodedata
import threading
from queue import Queue
from bs4 import BeautifulSoup
from bloom_filter import ScalableBloomFilter

article_url = "https://news.sina.com.cn/s/2020-10-23/doc-iiznezxr7629012.shtml"
initial_page = "http://news.sina.com.cn/china/"


class Spider:
    url_queue = Queue()
    bf = ScalableBloomFilter(initial_capacity=10000, error_rate=0.001)
    max_url_count = 100
    lock = threading.Lock()

    def __init__(self, max_url=100, init_url=None):
        self.url_queue.put(init_url)
        self.max_url_count = max_url

    def scratch_links(self, current_url):
        res = requests.get(current_url)
        res.encoding = "utf8"
        soup = BeautifulSoup(res.text, 'html.parser')
        text = str(soup.select('a[target="_blank"]'))
        # 正则获取所有url链接
        all_links = re.findall(r'(?<=<a href=\").*?(?=\")|(?<=href=\').*?(?=\')', text)
        for link in all_links:
            use = re.findall(r'https?://news.sina.com.cn/.*/doc-iiznezxr.*', link)
            if use:
                self.lock.acquire()
                if use[0] not in self.bf:
                    self.bf.add(use[0])
                    self.url_queue.put(use[0])
                self.lock.release()

    def run(self):
        while not self.url_queue.empty():
            url = self.url_queue.get()
            # 爬取当前url

            # 将当前页面的所有链接加入队列
            self.scratch_links(url)
            print(url)


def article_spider(url):
    res = requests.get(url)
    res.encoding = "utf8"
    soup = BeautifulSoup(res.text, 'html.parser')
    news_title = soup.select("body > div.main-content.w1240 > h1")
    plist = soup.find('div', attrs={'id': 'article'}).findAll('p')
    content = ''
    for p in plist:
        content += p.text + '\n'
    data = {
        "title:": news_title[0].text,
        "content:": unicodedata.normalize('NFKC', content)
    }
    print(data)
    return data


if __name__ == "__main__":
    spider = Spider(init_url=initial_page)
    spider.run()

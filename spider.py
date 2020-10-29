import requests
import re
import os
import threading
import time
from queue import Queue
from bs4 import BeautifulSoup
from bloom_filter import ScalableBloomFilter
from file_writer import FileWriter


article_url = "https://news.sina.com.cn/s/2020-10-23/doc-iiznezxr7629012.shtml"
initial_page = "http://news.sina.com.cn/china/"
OUT_PUT_DIR = os.path.abspath(os.path.join('/home', "ray", "HHD", "SpiderData"))
TAR_FILE_SIZE = 100 * 1024 * 1024


class Spider:
    url_queue = Queue()
    bf = ScalableBloomFilter(initial_capacity=10000, error_rate=0.001)
    lock = threading.Lock()

    def __init__(self, init_url=None):
        self.url_queue.put(init_url)

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
        file_writer = FileWriter(OUT_PUT_DIR, TAR_FILE_SIZE)
        while not self.url_queue.empty() or file_writer.count < 41:
            url = self.url_queue.get()
            # 爬取当前url
            page_data = page_spider(url)
            # save data into file
            file_writer.write('a', page_data)
            # 将当前页面的所有链接加入队列
            self.scratch_links(url)
            time.sleep(2)


def article_spider(url):
    res = requests.get(url)
    res.encoding = "utf8"
    soup = BeautifulSoup(res.text, 'html.parser')
    news_title = soup.select("body > div.main-content.w1240 > h1")
    date = soup.find_all('span', attrs={'class': 'date'})
    article = soup.find('div', attrs={'id': 'article'})
    if news_title is None or article is None:
        return None
    plist = article.find_all('p')
    content = ''
    for p in plist:
        content += p.text + '\n'
    if content == '':
        return None
    return {
        "title": news_title[0].text,
        "url": url,
        "date": date[0].text,
        "content": content
    }


def page_spider(url):
    res = requests.get(url)
    res.encoding = "utf8"
    soup = BeautifulSoup(res.text, 'html.parser')
    article_data = article_spider(url)
    if article_data is None:
        return None
    article_content = [s for s in article_data.get('content').splitlines(True) if s.strip()]
    page_content = ''
    for s in soup.text.splitlines(True):
        if s.strip():
            if s in article_content:
                page_content += s.replace('\n', '') + '\tlabel:1\n'
            else:
                page_content += s.replace('\n', '') + '\tlabel:0\n'
    page_content += '\n'
    return {
        "title": article_data.get("title"),
        "url": article_data.get("url"),
        "date": article_data.get("date"),
        "content": page_content
    }
    # return "".join([s for s in soup.text.splitlines(True) if s.strip()])


if __name__ == "__main__":
    # page_data = page_spider('https://news.sina.com.cn/w/2020-10-27/doc-iiznctkc7848233.shtml')
    # article_data = article_spider('https://news.sina.com.cn/w/2020-10-27/doc-iiznctkc7848233.shtml').get('content')
    # print(page_data)
    # print(page_spider('https://news.sina.com.cn/w/2020-10-27/doc-iiznctkc7848233.shtml'))
    spider = Spider(initial_page)
    spider.run()

import requests
import re
import os
import time
import multiprocessing
import argparse
from queue import Queue
from bs4 import BeautifulSoup
from bloom_filter import ScalableBloomFilter
from file_operator import FileOperator

article_url = "https://news.sina.com.cn/s/2020-10-23/doc-iiznezxr7629012.shtml"
initial_page = "http://news.sina.com.cn"
OUT_PUT_DIR = os.path.abspath(os.path.join('/home', "ray", "HHD", "SpiderData"))
TAR_FILE_SIZE = 100 * 1024 * 1024
TASK_NUM = 1


class Spider:
    url_queue = Queue()
    bf = ScalableBloomFilter(initial_capacity=10000, error_rate=0.00001)
    lock = multiprocessing.Lock()
    task_num = TASK_NUM
    file_operator = FileOperator(OUT_PUT_DIR, TAR_FILE_SIZE)

    def __init__(self, init_url=None, task_num=TASK_NUM):
        self.bf.add(init_url)
        self.url_queue.put(init_url)
        # read from url_list file to build the url_queue
        url_list = self.file_operator.read_url()
        if len(url_list) > 0:
            for url in url_list:
                url = url.strip('\n')
                if url not in self.bf:
                    self.bf.add(url)
        self.task_num = task_num

    def scratch_links(self, res):
        res.encoding = res.apparent_encoding
        soup = BeautifulSoup(res.text, 'html.parser')
        text = str(soup.select('a[target="_blank"]'))
        # 正则获取所有url链接
        all_links = re.findall(r'(?<=<a href=\").*?(?=\")|(?<=href=\').*?(?=\')', text)
        for link in all_links:
            # use = re.findall(r'https?://news.sina.com.cn/.*/doc-iiznezxr.*', link)
            use = re.findall(r'https?://.*sina.com.cn/.*', link)
            if len(use) > 0:
                if re.findall(r'https?://slide.*', use[0]):
                    continue
                elif re.findall(r'https?://.*video.*', use[0]):
                    continue
                elif re.findall(r'https?://career.*', use[0]):
                    continue
                elif re.findall(r'https?://photo.*', use[0]):
                    continue
                elif re.findall(r'https?://.*game.*', use[0]):
                    continue
                elif re.findall(r'https?://blog.*', use[0]):
                    continue
                elif re.findall(r'https?://search.*', use[0]):
                    continue
                elif re.findall(r'https?://baby.*', use[0]):
                    continue
                elif re.findall(r'https?://.*ent..*', use[0]):
                    continue
                elif re.findall(r'https?://app.*', use[0]):
                    continue
                elif re.findall(r'https?://db.*', use[0]):
                    continue
                elif re.findall(r'https?://vip.*', use[0]):
                    continue
                elif re.findall(r'https?://book.*', use[0]):
                    continue
                elif re.findall(r'https?://comment.*', use[0]):
                    continue
                elif re.findall(r'https?://classad.*', use[0]):
                    continue
                elif re.findall(r'https?://aipai.*', use[0]):
                    continue
                elif re.findall(r'https?://ka.*', use[0]):
                    continue
                elif re.findall(r'https?://match.*', use[0]):
                    continue
                elif re.findall(r'https?://sports.sina.com.cn/star/.*', use[0]):
                    continue
                if use[0] not in self.bf:
                    self.bf.add(use[0])
                    self.url_queue.put(use[0])

    def run(self):
        while not self.url_queue.empty() and self.file_operator.count < 25:
            url = self.url_queue.get()
            try:
                res = requests.get(url)
            except Exception as e:
                print(f"url: {url} Error : {e} happended, will try soon")
                self.url_queue.put(url)
                time.sleep(2)
                continue
            # 爬取当前url
            page_data = page_spider(url, res)
            # save data into file
            if page_data is None:
                print("this page :%s has nothing to write" % url)
            else:
                self.file_operator.write_data('a', page_data)
                self.file_operator.write_url(url=url)
            # 将当前页面的所有链接加入队列
            self.scratch_links(res)
            print("the reset url number is %s" % self.url_queue.qsize())
            # time.sleep(1)


def page_spider(url, res):
    res.encoding = res.apparent_encoding
    soup = BeautifulSoup(res.text, 'html.parser')
    article_data = article_spider(url, res)
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


def article_spider(url, res):
    res.encoding = res.apparent_encoding
    soup = BeautifulSoup(res.text, 'html.parser')
    news_title = soup.select("body > div.main-content.w1240 > h1")
    if len(news_title) == 0:
        news_title = soup.find('h1', attrs={'id': 'artibodyTitle'})
    else:
        news_title = news_title[0]
    date = soup.find('span', attrs={'class': 'date'})
    if date is None:
        date = soup.find('span', attrs={'id': 'navtimeSource'})
        if date is None:
            date = soup.find('span', attrs={'id': 'pub_date'})
    article = soup.find('div', attrs={'id': 'article'})
    if article is None:
        article = soup.find('div', attrs={'id': 'artibody'})
    if news_title is None or article is None:
        return None
    plist = article.find_all('p')
    content = ''
    for p in plist:
        content += p.text + '\n'
    if content == '':
        return None
    if news_title is None:
        return None
    if date is None:
        return None
    return {
        "title": news_title.text,
        "url": url,
        "date": date.text,
        "content": content
    }


def test(i):
    print("%s is doing %s" % (multiprocessing.current_process, i))
    time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", help="the start url of the spider", default=initial_page)
    args = parser.parse_args()
    # url = 'http://sky.news.sina.com.cn/2015-12-04/103661339.html'
    # res = requests.get(url)

    # article_data = article_spider(url, res)
    # print(article_data)
    # page_data = page_spider('https://news.sina.com.cn/w/2020-10-27/doc-iiznctkc7848233.shtml')
    # print(page_data)
    # data = page_spider(url, res)
    spider = Spider(str(args.url))
    spider.run()

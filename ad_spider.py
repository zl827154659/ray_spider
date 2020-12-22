import requests
import re
import os
import time
import multiprocessing
import argparse
from queue import Queue
from bs4 import BeautifulSoup

from ad_file_operator import ADFileOperator
from bloom_filter import ScalableBloomFilter


initial_page = "http://666tycp.com/260/"
OUT_PUT_DIR = os.path.abspath(os.path.join('/home', "ray", "HDD", "AdText"))
TAR_FILE_SIZE = 100 * 1024 * 1024
TASK_NUM = 1


class ADSpider:
    url_queue = Queue()
    bf = ScalableBloomFilter(initial_capacity=100000000, error_rate=0.00001)
    lock = multiprocessing.Lock()
    task_num = TASK_NUM
    file_operator = ADFileOperator(OUT_PUT_DIR, TAR_FILE_SIZE)

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
            if re.findall(r'https?://slide.*', link):
                continue
            if link not in self.bf:
                self.bf.add(link)
                self.url_queue.put(link)

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

    page_content = ''
    for s in soup.text.splitlines(True):
        if s.strip():
            page_content += s.replace('\n', '') + '\tlabel:0\n'
    page_content += '\n'
    return {
        "url": url,
        "page_content": page_content
    }
    # return "".join([s for s in soup.text.splitlines(True) if s.strip()])


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
    spider = ADSpider(str(args.url))
    spider.run()

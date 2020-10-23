import requests
import re
import unicodedata
from bs4 import BeautifulSoup
from bloom_filter import ScalableBloomFilter

article_url = "https://news.sina.com.cn/s/2020-10-23/doc-iiznezxr7629012.shtml"
initial_page = "http://news.sina.com.cn/china/"


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


def scratch_links(initial_url):
    url_count = 0
    page_links = []
    while url_count <= 5000:
        res = requests.get(initial_url)
        res.encoding = "utf8"
        soup = BeautifulSoup(res.text, 'html.parser')
        text = str(soup.select('a[target="_blank"]'))
        # 正则获取所有url链接
        all_links = re.findall(r'(?<=<a href=\").*?(?=\")|(?<=href=\').*?(?=\')', text)
        bf = ScalableBloomFilter(initial_capacity=10000, error_rate=0.001)
        for link in all_links:
            if link not in bf:
                bf.add(link)
                page_links.append(link)
    return page_links


if __name__ == "__main__":
    result = scratch_links("http://news.sina.com.cn/china/")
    print(result)

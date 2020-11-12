import os
import multiprocessing


class FileOperator:
    output_dir = os.path.abspath(os.path.join(os.getcwd(), "PageData"))
    tar_file_size = 100 * 1024 * 1024
    count = 0

    def __init__(self, output_dir=output_dir, tar_file_size=tar_file_size, count=count):
        self.output_dir = output_dir
        self.tar_file_size = tar_file_size
        self.count = count

    def write_data(self, mode: str = 'a', data: dict = None):
        if data is None:
            return
        tar_file = os.path.join(self.output_dir, "%07d.txt" % self.count)
        file_check(tar_file)
        data_size = os.path.getsize(tar_file)
        while data_size > self.tar_file_size:
            self.count += 1
            tar_file = os.path.join(self.output_dir, "%07d.txt" % self.count)
            file_check(tar_file)
            data_size = os.path.getsize(tar_file)
        with open(tar_file, mode, encoding='utf8') as f:
            f.write("title:" + data.get("title") + '\n')
            f.write("url:" + data.get("url") + '\n')
            f.write("date:" + data.get("date") + '\n')
            f.write("content:\n" + data.get("content") + '\n')
            print("%s has writen one page(url:%s) into file: %s" % (
                multiprocessing.current_process(), data.get('url'), tar_file))

    def write_url(self, mode: str = 'a', url: str = None):
        if url is None:
            return
        tar_file = os.path.join(self.output_dir, "url_list.txt")
        file_check(tar_file)
        with open(tar_file, mode, encoding='utf8') as f:
            f.write(url + '\n')
            print("%s has writen one url:%s into url_list file" % (multiprocessing.current_process, url))

    def read_url(self, mode: str = 'r'):
        url_list = []
        tar_file = os.path.join(self.output_dir, "url_list.txt")
        if not os.path.exists(tar_file):
            return url_list
        with open(tar_file, mode, encoding='utf8') as f:
            for url in f:
                url_list.append(url)
        return url_list


def file_check(file_path):
    file_dir = os.path.split(file_path)[0]
    if not os.path.exists(file_dir):
        os.makedirs(file_dir)
    if not os.path.exists(file_path):
        os.system(r'touch %s' % file_path)


if __name__ == "__main__":
    file_o = FileOperator()

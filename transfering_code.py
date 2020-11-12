import os
import chardet

tar_file_path = '/home/ray/HHD/SpiderData/0000004.txt'


def detectCode(filename):
    count = 0
    with open(filename, 'r') as f:
        for line in f:
            try:
                line.encode("gb2312")
            except UnicodeError:
                count += 1
    print(count)


if __name__ == '__main__':
    detectCode(tar_file_path)

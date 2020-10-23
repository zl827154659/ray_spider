import gzip
s = '\x08\x07\x08\x06\x05'
res = gzip.decompress(s).decode('utf-8')
print(res)
from bs4 import BeautifulSoup
import urllib.request
import re
import time
import random
import os
import sys


# 收集钱包的所有地址
def _progress(block_num, block_size, total_size):
    '''回调函数reporthook'''
    sys.stdout.write('\r>> Downloading  %.1f%%' % (
            float(block_size * block_num) / float(total_size) * 100.0))
    sys.stdout.flush()


def addr_collection(company):
    # company = 'CloudBet.com'
    url = 'https://www.walletexplorer.com/wallet/' + company + '/addresses?page=1'
    head = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0'
    }
    addr_response = urllib.request.urlopen(urllib.request.Request(url, headers=head))
    html = addr_response.read().decode('utf-8')
    # print(html)
    soup = BeautifulSoup(html, 'lxml')
    total_address = soup.small.text
    print(company, total_address)
    paging = soup.findAll('div', {'class': 'paging'})
    match = re.search(r'Page\s(\d+)\s/\s(\d+)', str(paging[0]))
    current_page, all_page = int(match.group(1)), int(match.group(2))
    print(current_page, all_page)
    root_path = 'D:/raw_addr_csv/'
    alter_path = os.path.join(root_path, company)
    if not os.path.exists(alter_path):
        os.makedirs(alter_path)
    for _ in range(all_page // 100):
        #     将csv文件下载到本地
        try:
            download_url = 'https://www.walletexplorer.com/wallet/' + company + '/addresses?page=' + str(
                current_page) + '&format=csv'
            filename = os.path.join(alter_path, str(current_page) + '.csv')
            print('正在下载第{}个文件'.format(current_page), end='')
            urllib.request.urlretrieve(download_url, filename, _progress)
        except:
            print('第{}个文件下载失败！'.format(current_page))
        finally:
            current_page += 1
            time.sleep(random.uniform(0, 0.5))


if __name__ == '__main__':
    com_list = ['CoinGaming.io']
    for company in com_list:
        addr_collection(company)

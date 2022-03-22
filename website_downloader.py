# -*- coding: utf-8 -*-

"""
网站下载器
"""

__author__ = 'StrayingCloud'

import threading
from queue import Queue, Empty
from urllib import request
from urllib import parse
from http import cookiejar
from urllib.request import Request, urljoin, urlretrieve, urlparse
import time
import re
import socket
import os
import logging
import shutil
import ssl
import traceback
from PyPDF2 import PdfFileReader
import argparse


THREAD_NUM = 32  # 默认开启的爬虫线程数
TRY_ERROR_LINK_THREAD_NUM = 3  # 重新处理异常链接的爬虫线程数
SPIDER_GET_LINK_TIMEOUT = 5  # 秒，获取链接的轮询时间
SOCKET_DEFAULT_TIMEOUT = 5 * 60  # 秒, socket 默认超时
SOCKET_DOWNLOAD_TIMEOUT = 1 * 60 * 60  # 秒，下载默认超时
MAX_TRY = 6  # 每个请求最大尝试次数
PORT = 80  # 网络端口
ADD_HTML_SUFFIX = True  # 如果没有后缀则补'.html'
# VIEWER_FILE_TO_LOCAL = True  # 将viewer.html打开的文件替换为本地直接路径，不带viewer.html工具

# 直接下载的其他文件格式
OTHER_SUFFIXES = set([
    'js', 'jpg', 'png', 'gif', 'svg', 'json', 'xml', 'ico', 'jpeg', 'ttf', 'mp3', 'mp4', 'wav',
    'doc', 'xls', 'pdf', 'docx', 'xlsx', 'eot', 'woff', 'csv', 'swf', 'tar', 'gz', 'zip', 'rar', 'txt',
    'exe', 'ppt', 'pptx', 'm3u8', 'avi', 'wsf'
])
MEDIA_SUFFIXES = set(['mp3', 'mp4', 'pdf', 'gz', 'tar', 'zip', 'rar', 'wav', 'm3u8', 'avi'])
# 域名名称
DOMAIN_NAME = set(['com', 'cn', 'net', 'org', 'gov', 'io'])
# html 内容里的链接匹配
HTML_PATTERN = re.compile(r'(href|src)=(\"|\')([^\"\']*)')
# css 内容里的链接匹配
CSS_PATTERN = re.compile(r'url\((\"|\')([^\"\']*)')


def init_opener():
    cookie = cookiejar.CookieJar()
    cookie_support = request.HTTPCookieProcessor(cookie)
    return request.build_opener(cookie_support)


# 使能控制台及文件打印日志
def init_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    file_handler = logging.FileHandler('log.log', mode='w', encoding='utf-8')
    file_handler.setLevel(logging.NOTSET)
    formatter = logging.Formatter('%(asctime)s[%(levelname)s] %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger


opener = init_opener()
logger = init_logger()

# 忽略证书验证
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context


class Spider(threading.Thread):
    '''
    爬虫线程，从爬虫管理器的链接队列获取链接，然后进行处理，保存链接文件，为主管理器提供新链接。

    参数：
        queue:      Queue,  主管理器的链接队列；
        home_dir:   str,    网站保存的文件home路径；
        netloc:     str,    网站点，用于判断链接是否同一网站。

    例子：
        url = 'http://www.daorenjia.com'
        url_parse = urlparse(url)
        home_dir = "{}-site/".format(url_parse.netloc)
        netloc = url_parse.netloc

        s = Spider(None, home_dir, netloc)
        f = s.handle_html('http://www.daorenjia.com/daozang11-408')
        logger.info(f)
    '''

    def __init__(self, queue, home_dir, netloc):
        threading.Thread.__init__(self)
        self.daemon = True
        self.queue = queue
        self.links = []
        self.running = False
        self.working = False
        self.text_code = 'utf-8'
        self.home_dir = home_dir
        self.netloc = netloc
        self.error_links = set()

    def get_res(self, link):
        '''
        获取 html 、 css 链接的响应
        '''
        socket.setdefaulttimeout(SOCKET_DEFAULT_TIMEOUT)
        num_tries = 0
        # 多次尝试获取
        while num_tries < MAX_TRY:
            try:
                res = opener.open(Request(link)).read()
                break
            except Exception as e:
                num_tries += 1
                logger.warning('[{}]\t {} retry{}'.format(repr(e), link, num_tries))

        if num_tries >= MAX_TRY:
            logger.error('[failed get]\t{0}'.format(link))
            self.error_links.add(link)
            return None
        # 解码响应内容
        try:
            text = res.decode('utf-8')
            self.text_code = 'utf-8'
            return text
        except UnicodeDecodeError:
            pass
        try:
            text = res.decode('gb2312')
            self.text_code = 'gb2312'
            return text
        except UnicodeDecodeError:
            pass
        try:
            text = res.decode('gbk')
            self.text_code = 'gbk'
            return text
        except UnicodeDecodeError:
            pass
        self.text_code = None
        logger.warning('[UnicodeDecodeError]\t{0}'.format(link))
        return None

    def is_valid_link(self, link):
        '''
        检测有效链接
        嵌入的 data:image 图片不作为新链接，且过滤非本网站的链接
        '''
        if link.find('javascript:') >= 0 or link.find('@') >= 0 or link.find('data:image') >= 0:
            return False
        if link.find('http') >= 0:
            netloc = urlparse(link).netloc
            if netloc:
                if netloc.find(':{}'.format(PORT)) > 0:
                    netloc = netloc.replace(':{}'.format(PORT), '')
                # return netloc[netloc.find('.') + 1:] == self.netloc[self.netloc.find('.') + 1:]
                return netloc == self.netloc
        return True

    def handle_valid_link(self, link):
        link = parse.unquote(link)
        link = link.replace('\\', '/')

        # 转换viewer file文件路径为下载路径
        link = self.get_viewer_file_link(link)
        return link

    def save_link_file(self, link, content):
        # 获取本地路径
        filepath = self.make_filepath(link)
        # 保存文件
        if os.path.exists(filepath):
            logger.info('Existed\t{}'.format(filepath))
        else:
            with open(filepath, 'w', encoding=self.text_code) as fp:
                fp.write(content)
                logger.info('Saved\t{}'.format(filepath))
        logger.info('Handled\t{}'.format(link))

    def make_filepath(self, link):
        '''
        把链接创建为本地网站文件夹的绝对路径
        '''
        # 需要的话创建新文件夹
        abs_filepath = self.get_abs_filepath(link)
        dirname = os.path.dirname(abs_filepath)
        if not os.path.exists(dirname):
            try:
                os.makedirs(dirname)
            except FileExistsError:
                pass
            except NotADirectoryError:
                logger.error('[NotADirectoryError]\t{0}\t{1}'.format(link, abs_filepath))
        return abs_filepath

    def encode_link(self, link):
        '''
        为link编码，方便本地保存相对路径及本地网页跳转
        '''
        link = parse.unquote(link)
        link = parse.quote(link)

        # # ['+', ' ', '/', '?', '%', '#', '&', '=']
        # codes = ['%2B', '%20', '%2F', '%3F', '%25', '%23', '%26', '%3D']

        # ['+', ' ', '?', '%', '&', '=']
        codes = ['%2B', '%20', '%3F', '%25', '%26', '%3D']
        # insert '-', as '%2B' to '%2-B'
        for c in codes:
            if c in link:
                link = link.replace(c, c[:2] + '-' + c[2])

        link = parse.unquote(link)
        return link

    def decode_line(self, link):
        codes = ['%2-B', '%2-0', '%2-F', '%3-F', '%2-5', '%2-3', '%2-6', '%3-D']
        # remove nsert '-', as '%2-B' to '%2B'
        for c in codes:
            if c in link:
                link = link.replace(c, c[:2] + c[-1])
        return link

    def get_abs_filepath(self, link):
        '''
        获取链接绝对路径
        '''
        link = self.encode_link(link)

        # 如果链接为非文件，则补上index.html
        if link[-1] == '/':
            link += 'index.html'
        elif link.split('.')[-1] in DOMAIN_NAME:
            link += '/index.html'

        if ADD_HTML_SUFFIX:
            # 如果没有文件后缀，或者非正常后缀带'%', 则补上'.html'
            if link.split('/')[-1].find('.') < 0 or ('%' in link.split('/')[-1].split('.')[-1]):
                link += '.html'

        abs_filepath = os.path.join(self.home_dir, link[link.find('//') + 2:])
        return abs_filepath

    def replace_links(self, content, links, current_link):
        curr_link_path = self.get_abs_filepath(current_link)
        for link in set(links):
            handled_link = self.handle_valid_link(link)
            new_link = urljoin(current_link, handled_link)

            # 如果带有#符号，将链接与#号分开，获取路径后再拼接
            if '#' in new_link:
                split = new_link.split('#')
                vlink = split[0]
                flag = split[1]
                new_link_path = self.get_abs_filepath(vlink)
                new_link_path += '#{}'.format(flag)
            else:
                new_link_path = self.get_abs_filepath(new_link)

            rel_link = os.path.relpath(new_link_path, os.path.dirname(curr_link_path))
            rel_link = rel_link.replace('\\', '/')

            # 保留使用viewer.html工具打开文件
            # if (not VIEWER_FILE_TO_LOCAL) and self.is_viewer_file_link(link):
            #     rel_link = self.get_viewer_file_rellink(rel_link)

            replacement = '"{}"'.format(rel_link)
            content = content.replace(
                '"{}"'.format(link), replacement
            ).replace('\'{}\''.format(link), replacement)
        return content

    def handle_html(self, link):
        # 获取链接文本
        text = self.get_res(link)
        if text is None:
            return []
        # 提取文本中的链接
        raw_links = set([_[2] for _ in HTML_PATTERN.findall(text)])
        raw_links = raw_links.union([_[1] for _ in CSS_PATTERN.findall(text)])
        handled_links = []
        if raw_links:
            # 提取有效的链接
            valid_links = list(filter(self.is_valid_link, raw_links))
            # 对有效的链接进行处理
            handled_links = list(map(self.handle_valid_link, valid_links))
            # 替换 text 内容里的链接为本地网站文件夹里的相对路径
            text = self.replace_links(text, valid_links, link)
        # 保存 text 文件
        self.save_link_file(link, text)

        # 获取有效的链接供主线程爬虫管理器获取
        links = set([urljoin(link, t_link) for t_link in handled_links])
        return links

    def get_viewer_file_link(self, link):
        '''
        如果是viewer链接，获取指定的文件路径
        '''
        link = parse.unquote(link)
        if 'viewer.html?file=' in link:
            link = link.split('viewer.html?file=')[-1]
        return link

    def is_viewer_file_link(self, link):
        return 'viewer.html' in link

    def get_viewer_file_rellink(self, link):
        '''
        获取viewer file的路径名
        '''
        ulink = parse.unquote(link)
        if 'viewer.html?file=' in ulink:
            # 相对与viewer的路径
            link = ulink.split('viewer.html?file=')[0] + 'viewer.html?file=..' + ulink.split('viewer.html?file=')[1]
        link = 'web/viewer.html?file={}'.format(link)
        return link

    def is_pdf_valid(self, path):
        state = True
        try:
            reader = PdfFileReader(path)
            if reader.getNumPages() < 1:
                state = False
        except Exception:
            state = False
            logger.warning('{}, {}'.format(path, traceback.format_exc()))
        return state

    def download(self, link):
        '''
        直接下载链接文件
        '''
        socket.setdefaulttimeout(SOCKET_DEFAULT_TIMEOUT)
        if link.split('.')[-1].lower() in MEDIA_SUFFIXES:
            socket.setdefaulttimeout(SOCKET_DOWNLOAD_TIMEOUT)
        num_tries = 0
        # 多次尝试下载
        while num_tries < MAX_TRY:
            try:
                file_path = self.make_filepath(link)

                # 如果文件存在则不重新下载
                if os.path.exists(file_path):
                    # 判断PDF文件是否有效
                    if not file_path.endswith('.pdf') or self.is_pdf_valid(file_path):
                        logger.info('exists \t{0}'.format(link))
                        return
                urlretrieve(link, file_path)
                break
            except Exception as e:
                num_tries += 1
                logger.warning('[{}]\t {} retry{}'.format(repr(e), link, num_tries))

        if num_tries >= MAX_TRY:
            logger.error('[failed download]\t{0}'.format(link))
            self.error_links.add(link)
        else:
            logger.info('Downloaded\t{0}'.format(link))

    def handle(self, link):
        logger.info("{}, handle: {}".format(threading.current_thread().name, link))
        links = []

        # 获取链接类型
        link_type = link.split('.')[-1].lower()
        # 判断链接类型
        if link_type in OTHER_SUFFIXES:
            self.download(link)
        else:
            links = self.handle_html(link)
        return links

    def run(self):
        logger.info('{} start.'.format(threading.current_thread().name))
        self.running = True
        self.working = True
        while self.running:
            try:
                # 如果links不为空，则等待管理器获取
                if len(self.links):
                    logger.info('{} - links not empty, waiting...'.format(threading.current_thread().name))
                    time.sleep(0.1)
                    continue

                # 从管理器queue获取link
                link = self.queue.get(timeout=SPIDER_GET_LINK_TIMEOUT)
                logger.info('{} - queue size: {}'.format(threading.current_thread().name, self.queue.qsize()))
                self.working = True
                # 处理link，得到links
                self.links = set(self.handle(link))
            except Empty:
                self.working = False
        logger.info('{} end.'.format(threading.current_thread().name))

    def is_working(self):
        '''
        提供管理器判断是否工作完成
        '''
        return self.working

    def close(self):
        '''
        提供管理器退出爬虫线程
        '''
        self.running = False

    def get_links(self):
        '''
        提供管理器重新放入爬虫队列
        '''
        # 获取且清空links
        links = self.links
        self.links = []
        return links

    def get_error_links(self):
        '''
        提供主线程爬虫管理器再次放入爬虫链接队列
        '''
        export_links = self.error_links.copy()
        self.error_links.clear()
        return export_links


class Manager(object):
    '''
    爬虫主管理器，开启爬虫线程，提供链接队列于爬虫线程获取，遍历爬虫线程收取新链接，然后提交到队列。

    参数：
        url:    str,    网站地址，格式如为'http://www.xxx.com'.

    例子：
        url = 'http://www.daorenjia.com'
        m = Manager(url)
        m.start()

    '''

    def __init__(self, url):
        self.url = url.replace('\\', '/')
        self.links = set()
        self.queue = Queue()
        self.spiders = []

    def is_valid_link(self, link):
        '''
        判断是否url下的链接
        '''
        # if link.find('http') >= 0:
        #     netloc = urlparse(link).netloc
        #     if netloc:
        #         if netloc.find(':{}'.format(PORT)) > 0:
        #             netloc = netloc.replace(':{}'.format(PORT), '')
        #         # return netloc[netloc.find('.') + 1:] == self.netloc[self.netloc.find('.') + 1:]
        #         return netloc == self.netloc
        return self.url in link

    def handle_link(self, link):
        '''
        处理链接，去掉#号的链接
        '''
        sharp_index = link.find('#')
        if sharp_index > 0:
            logger.info("-{}".format(link))
            link = link[0:sharp_index]
        return link

    def start(self):
        url_parse = urlparse(self.url)
        home_dir = "{}-site/".format(url_parse.netloc)
        netloc = url_parse.netloc

        # 删除home目录文件夹
        # if os.path.exists(home_dir):
        #     logger.info("rm home dir: {}".format(home_dir))
        #     shutil.rmtree(os.path.dirname(home_dir))

        # 创建home目录文件夹
        if not os.path.exists(home_dir):
            os.makedirs(home_dir)

        logger.info("start...")
        # 新建且启动多个爬虫线程
        logger.info("Thread number: {}".format(THREAD_NUM))
        [self.spiders.append(Spider(self.queue, home_dir, netloc)) for i in range(THREAD_NUM)]
        [spider.start() for spider in self.spiders]

        # 将网址放入队列
        self.links.add(self.url)
        self.queue.put(self.url)

        retry_error_link = True  # 用于判断是否将异常链接重新放入工作队列
        while True:
            # 遍历爬虫列表，获取结果，将新的link放入队列
            for spider in self.spiders:
                new_links = spider.get_links()
                for link in new_links:
                    link = self.handle_link(link)
                    if self.is_valid_link(link) and (link not in self.links):
                        self.links.add(link)
                        self.queue.put(link)
            # 判断是否工作结束
            # 如果队列为空，且爬虫线程都没有在工作，则结束
            if self.queue.empty():
                state = [spider.is_working() for spider in self.spiders]
                if True in state:
                    logger.info('queue is empty, but {} spider working.'.format(state.count(True)))
                    logger.info('{}'.format(state))
                    time.sleep(SPIDER_GET_LINK_TIMEOUT)
                else:
                    logger.info('queue is empty, and no spider working.')

                    # 从子线程获取失败链接
                    error_links = set()
                    for spider in self.spiders:
                        error_links |= spider.get_error_links()
                    logger.info('error links: {}, len={}'.format(error_links, len(error_links)))

                    ''' 将失败的链接重新加入队列，仅一次 '''
                    if retry_error_link and len(error_links):
                        # 若有失败连接，则重新放入队列，且保留少量线程
                        logger.info("Thread reduce to {}, and try error link again.".format(TRY_ERROR_LINK_THREAD_NUM))
                        for spider in self.spiders[TRY_ERROR_LINK_THREAD_NUM:]:
                            spider.running = False  # 退出线程
                        time.sleep(SPIDER_GET_LINK_TIMEOUT)
                        self.spiders = self.spiders[:TRY_ERROR_LINK_THREAD_NUM]
                        [self.queue.put(link, True) for link in error_links]

                        retry_error_link = False

                        time.sleep(0.1)
                        logger.info('continue.')
                        continue
                    logger.info('break.')
                    break

        # 释放爬虫线程资源
        [spider.close() for spider in self.spiders]
        time.sleep(SPIDER_GET_LINK_TIMEOUT + 2)

        # 响铃提醒下载完成
        for i in range(6):
            print('\a')
            time.sleep(0.5)

        logger.info("finish.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--url', help="url link, default='http://www.daorenjia.com/'",
                        type=str, default='http://www.daorenjia.com/')
    args = parser.parse_args()

    # url = "https://zhms8.com/tag/daojiadianji/"
    # url = 'http://www.daorenjia.com/'
    url = args.url
    m = Manager(url)
    m.start()

    # url_parse = urlparse(url)
    # home_dir = "{}-site/".format(url_parse.netloc)
    # netloc = url_parse.netloc
    # s = Spider(None, home_dir, netloc)
    # f = s.handle_html('http://www.daorenjia.com/daozang11-408')
    # logger.info(f)

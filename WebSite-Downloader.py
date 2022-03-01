# -*- coding: utf-8 -*-

"""
网站下载器
"""
__author__ = 'StrayingCloud'

from urllib import request, error
from urllib.request import Request, urljoin, urlretrieve, urlparse
from urllib import parse
import os
import re
import time
import threading
import http
from http import cookiejar
from queue import Queue, Empty
import logging
import traceback
from PyPDF2 import PdfFileReader
import io
import sys
import argparse
import socket
import ssl

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')  # 改变标准输出的默认编码

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context


SOCKET_DEFAULT_TIMEOUT = 5 * 60  # 秒, socket 默认超时
SOCKET_DOWNLOAD_TIMEOUT = 1 * 60 * 60  # 秒，下载默认超时
SPIDER_GET_LINK_TIMEOUT = 10  # 秒，获取链接的轮询时间
THREAD_NUM = 64  # 默认开启子线程数
SECONDE_THREAD_NUM = 3  # 重新跑失败链接的子线程数
MAX_TRY = 6  # 每个请求最大尝试次数
PORT = 80  # 网络端口


def init_opener():
    cookie = cookiejar.CookieJar()
    cookie_support = request.HTTPCookieProcessor(cookie)
    return request.build_opener(cookie_support)


def init_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    file_handler = logging.FileHandler('log.log', mode='w', encoding='UTF-8')
    file_handler.setLevel(logging.NOTSET)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger


opener = init_opener()
logger = init_logger()


class Manager:
    """
    爬虫主线程的管理器
    从子线程里获取新的链接，处理后添加进要爬取的链接 Queue 队列
    子线程从主线程提供的链接 Queue 队列获取链接进行爬取
    """

    def __init__(self, home_url):
        # 爬取网站域名的各个子域名

        # 下载的网站的根文件夹，网站可能有不同子域名，提供一个更高级的文件夹路径 -site

        home_dir = '{0}-site/{1}'.format(home_url.split('.')[1], home_url.split('//')[1])
        # home_dir = '/Users/liebeu/Desktop/localhost-site/localhost'

        # if os.path.exists(home_dir):
        #     shutil.rmtree(os.path.dirname(home_dir))
        # os.makedirs(home_dir)

        parsed_url = urlparse(home_url)
        scheme = parsed_url.scheme
        # 爬取的网站的顶级域名
        top_domain = '.'.join(parsed_url.netloc.split('.')[1:])
        # 每个请求最大尝试次数
        max_tries = MAX_TRY

        # 要爬取的链接 Queue 队列
        self.link_queue = Queue()
        self.link_queue.put(home_url)
        # 链接 set ，对新连接进行唯一性判断，然后添加进 Queue 队列
        self.links = set([home_url])
        self.error_link = set()  # 对失败链接重新放入队列，然后设为None
        # 子线程爬虫列表
        self.spiders = []

        for i in range(THREAD_NUM):
            self.spiders.append(Spider(home_dir, home_url, self.link_queue, scheme, top_domain, max_tries))

    def start(self):
        """
        开启主线程的爬虫管理器
        """
        for spider in self.spiders:
            spider.start()
        # 从子线程获取新链接，添加进 Queue 队列
        while True:
            for spider in self.spiders:
                new_links = spider.get_links()
                for link in new_links:
                    if link not in self.links and len(link) < 250:
                        sharp_index = link.find('#')
                        if sharp_index > 0:
                            origin_link = link
                            link = link[0:sharp_index]
                        if link in self.links:
                            logger.info('+++ {}'.format(origin_link))
                            continue
                        self.links.add(link)
                        self.link_queue.put(link, True)
            # 子线程都不在工作状态，及Queue为空，则完成，结束
            if True not in [spider.working for spider in self.spiders] and self.link_queue.empty():
                logger.info('no spider working and queue is empty.')

                ''' 将失败的链接重新加入队列，仅一次 '''
                if None is not self.error_link:
                    # 从子线程获取失败链接
                    for spider in self.spiders:
                        self.error_link |= spider.get_error_links()
                    # 若有失败连接，则重新放入队列，且保留少量线程
                    if len(self.error_link):
                        logger.info("Thread reduce to {}, and try error link again.".format(SECONDE_THREAD_NUM))
                        for spider in self.spiders[SECONDE_THREAD_NUM:]:
                            spider.running = False  # 退出线程
                        time.sleep(SPIDER_GET_LINK_TIMEOUT)
                        self.spiders = self.spiders[:SECONDE_THREAD_NUM]
                        [self.link_queue.put(link, True) for link in self.error_link]
                        self.error_link = None
                        time.sleep(0.1)
                        continue

                for spider in self.spiders:
                    spider.running = False
                time.sleep(SPIDER_GET_LINK_TIMEOUT)
                break
            time.sleep(0.0001)
        # 响铃提醒下载完成
        for i in range(10):
            print('\a')
            time.sleep(0.5)
        logger.info("finish.")


class Spider(threading.Thread):
    """
    爬虫线程
    从主线程获取链接进行爬取，并处理 html 、css 文件获取新链接，以及直接下载其他文件
    """

    def __init__(self, home_dir, home_url, link_queue, scheme, top_domain, max_tries):
        threading.Thread.__init__(self)
        self.daemon = True
        self.home_dir = home_dir
        self.home_url = home_url
        self.link_queue = link_queue
        self.scheme = scheme
        self.top_domain = top_domain
        self.max_tries = max_tries
        # 直接下载的其他文件格式
        self.other_suffixes = set([
            'js', 'jpg', 'png', 'gif', 'svg', 'json', 'xml', 'ico', 'jpeg', 'ttf', 'mp3', 'mp4', 'wav',
            'doc', 'xls', 'pdf', 'docx', 'xlsx', 'eot', 'woff', 'csv', 'swf', 'tar', 'gz', 'zip', 'rar', 'txt',
            'exe', 'ppt', 'pptx', 'm3u8', 'avi', 'wsf'
        ])
        self.media_suffixes = set(['mp3', 'mp4', 'pdf', 'gz', 'tar', 'zip', 'rar', 'wav', 'm3u8', 'avi'])
        # 域名名称
        self.domain_names = set(['com', 'cn', 'net', 'org', 'gov', 'io'])
        # html 内容里的链接匹配
        self.html_pat = re.compile(r'(href|src)=(\"|\')([^\"\']*)')
        # css 内容里的链接匹配
        self.css_pat = re.compile(r'url\((\"|\')([^\"\']*)')

        self.links = set()

        self.working = False  # 判断是否在工作中，用于主线程检测.
        self.running = True  # 提供主线程修改状态，让子线程退出
        self.error_link = set()

    def run(self):
        logger.info('{} start.'.format(threading.current_thread().name))
        # 尝试从主线程的链接队列获取新链接，10秒轮询一次
        while self.running:
            try:
                if len(self.links) != 0:
                    logger.info('{} - links not empty, waiting...'.format(threading.current_thread().name))
                    time.sleep(0.0001)
                    continue
                link = self.link_queue.get(timeout=SPIDER_GET_LINK_TIMEOUT)
                logger.info('{} - queue size: {}'.format(threading.current_thread().name, self.link_queue.qsize()))
                self.working = True
                self.spide(link)
            except Empty:
                self.working = False
        logger.info('{} end.'.format(threading.current_thread().name))

    def spide(self, link):
        # 爬取链接，对不同链接不同处理
        try:
            suffix = link.split('.')[-1].lower()
            if suffix == 'css':
                self.handle_css(link)
            elif suffix in self.other_suffixes:
                self.download(link)
            else:
                self.handle_html(link)
        except Exception:
            logger.error('[Unknown Error]\t{0}'.format(link))
            logger.error('----traceback:{0}'.format(traceback.format_exc()))

    def get_quote_link(self, link):
        # 替换'?'及'='等字符，以便文件名支持保存
        link = parse.unquote(self.normalize_link(link))
        if '://' in link:
            link = link.split('://')[0] + '://' + parse.quote(link.split('://')[1])
        else:
            link = parse.quote(link)
        return link

    def encode_link(self, link):
        '''
        为link编码，方便本地保存相对路径及本地网页跳转
        '''
        # # ['+', ' ', '/', '?', '%', '#', '&', '=']
        # codes = ['%2B', '%20', '%2F', '%3F', '%25', '%23', '%26', '%3D']
        # ['+', ' ', '/', '?', '%', '&', '=']
        codes = ['%2B', '%20', '%2F', '%3F', '%25', '%26', '%3D']
        # insert '-', as '%2B' to '%2-B'
        for c in codes:
            if c in link:
                link = link.replace(c, c[:2] + '-' + c[2])
        return link

    def decode_line(self, link):
        codes = ['%2-B', '%2-0', '%2-F', '%3-F', '%2-5', '%2-3', '%2-6', '%3-D']
        # remove nsert '-', as '%2-B' to '%2B'
        for c in codes:
            if c in link:
                link = link.replace(c, c[:2] + c[-1])
        return link

    def save_link_file(self, link, content):
        qlink = self.get_quote_link(link)
        qlink = self.encode_link(qlink)
        f_name = self.make_filepath(qlink)
        if os.path.exists(f_name):
            logger.info('exists \t{0}'.format(link))
            return None
        with open(f_name, 'w', encoding=self.text_code) as f_w:
            f_w.write(content)
        logger.info('Handled\t{0}'.format(link))

    def handle_html(self, link):
        # 处理 html 链接
        html = self.get_res(link)
        if html is None:
            return
        html_raw_links = set([ele[2] for ele in self.html_pat.findall(html)])
        html_raw_links = html_raw_links.union([ele[1] for ele in self.css_pat.findall(html)])
        if html_raw_links:
            # 提取有效的链接
            valid_links = list(filter(self.is_valid_link, html_raw_links))
            # 对有效的链接进行处理
            handled_links = list(map(self.handle_valid_link, valid_links))
            # 把有效的链接放入线程的 links ，供主线程爬虫管理器获取
            self.links = self.links.union([urljoin(link, t_link) for t_link in handled_links])
            # 替换 html 内容里的链接为本地网站文件夹里的相对路径
            html = self.replace_links(html, valid_links, self.normalize_link(link))
        # 保存 html 文件
        self.save_link_file(link, html)

    def handle_css(self, link):
        """
        处理 css 链接
        """
        text = self.get_res(link)
        if text is None:
            return
        css_raw_links = set([ele[1] for ele in self.css_pat.findall(text)])
        if css_raw_links:
            css_raw_links = list(filter(self.is_valid_link, css_raw_links))
            self.links = self.links.union([urljoin(link, t_link) for t_link in css_raw_links])
            text = self.replace_links(text, css_raw_links, self.normalize_link(link))
        self.save_link_file(link, text)

    def is_valid_link(self, link):
        """
        检测有效链接
        嵌入的 data:image 图片不作为新链接
        os.path.relpath 返回值最前面多一个 . 需要删掉
        """
        if link.find('javascript:') >= 0 or link.find('@') >= 0 or link.find('data:image') >= 0:
            return False
        if link.find('http') >= 0:
            netloc = urlparse(link).netloc
            if netloc:
                if netloc.find(':80') > 0:
                    netloc = netloc.replace(':80', '')
                return netloc[netloc.find('.') + 1:] == self.top_domain
        return True

    def handle_valid_link(self, link):
        """
        处理链接的错误 协议 写法
        http:www.baidu.com http:/www.baidu.com 转换为 http://www.baidu.com
        """
        if not link:
            return link
        if link[0:2] == '//':
            return self.scheme + link
        if link[0] == '/':
            return urljoin(self.home_url, link)
        if link.find('http') < 0 or link.find('http://') >= 0 or link.find('https://') >= 0:
            return link
        if link.find('http:/') >= 0 or link.find('https:/') >= 0:
            return link.replace(':/', '://')
        if link.find('http:') >= 0 or link.find('https:') >= 0:
            first_colon = link.find(':')
            link = link[0:first_colon] + '://' + link[first_colon + 1:]
            return link
        return link

    def get_res(self, link):
        """
        获取 html 、 css 链接的响应
        """
        socket.setdefaulttimeout(SOCKET_DEFAULT_TIMEOUT)
        num_tries = 0
        # 多次尝试获取
        while num_tries < self.max_tries:
            try:
                res = opener.open(Request(link)).read()
                break
            except error.HTTPError:
                logger.warning('[error.HTTPError]\t{0}'.format(link))
                # return None
                num_tries += 1
            except error.URLError:
                logger.warning('[error.URLError]\t{0}'.format(link))
                # return None
                num_tries += 1
            except UnicodeEncodeError:
                logger.warning('[UnicodeEncodeError]\t{0}'.format(link))
                # return None
                num_tries += 1
            except http.client.BadStatusLine:
                logger.warning('[http.client.BadStatusLine]\t{0}'.format(link))
                # return None
                num_tries += 1
            except http.client.IncompleteRead:
                logger.warning('[http.client.IncompleteRead]\t{0}'.format(link))
                # return None
                num_tries += 1
            except TimeoutError:
                logger.warning('[TimeoutError]\t{0}'.format(link))
                num_tries += 1
            except socket.timeout:
                logger.warning('[socket.timeout]\t{0}'.format(link))
                num_tries += 1
            except http.client.RemoteDisconnected:
                logger.warning('[RemoteDisconnected]\t{0}'.format(link))
                num_tries += 1
            except ConnectionResetError:
                logger.warning('[ConnectionResetError]\t{0}'.format(link))
                num_tries += 1
        if num_tries >= self.max_tries:
            logger.error('[failed get]\t{0}'.format(link))
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
        logger.error('[UnicodeDecodeError]\t{0}'.format(link))
        return None

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
        """
        直接下载其他格式的文件
        """
        socket.setdefaulttimeout(SOCKET_DEFAULT_TIMEOUT)
        if link.split('.')[-1].lower() in self.media_suffixes:
            socket.setdefaulttimeout(SOCKET_DOWNLOAD_TIMEOUT)
        num_tries = 0
        # 多次尝试下载
        while num_tries < self.max_tries:
            try:
                # pdf 文件特别处理
                link = self.get_viewer_file_link(link)
                file_path = self.make_filepath(self.get_quote_link(link))
                if os.path.exists(file_path):
                    if not file_path.endswith('.pdf') or self.is_pdf_valid(file_path):
                        logger.info('exists \t{0}'.format(link))
                        return
                urlretrieve(link, file_path)
                break
            except error.HTTPError:
                logger.warning('[error.HTTPError]\t{0}'.format(link))
                # break
                num_tries += 1
            except error.URLError:
                logger.warning('[error.URLError]\t{0}'.format(link))
                # break
                num_tries += 1
            except UnicodeEncodeError:
                logger.warning('[UnicodeEncodeError]\t{0}'.format(link))
                # break
                num_tries += 1
            except http.client.BadStatusLine:
                logger.warning('[http.client.BadStatusLine]\t{0}'.format(link))
                # break
                num_tries += 1
            except http.client.IncompleteRead:
                logger.warning('[http.client.IncompleteRead]\t{0}'.format(link))
                # break
                num_tries += 1
            except TimeoutError:
                logger.warning('[TimeoutError]\t{0}'.format(link))
                num_tries += 1
            except socket.timeout:
                logger.warning('[socket.timeout]\t{0}'.format(link))
                num_tries += 1
            except http.client.RemoteDisconnected:
                logger.warning('[RemoteDisconnected]\t{0}'.format(link))
                num_tries += 1
            except ConnectionResetError:
                logger.warning('[ConnectionResetError]\t{0}'.format(link))
                num_tries += 1
        if num_tries >= self.max_tries:
            logger.error('[failed download]\t{0}'.format(link))
            self.error_link.add(link)
        else:
            logger.info('Downloaded\t{0}'.format(link))

    def make_filepath(self, link):
        """
        把链接创建为本地网站文件夹的绝对路径
        """
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

    def get_viewer_file_link(self, link):
        '''
        如果是viewer链接，获取指定的文件路径
        '''
        link = parse.unquote(link)
        if 'viewer.html?file=' in link:
            link = urljoin(self.home_url, link.split('viewer.html?file=')[1])
        return link

    def get_abs_filepath(self, link):
        """
        把链接转换为本地网站文件夹的绝对路径
        """
        link = link.replace('\\', '/')
        if link[-1] == '/':
            link += 'index.html'
        elif link.split('.')[-1] in self.domain_names:
            link += '/index.html'
        rel_url = os.path.relpath(link, self.home_url)
        # if rel_url.find('?') >= 0:
        #     rel_url += '.html'
        # if rel_url.split('/')[-1].find('.') < 0 or rel_url == '.':
        # rel_url += 'index.html'
        if rel_url == '.':
            rel_url = 'index.html'

        abs_filepath = os.path.join(self.home_dir, rel_url)
        if abs_filepath.find('..') > 0:
            parts = abs_filepath.split('..')
            abs_filepath = '/'.join(parts[0].split('/')[0:-2]) + parts[1]
        if os.path.isdir(abs_filepath):
            logger.warning('[isdir]\t{0}\t{1}'.format(link, abs_filepath))
            abs_filepath = os.path.join(abs_filepath, 'index.html')

        # 如果文件不带后缀或后缀带'%'符，则添加html后缀，为本地正常打开
        if abs_filepath.split('/')[-1].find('.') < 0 or abs_filepath.split('.')[-1].find('%') >= 0:
            abs_filepath += '.html'
        return abs_filepath

    def get_viewer_file_rellink(self, link):
        '''
        如果是viewer链接，需unquote且将指定的文件换成相对路径
        '''
        ulink = parse.unquote(link)
        if 'viewer.html?file=' in ulink:
            # 相对与viewer的路径
            link = ulink.split('viewer.html?file=')[0] + 'viewer.html?file=..' + ulink.split('viewer.html?file=')[1]
        return link

    def replace_links(self, content, links, cur_url):
        """
        替换 html 、 css 内容里的链接
        """
        links.sort(key=lambda link: len(link), reverse=True)
        for link in set(links):
            vlink = self.get_viewer_file_link(link)  # 如果是viewer打开的文件，替换成文件路径

            # 如果带有#符号，将链接与#号分开，获取路径后再拼接
            if '#' in parse.unquote(link):
                split = parse.unquote(link).split('#')
                vlink = split[0]
                flag = split[1]
            link_abspath = self.get_abs_filepath(self.get_quote_link(urljoin(cur_url, vlink)))
            if '#' in parse.unquote(link):
                link_abspath += '#{}'.format(flag)

            cur_url_abspath = self.get_abs_filepath(self.get_quote_link(cur_url))
            # rel_link = os.path.relpath(link_abspath, cur_url_abspath)[1:].replace('?', '%3F')
            rel_link = os.path.relpath(link_abspath, os.path.dirname(cur_url_abspath))
            # rel_link = self.get_viewer_file_rellink(rel_link)  # 如果是viewer链接，需特别处理
            # 编码link，与保存的文件一致
            rel_link = self.encode_link(rel_link)
            replacement = '"{0}"'.format(rel_link)
            content = content.replace(
                '"{0}"'.format(link), replacement
            ).replace('\'{0}\''.format(link), replacement)
        return content

    def normalize_link(self, link):
        if link.find('http') < 0:
            return link
        if link.find(':{}'.format(PORT)) > 0:
            link = link.replace(':{}'.format(PORT), '')
        first_colon = link.find(':')
        link = self.scheme + link[first_colon:]
        return link

    def get_links(self):
        """
        主线程爬虫管理器从这里获取爬虫子线程的新链接
        获取后子线程就删除旧链接，为后面获取的链接做准备
        """
        export_links = self.links.copy()
        self.links.clear()
        return export_links

    def get_error_links(self):
        """
        提供主线程爬虫管理器再次放入爬虫链接队列
        """
        export_links = self.error_link.copy()
        self.error_link.clear()
        return export_links


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--url', help="url link, default='http://www.daorenjia.com/'",
                        type=str, default='http://www.daorenjia.com/')
    args = parser.parse_args()

    # url = "https://zhms8.com/tag/daojiadianji/"
    # url = 'http://www.daorenjia.com/'
    url = args.url
    manager = Manager(url)
    manager.start()

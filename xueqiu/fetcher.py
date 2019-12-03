# encoding: utf-8

import threading
import logging
import time
from queue import Queue, Empty

import requests
from fake_useragent import UserAgent

def parseIPList():
    IPs = []
    response = requests.get("http://www.xicidaili.com/wt", headers={'User-Agent':ua.random}).text
    soup = BeautifulSoup(response, 'html.parser')
    ip_table = soup.find("table") # id='ip_list'
    trs = ip_table.findChildren(recursive=False)
    for tr in trs[1:]: #skip header
        tds = tr.findChildren(recursive=False)
        ip, port, htype = tds[1].string, tds[2].string, tds[5].string
        try:
            ipaddress.ip_address(ip)
            IPs.append((ip, port, htype.lower()))
        except Exception:
            logging.info(f'{ip} is not valid')
    return IPs

def getProxies(n, timeout=2):
    validProxies = []
    for ip, port, htype in parseIPList():
        p = f'{htype}://{ip}:{port}'
        proxy = {'http':p, 'https':p}
        headers = { 'User-Agent': ua.random }
        try:
            res = requests.get("https://xueqiu.com", proxies=proxy, headers=headers, timeout=timeout)  #  "https://api.ip.sb/jsonip" 
            if res.status_code == 200:
                validProxies.append(p)
                logging.info(p)
                if n and len(validProxies) >= n:
                    break
        except requests.exceptions.ProxyError:
            pass #logging.info(f"{ip}:{port} invalid")
        except Exception as e:
            pass #logging.info(e)
        #logging.info()
    return validProxies

# class limitSession(requests.Session):
#     def __init__(self, inter=2, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.inter = inter
#         self.last = None

#     def get(self, *args, **kwargs):
#         if self.inter > 0 and self.last:
#             gap = self.last + self.inter - time.time()
#             if gap > 0:
#                 time.sleep(gap)
#         self.last = time.time()
#         return super().get(*args, **kwargs)

def net_sessions(proxies):
    headers = {
        # 'Cookie':'aliyungf_tc=AQAAAGS4K2BVQAUA4ZpccSpfiQ0TAZzf; acw_tc=2760823615741450343547074ed6d3da1e3cefce0ec5bdc579105a4235c520; s=bp18s3trg8; xq_a_token=5e0d8a38cd3acbc3002589f46fc1572c302aa8a2; xq_r_token=670668eda313118d7214487d800c21ad0202e141; u=691574145060230; Hm_lvt_1db88642e346389874251b5a1eded6e3=1574145064; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1574145269; __utma=1.656725657.1574145064.1574145064.1574145064.1; __utmb=1.2.10.1574145064; __utmc=1; __utmz=1.1574145064.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utmt=1; device_id=07fedbd5879b248757ab08dce2d97298',
        # 'Cookie':'bid=47cef4283992c2b6bd935bdcd877e6dd_jz3m4fcj;device_id=7130403e5178e54376d598e21601f31d;Hm_lpvt_1db88642e346389874251b5a1eded6e3=1573721803;Hm_lvt_1db88642e346389874251b5a1eded6e3=1573698944;s=ck16erzsji;snbim_minify=true;u=8347941443;xq_a_token=f82dd58de09ef3d915f9c349e1f3addca313e848;xq_is_login=1;xq_r_token=2e436404e09579cda5d3d03887e43c9a7dafb0c2;xq_token_expire=Mon%20Dec%2009%202019%2011%3A01%3A28%20GMT%2B0800%20(China%20Standard%20Time);xqat=f82dd58de09ef3d915f9c349e1f3addca313e848;__utma=1.1900063763.1573721440.1573721440.1573721440.1;__utmc=1;__utmz=1.1573721440.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none);acw_tc=2760827d15736989415012656ec9f7dbd31c77c0672df7b1030da4054accec;aliyungf_tc=AQAAAMMMjiWbCw8AJON2ca5e8P94fS2f;',
        'Cookie': 'xq_a_token=5e0d8a38cd3acbc3002589f46fc1572c302aa8a2;', #anonymous user
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:70.0) Gecko/20100101 Firefox/70.0',
        'Accept': 'application/json, text/plain, */*',
        "Accept-Encoding": 'gzip, deflate, br',
        "Accept-Language": 'zh-CN,zh-TW;q=0.9,zh;q=0.8,en-US;q=0.7,en;q=0.6',
        'Connection': 'keep-alive',
        "Cache-Control": "max-age=0",
        "DNT": "1",
        "Host": "xueqiu.com",
        # "Referer": "https://google.com",
    }
    # proxies.extend(getProxies(3))

    ss = []
    for i, p in enumerate(proxies):
        session = requests.Session() #limitSession(interval) #
        session.headers = headers
        if p:
            session.proxies = {'http':p, 'https':p}
        if 'Cookie' not in session.headers:
            session.get('https://xueqiu.com')       #get cookie , xq_a_token
            logging.debug(session.cookies.get_dict())
        ss.append(session)
    
    return ss


class TrdFetcher():
    def __init__(self):
        self.job = Queue()
        self.out = Queue()
        self.threads = []
        self.log = logging.getLogger('Fetcher')

        self.ua = UserAgent()
    
    def start(self, fetcher, proxies, inter, autoua):
        self.ss = net_sessions(proxies)
        for s in self.ss:
            t = threading.Thread(target=self.sched_downloaders, 
                    args=(fetcher, s, inter, autoua))
            self.threads.append(t)
            t.daemon = True
            t.start()
    
    def join(self):
        self.add_endsignal()
        for t in self.threads:
            t.join()
        for s in self.ss:
            s.close()
    
    def is_running(self):
        return any(t.is_alive() for t in self.threads)

    def add_endsignal(self):
        self.job.put(None)
    
    def add_job(self, elem):
        self.job.put(elem)

    # entry point
    def sched_downloaders(self, fetcher, session, inter, autoua):
        last_at = None
        cfail = 0
        while True:
            elem = self.job.get(True)   #block
            if elem is None:
                self.add_endsignal()    #notify peer threads
                break
            else:
                if inter > 0 and last_at:
                    gap = last_at + inter - time.time()
                    if gap > 0:
                        time.sleep(gap)
                if autoua:
                    session.headers['User-Agent'] = self.ua.random

                last_at = time.time()
                ret = fetcher(session, elem)
                self.out.put(ret)
                
                if ret:
                    cfail = 0
                else:
                    cfail += 1
                    stime = min(4**cfail*inter, 300)
                    time.sleep(stime)
                    self.log.debug(f'{cfail}, sleep {stime} seconds')
        # waiting for downloader tasks to finish
        self.log.info('Queue Finished')

if "__main__" == __name__:
    import sys
    from cube import get_cube_detail    

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(threadName)10s %(levelname)s: %(message)s',
        datefmt='%H:%M:%S'
    )

    dld = TrdFetcher()
    for i in range(100001,100020):
        dld.add_job(f"ZH{i:06d}")
    dld.add_endsignal()

    logging.info('Start ...')

    proxies = [
        "",
        "socks5://localhost:7089",
        "socks5://localhost:7080",
        "socks5://localhost:7088",
    ]
    dld.start(get_cube_detail, proxies, 1.5, True)
    dld.join()

    logging.info('Exit ..')


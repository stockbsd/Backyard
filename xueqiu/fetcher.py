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

def net_sessions(proxies, cktype=1):
    cks = [
        '',
        'xq_a_token=c9d3b00a3bd89b210c0024ce7a2e049f437d4df3;', #anonymous user
        'aliyungf_tc=AQAAAMMMjiWbCw8AJON2ca5e8P94fS2f; s=ck16erzsji; bid=47cef4283992c2b6bd935bdcd877e6dd_jz3m4fcj; snbim_minify=true; u=8347941443; acw_tc=2760822d15756697011777055e6f32a282d01493264a0a75e9187e5bcadf3b; xq_a_token=fd5aeeb678322a4465d174affc962efa5c58784f; xqat=fd5aeeb678322a4465d174affc962efa5c58784f; xq_r_token=03b2f32b7f1dcebccebe3a9a160c558895ca345c; xq_token_expire=Thu%20Jan%2002%202020%2012%3A36%3A21%20GMT%2B0800%20(China%20Standard%20Time); xq_is_login=1;',
    ]
    if cktype not in [1, 2]:
        cktype = 0

    headers = {
        'Cookie': cks[cktype],
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
        if not session.headers['Cookie']:
            session.get('https://xueqiu.com') #get cookie , xq_a_token
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
                    stime = min(5**cfail*inter, 300)
                    time.sleep(stime)
                    self.log.debug(f'{cfail}, sleep {stime} seconds')
        # waiting for downloader tasks to finish
        self.log.info('Queue Finished')

if "__main__" == __name__:
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


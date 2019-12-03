# coding: utf-8
 
import time, logging
import json
from sqlalchemy import create_engine, MetaData, Table, func, select, exists
from sqlalchemy.dialects.postgresql import insert
import requests
from itertools import groupby

# import re
# from bs4 import BeautifulSoup

from fetcher import TrdFetcher, net_sessions

def get_user_data(page):
    user_data =[]
    for i in range(page):
        url = 'https://xueqiu.com/friendships/groups/members.json?uid=3094390085&page={}&gid=0'.format(i+1)
        response = requests.get(url,headers=headers).json()['users']
        user_data.extend(response)
        logging.info('正在打印%s页' % str(i+1))
        time.sleep(1)
    return user_data

def get_user_friends(session, userid):
    pass
    #用户关注 https://xueqiu.com/friendships/groups/members.json?uid=1866368892&page=1&gid=0&_=1573818170807
    # https://xueqiu.com/private_fund/manager_rate/query.json?user_id=1866368892
    #https://xueqiu.com/cubes/list.json?user_id=1866368892&sp_only=true
    #用户信息 https://xueqiu.com/statuses/original/show.json?user_id=1866368892
    #用户推荐 https://xueqiu.com/recommend/user/editor.json?type=0&count=3
    #https://xueqiu.com/friendships/follow_each_other.json?_=1573818570081

def get_user_cubes(session, userid):
    #关注 https://stock.xueqiu.com/v5/stock/portfolio/stock/list.json?size=1000&category=3&uid=1866368892&pid=-120
    #创建 https://stock.xueqiu.com/v5/stock/portfolio/stock/list.json?size=1000&category=3&uid=1866368892&pid=-24
    #数据 https://xueqiu.com/cubes/quote.json?code=ZH009529,ZH133882,ZH009494,ZH112718
    pass

# https://xueqiu.com/cubes/rebalancing/history.json?cube_symbol=ZH009529
# https://xueqiu.com/cubes/rebalancing/history.json?cube_symbol=ZH009529&count=20&page=1
# https://xueqiu.com/cubes/rank/summary.json?symbol=ZH009529&ua=web

def get_cubes_info(session, cubes):
    code = ','.join(cubes)
    url = f"https://xueqiu.com/cubes/quote.json?code={code}"
    resp = session.get(url)
    data = resp.json()
    if resp.status_code == 200:
        return list(data.values())
    elif (resp.status_code == 400 and data['error_code']=='20809'):
        logging.info(f"{code[:20]} {data['error_description']}")
        # logging.debug(session.cookies.get_dict())
        # logging.debug(resp.headers)
        return [] #组合不存在
    else:
        logging.info(f'{url} {resp.status_code} {resp.text} failed')
    raise Exception('组合信息获取失败')


def process_cube_info(row):
    for c in ["net_value", "daily_gain", "monthly_gain","total_gain","annualized_gain"]:
        row[c] = float(row[c])
    row['closed_at'] = int(row['closed_at']) if row['closed_at'] else 0
    row['symbolid'] = int(row['symbol'][2:])
    for c in ['hasexist', 'badges_exist']:
        row[c] = True if row[c].lower()=='true' else False    

# def get_cube_holdings(session, cube):
#     url = f"https://xueqiu.com/P/{cube}"
#     resp = session.get(url)
#     if resp.status_code == 200:
#         # soup = BeautifulSoup(resp.text, 'html.parser')
#         # script = soup.find('script', text=re.compile('SNB\.cubeInfo'))
#         # json_text = re.search(r'^\s*SNB\.cubeInfo\s*=\s*({.*?})\s*;\s*$',
#         #                 script.string, flags=re.DOTALL | re.MULTILINE).group(1)
#         pos_start = resp.text.find('SNB.cubeInfo = ') + len('SNB.cubeInfo = ')
#         pos_end = resp.text.find('SNB.cubePieData', pos_start)
#         pos_end = resp.text.rfind(';', pos_start, pos_end)
#         json_text = resp.text[pos_start:pos_end]
#         # print(json_text[:15], json_text[-20:])
        
#         data = json.loads(json_text)
#         print(f"{cube} {data['name']} {data['follower_count']}: {data['owner']['screen_name']} {data['owner']['followers_count']}")
#         for d in data["view_rebalancing"]["holdings"]:
#             print('\t', d['stock_symbol'], d['stock_name'], d['weight'])

#         return data
#     else:
#         logging.info(f'{url} {resp.status_code} failed')
#         return []


def get_cube_detail(session, cube):
    try:
        url = f"https://xueqiu.com/P/{cube}"
        resp = session.get(url)
        if resp.status_code == 200:
            # soup = BeautifulSoup(resp.text, 'html.parser')
            # script = soup.find('script', text=re.compile('SNB\.cubeInfo'))
            # json_text = re.search(r'^\s*SNB\.cubeInfo\s*=\s*({.*?})\s*;\s*$',
            #                 script.string, flags=re.DOTALL | re.MULTILINE).group(1)
            pos_start = resp.text.find('SNB.cubeInfo = ') + len('SNB.cubeInfo = ')
            pos_end = resp.text.find('SNB.cubePieData', pos_start)
            pos_end = resp.text.rfind(';', pos_start, pos_end)
            json_text = resp.text[pos_start:pos_end]
            logging.info(f'{cube} detail done, with {session.proxies.get("http")}')
            return json_text
        else:
            logging.warning(f'{cube} detail {resp.status_code}, with {session.proxies.get("http")}')
            return ""
    except Exception as e:
        print(e)

    return None


def process_cube_detail(detail):
    try:
        if detail:
            cube = json.loads(detail)
            user = cube.pop('owner')

            for k,v in cube.items():
                if isinstance(v, (dict, list, tuple)):
                    cube[k] = json.dumps(v)
            for k,v in user.items():
                if isinstance(v, (dict, list, tuple)):
                    user[k] = json.dumps(v)
            
            cube['closed_at'] = int(cube['closed_at']) if cube['closed_at'] else 0
            cube['symbolid'] = int(cube['symbol'][2:])
            cube['annualized_gain'] = cube.pop('annualized_gain_rate')

            return cube, user
    except Exception as e:
        print(e, detail[:10], detail[-10:])

    return None, None


def scrap_cubes_info(engine, step, loopinter, proxies, ctype='ZH', since=0):
    assert(ctype in ('ZH','SP'))
    # engine = create_engine("postgres://stockbsd:stockbsd@localhost:5432/postgres")
    metadata = MetaData(bind=engine)

    tpData = ('cubeinfo', 0) if ctype=='ZH' else ('spinfo', 999999)
    CITable = Table(tpData[0], metadata, autoload=True)

    ins = insert(CITable).on_conflict_do_nothing()
    # stmt = insert(CITable)
    # update_dict = { c.name:c for c in stmt.excluded if not c.primary_key }  #update dict 
    # ins = stmt.on_conflict_do_update(CITable.primary_key, set_=update_dict)
    logging.debug(ins)

    conn = engine.connect()
    # maxid = conn.execute("select max(symbolid) from cubeinfo").scalar() or 0
    if since>0:
        nextid = since
    else:
        nextid = (conn.execute(select([func.max(CITable.c.symbolid)])).scalar() or tpData[1]) + 1
    logging.info(f"{nextid}")

    ss = net_sessions(proxies)
    interval = loopinter/len(ss)     #一分钟40次

    loop = 0
    while True:
        cubes = [f'{ctype}{i:06d}' for i in range(nextid, nextid+step)]

        try:
            session = ss[loop % len(ss)]
            loop += 1
            info = get_cubes_info(session, cubes)
        except Exception as e:
            info = None
            interval = min(interval*2, 300)

        if info:
            for row in info:
                process_cube_info(row)
            
            conn.execute(ins, info) #autocommit
            logging.info(f'{nextid} {len(info)}/{step} done, with {session.proxies.get("http")}')

            nextid = max(row['symbolid'] for row in info) + 1
        elif info == []:
            nextid += step  #全空，也可以退出
            if not since: break
        
        time.sleep(interval)

    for s in ss:
        s.close()
    conn.close()


def scrap_missed_cubes_info(engine, step, loopinter, proxies):
    # engine = create_engine("postgres://stockbsd:stockbsd@localhost:5432/postgres")
    metadata = MetaData(bind=engine)

    CITable = Table('cubeinfo', metadata, autoload=True)
    ins = insert(CITable).on_conflict_do_nothing() #CITable.insert()
    logging.debug(ins)

    conn = engine.connect()
    result = conn.execute("SELECT s.i AS missing_num FROM generate_series(0,2053895) s(i) WHERE NOT EXISTS (SELECT 1 FROM cubeinfo WHERE symbolid = s.i)")
    missing = [r['missing_num'] for r in result]

    ss = net_sessions(proxies)
    interval = loopinter/len(ss)

    nextid = 0
    for loop in range(1000000):
        if nextid > len(missing):
            break

        cubes = [f'ZH{i:06d}' for i in missing[nextid: nextid+step]]

        try:
            session = ss[loop % len(ss)]
            info = get_cubes_info(session, cubes)
            nextid += step
        except Exception as e:
            info = None
            interval = min(interval*2, 300)

        if info:
            for row in info:
                process_cube_info(row)
            
            # df = pd.DataFrame(info)
            # df.to_sql('cubeinfo', conn, if_exists="append", index=False)
            conn.execute(ins, info) #autocommit
            logging.info(f'{nextid} {len(info)}/{step} done, with {session.proxies.get("http")}')
        
        time.sleep(interval)

    for s in ss:
        s.close()
    conn.close()


def scrap_cubes_detail(engine, loopinter, proxies, ctype='ZH'):
    # engine = create_engine("postgres://stockbsd:stockbsd@localhost:5432/postgres")
    metadata = MetaData(bind=engine)
    conn = engine.connect()

    TCubes = Table('cubes', metadata, autoload=True)
    TUsers = Table('users', metadata, autoload=True)

    ss = net_sessions(proxies)
    interval = loopinter/len(ss)

    #sql = "SELECT l.symbolid FROM cubeinfo l LEFT JOIN cubes r ON r.symbolid = l.symbolid WHERE r.symbolid IS NULL and l.symbolid<2040000"
    sql = "SELECT symbolid FROM cubeinfo \
            WHERE NOT EXISTS (SELECT FROM cubes WHERE symbolid=cubeinfo.symbolid) \
            AND closed_at=0 AND total_gain <= 50 and total_gain > 30 \
            Order by total_gain desc "
    result = conn.execute(sql)

    loop = 0
    cubelist, userlist = [],[]
    for row in result:
        symbol = f"{ctype}{row['symbolid']:06d}"
        session = ss[loop % len(ss)]
        # session.headers['User-Agent'] = ua.random
        loop += 1

        detail = get_cube_detail(session, symbol)

        cube, user = process_cube_detail(detail)
        if cube:
            conn.execute(insert(TCubes).on_conflict_do_nothing(), cube)        
            conn.execute(insert(TUsers).on_conflict_do_nothing(), user)
            # logging.info(f'{symbol} done, with {session.proxies.get("http")}')

    for s in ss:
        s.close()
    conn.close()


def scrap_cubes_detail2(engine, loopinter, proxies, maxtg, mintg, ctype='ZH'):
    metadata = MetaData(bind=engine)
    conn = engine.connect()

    CI = Table('cubeinfo', metadata, autoload=True)
    TCubes = Table('cubes', metadata, autoload=True)
    TUsers = Table('users', metadata, autoload=True)

    # sql = "SELECT l.symbolid FROM cubeinfo l LEFT JOIN cubes r ON r.symbolid = l.symbolid WHERE r.symbolid IS NULL and l.symbolid<2040000"
    # sql = "SELECT symbolid FROM cubeinfo \
    #         WHERE NOT EXISTS (SELECT FROM cubes WHERE symbolid=cubeinfo.symbolid) \
    #         AND closed_at=0 AND total_gain <= 10 and total_gain > 5 \
    #         Order by total_gain desc "
    sql = (select([CI.c.symbolid])
            .where(~exists().where(CI.c.symbolid==TCubes.c.symbolid))
            .where(CI.c.closed_at==0)
            .where(CI.c.total_gain <= maxtg).where(CI.c.total_gain > mintg)
            .order_by(CI.c.total_gain.desc())
            )
    result = conn.execute(sql)

    dld = TrdFetcher()
    dld.start(get_cube_detail, proxies, loopinter, True)

    for row in result:
        symbol = f"{ctype}{row['symbolid']:06d}"
        dld.add_job(symbol)
    dld.add_endsignal()

    bulk = 100
    ended = False
    cubelist, userlist = [],[]
    while not ended:
        try:
            detail = dld.out.get(block=True, timeout=3)
            cube, user = process_cube_detail(detail)
            if cube: cubelist.append(cube)
            if user: userlist.append(user)
        except Exception as e:
            ended = dld.out.empty() and not dld.is_running()

        if len(cubelist) >= bulk or (ended and cubelist):
            conn.execute(insert(TCubes).on_conflict_do_nothing(), cubelist)        
            conn.execute(insert(TUsers).on_conflict_do_nothing(), userlist)
            logging.info(f'finished {len(cubelist)} records')
            cubelist[:] = []
            userlist[:] = []

    # dld.join()
    # for s in ss:
    #     s.close()
    conn.close()


if __name__ == '__main__':
    import sys
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, 
        format='%(asctime)s %(threadName)10s %(levelname)s: %(message)s', datefmt='%H:%M:%S')

    engine = create_engine("postgres://stockbsd:stockbsd@localhost:5432/postgres")

    proxies = [
        "",
        "socks5://localhost:7089",
        "socks5://localhost:7080",
        "socks5://localhost:7088",
    ]
    scrap_cubes_detail2(engine, 2.0, proxies, 5, 0)
    # scrap_cubes_detail(engine, 1.0)

    # scrap_cubes_info(engine, 50, 2, proxies, ctype='ZH', since=0)
    # scrap_cubes_info(engine, 50, 2, proxies, ctype='SP', since=0)
    # scrap_missed_cube_info(50, 2)

    engine.dispose()

# coding: utf-8
 
import time, logging
import json
import requests
from sqlalchemy import create_engine, MetaData, Table, func, select, exists
from sqlalchemy.dialects.postgresql import insert

from fetcher import TrdFetcher, net_sessions


def get_user_friends(session, userid):
    pass
    #用户关注 https://xueqiu.com/friendships/groups/members.json?uid=1866368892&page=1&gid=0&_=1573818170807
    # https://xueqiu.com/private_fund/manager_rate/query.json?user_id=1866368892
    #https://xueqiu.com/cubes/list.json?user_id=1866368892&sp_only=true
    #用户信息 https://xueqiu.com/statuses/original/show.json?user_id=1866368892
    #用户推荐 https://xueqiu.com/recommend/user/editor.json?type=0&count=3
    #https://xueqiu.com/friendships/follow_each_other.json?_=1573818570081
    #用户文章 https://xueqiu.com/v4/statuses/user_timeline.json?page=1&user_id=5135726 

def get_user_cubes(session, userid):
    #关注 https://stock.xueqiu.com/v5/stock/portfolio/stock/list.json?size=1000&category=3&uid=1866368892&pid=-120
    #创建 https://stock.xueqiu.com/v5/stock/portfolio/stock/list.json?size=1000&category=3&uid=1866368892&pid=-24
    #数据 https://xueqiu.com/cubes/quote.json?code=ZH009529,ZH133882,ZH009494,ZH112718
    pass

# https://xueqiu.com/cubes/rebalancing/history.json?cube_symbol=ZH009529
# https://xueqiu.com/cubes/rebalancing/history.json?cube_symbol=ZH009529&count=20&page=1
# https://xueqiu.com/cubes/rank/summary.json?symbol=ZH009529&ua=web


def get_user_data(session, uid):
    try:
        url = f'https://xueqiu.com/statuses/original/show.json?user_id={uid}'
        resp = session.get(url)
        data = resp.json()
        if resp.status_code == 200:
            user = data['user']

            for k,v in user.items():
                if isinstance(v, (dict, list, tuple)):
                    user[k] = json.dumps(v)
      
            logging.info(f'user {uid} done, with {session.proxies.get("http")}')
            return user
    except Exception as e:
        print(e)

    logging.warning(f'user {uid} failed, with {session.proxies.get("http")}')
    return None


def get_cubes_info(session, cubes):
    try:
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
            logging.warning(f"{code[:20]} {data['error_description']}")
    except Exception as e:
        print(e)

    return None


def process_cube_info(row):
    for c in ["net_value", "daily_gain", "monthly_gain", "total_gain", "annualized_gain"]:
        row[c] = float(row[c])
    row['closed_at'] = int(row['closed_at']) if row['closed_at'] else 0
    row['symbolid'] = int(row['symbol'][2:])
    for c in ['hasexist', 'badges_exist']:
        row[c] = True if row[c].lower()=='true' else False    


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
            if 'annualized_gain_rate' in cube:  # no this key in SP
                cube['annualized_gain'] = cube.pop('annualized_gain_rate')

            return cube, user
    except Exception as e:
        print(f"detail_process 出错 {e}, {detail[:10]} ... {detail[-10:]}")

    return None, None


def construct_upsert(ATable):
    stmt = insert(ATable)
    update_dict = { c.name:c for c in stmt.excluded if not c.primary_key }  #update dict 
    ins = stmt.on_conflict_do_update(ATable.primary_key, set_=update_dict)
    return ins


def scrap_cubes_info(engine, step, loopinter, proxies, ctype='ZH', since=0):
    assert(ctype in ('ZH','SP'))
    # engine = create_engine("postgres://stockbsd:stockbsd@localhost:5432/postgres")
    metadata = MetaData(bind=engine)

    tpData = ('zhinfo', 0) if ctype=='ZH' else ('spinfo', 999999)
    CITable = Table(tpData[0], metadata, autoload=True)

    ins = insert(CITable).on_conflict_do_nothing()
    logging.debug(ins)

    conn = engine.connect()
    # maxid = conn.execute("select max(symbolid) from zhinfo").scalar() or 0
    if since>0:
        nextid = since
    else:
        nextid = (conn.execute(select([func.max(CITable.c.symbolid)])).scalar() or tpData[1]) + 1
    logging.info(f"From {nextid}")

    ss = net_sessions(proxies)
    interval = loopinter/len(ss)     #一分钟40次

    loop = 0
    while True:
        cubes = [f'{ctype}{i:06d}' for i in range(nextid, nextid+step)]

        session = ss[loop % len(ss)]
        loop += 1
        info = get_cubes_info(session, cubes)

        if info:
            for row in info:
                process_cube_info(row)
            
            conn.execute(ins, info) #autocommit
            logging.info(f'{nextid} {len(info)}/{step} done, with {session.proxies.get("http")}')

            nextid = max(row['symbolid'] for row in info) + 1
        elif info == []:
            nextid += step  #全空，也可以退出
            if not since: break
        else: # None失败 
            interval = min(interval*2, 300)

        time.sleep(interval)

    for s in ss:
        s.close()
    conn.close()


def scrap_misOup_cubes_info(engine, step, loopinter, proxies, maxid, minid, bUpdate, ctype='ZH'):
    assert(ctype in ('ZH','SP'))
    metadata = MetaData(bind=engine)
    tablename = 'zhinfo' if ctype=='ZH' else 'spinfo'
    CITable = Table(tablename, metadata, autoload=True)
    conn = engine.connect()

    if bUpdate:
        ins = construct_upsert(CITable)

        sql = (select([CITable.c.symbolid])
                .where(CITable.c.symbolid > minid).where(CITable.c.symbolid <= maxid)
                .where(CITable.c.closed_at==0).order_by(CITable.c.symbolid))
        result = conn.execute(sql)
        todos = [r['symbolid'] for r in result]
    else:
        ins = insert(CITable).on_conflict_do_nothing() #CITable.insert()

        sql = f"SELECT s.i AS missing_num FROM generate_series({minid},{maxid}) s(i) WHERE NOT EXISTS (SELECT 1 FROM {tablename} WHERE symbolid = s.i)"
        result = conn.execute(sql)
        todos = [r['missing_num'] for r in result]
    # logging.debug(ins)

    ss = net_sessions(proxies)
    interval = loopinter/len(ss)

    nextid, loop = 0, 0
    while nextid < len(todos):
        cubes = [f'{ctype}{i:06d}' for i in todos[nextid: nextid+step]]

        session = ss[loop % len(ss)]
        loop += 1
        info = get_cubes_info(session, cubes)

        if info:
            nextid += step
            for row in info:
                process_cube_info(row)
            
            conn.execute(ins, info) #autocommit
            logging.info(f'{cubes[-1]} {len(info)}/{step} done, with {session.proxies.get("http")}')
        elif info == []:
            nextid += step  # 推进

        time.sleep(interval)

    logging.info(f'{todos[-1]} processed')
    for s in ss:
        s.close()
    conn.close()


def scrap_cubes_detail(engine, loopinter, proxies, maxnv, minnv, bUpdate, ctype):
    metadata = MetaData(bind=engine)
    conn = engine.connect()

    CI = Table(f'{ctype.lower()}info', metadata, autoload=True)
    CD = Table(f'{ctype.lower()}detail', metadata, autoload=True)
    TUsers = Table('users', metadata, autoload=True)

    upsert = construct_upsert(CD)
    user_ins = construct_upsert(TUsers) #insert(TUsers).on_conflict_do_nothing()

    ss = net_sessions(proxies, 2 if ctype=='SP' else 1)
    interval = loopinter/len(ss)

    sql = (select([CI.c.symbolid])
            .where(CI.c.closed_at==0)
            .where(CI.c.net_value <= maxnv).where(CI.c.net_value > minnv)
            .order_by(CI.c.net_value.desc())
            )
    if not bUpdate:
        sql = sql.where(~exists().where(CI.c.symbolid==CD.c.symbolid))
                
    result = conn.execute(sql)

    loop = 0
    cubelist, userlist = [],[]
    for row in result:
        symbol = f"{ctype}{row['symbolid']:06d}"
        session = ss[loop % len(ss)]
        loop += 1

        detail = get_cube_detail(session, symbol)

        cube, user = process_cube_detail(detail)
        if cube:
            conn.execute(upsert, cube)

            # if ctype == 'SP':
            #     user = get_user_data(session, user['id'])
            # if user:
            #     conn.execute(user_ins, user)
            # logging.info(f'{symbol} done, with {session.proxies.get("http")}')

        time.sleep(interval)

    for s in ss:
        s.close()
    conn.close()


def scrap_cubes_detail2(engine, loopinter, proxies, maxnv, minnv, bUpdate, ctype='ZH'):
    assert(ctype in ('ZH','SP'))
    metadata = MetaData(bind=engine)
    conn = engine.connect()

    CI = Table(f'{ctype.lower()}info', metadata, autoload=True)
    CD = Table(f'{ctype.lower()}detail', metadata, autoload=True)
    TUsers = Table('users', metadata, autoload=True)

    ins = construct_upsert(CD)
    sql = (select([CI.c.symbolid])
            .where(CI.c.closed_at==0)
            .where(CI.c.net_value <= maxnv).where(CI.c.net_value > minnv)
            .order_by(CI.c.net_value.desc())
            )
    if not bUpdate:
        sql = sql.where(~exists().where(CI.c.symbolid==CD.c.symbolid))
        ins = insert(CD).on_conflict_do_nothing()

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
            if ctype=='ZH':
                conn.execute(insert(TUsers).on_conflict_do_nothing(), userlist)
            conn.execute(ins, cubelist)        
            logging.info(f'finished {len(cubelist)} records')
            cubelist[:] = []
            userlist[:] = []

    dld.join()
    conn.close()


def scrap_cube_users(engine, loopinter, proxies):
    metadata = MetaData(bind=engine)
    conn = engine.connect()

    # CI = Table(f'{ctype.lower()}info', metadata, autoload=True)
    CD = Table('spdetail', metadata, autoload=True)
    TUsers = Table('users', metadata, autoload=True)

    ins = insert(TUsers).on_conflict_do_nothing()
    sql = (select([CD.c.owner_id])
            .where(~exists().where(CD.c.owner_id==TUsers.c.id))
            )
    # print(sql)
    result = conn.execute(sql)

    dld = TrdFetcher()
    dld.start(get_user_data, proxies, loopinter, True)

    for row in result:
        dld.add_job(row['owner_id'])
    dld.add_endsignal()

    ended = False
    userlist = []
    while not ended:
        try:
            user = dld.out.get(block=True, timeout=3)
            if user: userlist.append(user)
        except Exception as e:
            ended = dld.out.empty() and not dld.is_running()

        if len(userlist) >= 60 or (ended and userlist):
            conn.execute(ins, userlist)
            logging.info(f'finished {len(userlist)} records')
            userlist[:] = []

    dld.join()
    conn.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, 
        format='%(asctime)s %(threadName)10s %(levelname)s: %(message)s', datefmt='%H:%M:%S')

    engine = create_engine("postgres://stockbsd:stockbsd@localhost:5432/postgres")

    proxies = [
        "",
        "socks5://localhost:7089",
        "socks5://localhost:7080",
        "socks5://localhost:7088",
    ]

    scrap_cubes_info(engine, 50, 2, proxies, ctype='ZH', since=0)
    # scrap_cubes_info(engine, 50, 2, proxies, ctype='SP', since=0)

    # for i in range(7):
    #     scrap_misOup_cubes_info(engine, 50, 2, proxies, 300000*(i+1), 300000*i, True)
    # scrap_misOup_cubes_info(engine, 50, 2, proxies, 1015000, 1000000, True, ctype='SP')

    # scrap_cubes_detail2(engine, 2.5, proxies, 3, 2.0, True, 'ZH')
    # scrap_cubes_detail2(engine, 2.0, proxies, 2, 1.5, True, 'SP')
    
    scrap_cubes_detail(engine, 5, proxies[:2], 40, 1.5, True, 'SP')
    # scrap_cubes_detail(engine, 2, proxies, 1.05, 1.0, False, 'ZH')

    scrap_cube_users(engine, 2, proxies)

    engine.dispose()

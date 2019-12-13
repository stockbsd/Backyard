import logging
import json
from datetime import datetime, timedelta
from itertools import groupby
from sqlalchemy import create_engine, MetaData, Table, func, select
import pandas as pd


def analy_cubes(engine, maxnv, minnv, c_fl, p_fl, ctype, bPer=True, bSum=True, sumLine=20):
    metadata = MetaData(bind=engine)
    dt = f'{ctype.lower()}detail'
    CI = Table(dt, metadata, autoload=True)
    Users = Table("users", metadata, autoload=True)

    qs = (select([CI.c.name, CI.c.symbolid, CI.c.net_value, CI.c.follower_count, 
                    CI.c.view_rebalancing, CI.c.sell_rebalancing, 
                    Users.c.screen_name, Users.c.followers_count])
                    .select_from(CI.join(Users, Users.c.id == CI.c.owner_id))
                    # .where(CI.c.market=='cn')
                    .where(CI.c.closed_at==0)
                    .where(CI.c.net_value <= maxnv).where(CI.c.net_value > minnv)
                    .where(CI.c.follower_count > c_fl)
                    .where(Users.c.followers_count > p_fl)
                    .order_by(CI.c.net_value.desc())
                    # .limit(3000)
            )
    
    # v_qs = (select([CI.c.name, CI.c.symbolid, CI.c.net_value, CI.c.view_rebalancing, 
    #                 CI.c.follower_count, Users.c.screen_name, Users.c.followers_count])
    #                 .select_from(Users.join(CI, Users.c.id == CI.c.owner_id))
    #                 .where(CI.c.market=='cn')
    #                 .where(CI.c.closed_at==0)
    #                 # .where(CI.c.net_value <= maxnv).where(CI.c.net_value > minnv)
    #                 # .where(CI.c.follower_count > c_fl)
    #                 .where(Users.c.followers_count > p_fl)
    #                 .order_by(CI.c.net_value.desc())
    #                 # .limit(3000)
    #         )
    # print(qs)
    
    conn = engine.connect()
    rows = conn.execute(qs)

    allstocks = []
    count = 0
    for r in rows:
        count += 1
        stocks = json.loads(r['view_rebalancing'])['holdings']
        allstocks.extend(stocks)

        if bPer:
            sr = json.loads(r['sell_rebalancing'])
            if sr['updated_at']:
                yestd = datetime.now() - timedelta(2)
                yesut = datetime(yestd.year, yestd.month, yestd.day, 0,0,0,0).timestamp()
                if sr['updated_at']/1000 > yesut:
                    print(r.name, r.net_value, r.follower_count, r.screen_name, r.followers_count, sum(d['weight'] for d in stocks))
                    for d in stocks:
                        print('\t', d['stock_symbol'], d['stock_name'], d['weight'])

                    print('  更新于', datetime.fromtimestamp(sr['updated_at']/1000))
                    his = sr['rebalancing_histories']
                    for h in his:
                        print('\t', h['stock_name'], h["prev_weight_adjusted"], "->", h["target_weight"])
                    print()

    if bSum:
        ava_w = sum(d['weight'] for d in allstocks)/count
        print(f'{count}个组合, 平均仓位{ava_w}')
        lk = lambda x:(x['stock_symbol'], x['stock_name'])
        for k, grp in groupby(sorted(allstocks, key=lk), key=lk):
            cg, wg = 0, 0.0
            for s in grp:
                cg += 1
                wg += s['weight']
            if cg * sumLine >= count:
                print(f'{k[0]} {k[1]}，{cg} 个组合持有，共{wg:.2f}权重，均{wg/cg:.2f}')

    conn.close()


def analy_sp(engine):
    df = pd.read_sql_table('spinfo', engine)
    df = df[(df['net_value']<100) & (df['net_value']> 0.2) & (df['annualized_gain']<100)]
    print(df.describe())

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, 
        format='%(asctime)s %(levelname)s: %(message)s', datefmt='%H:%M:%S')

    engine = create_engine("postgres://stockbsd:stockbsd@localhost:5432/postgres")
    
    analy_cubes(engine, 12, 2, 300, 10000, 'ZH', True, False, 30)
    # analy_sp(engine)

    engine.dispose()
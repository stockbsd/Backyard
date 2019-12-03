import logging
from itertools import groupby
import json
from sqlalchemy import create_engine, MetaData, Table, func, select

def analy_cubes(engine, maxnv, minnv, flcount):
    metadata = MetaData(bind=engine)
    CI = Table("cubes", metadata, autoload=True)
    Users = Table("users", metadata, autoload=True)

    qs = (select([CI.c.name, CI.c.symbolid, CI.c.net_value, CI.c.view_rebalancing, 
                    CI.c.follower_count, Users.c.screen_name, Users.c.followers_count])
                    .select_from(CI.join(Users, Users.c.id == CI.c.owner_id))
                    .where(CI.c.market=='cn')
                    .where(CI.c.closed_at==0)
                    .where(CI.c.net_value < maxnv).where(CI.c.net_value > minnv)
                    .where(CI.c.follower_count > flcount)
                    .order_by(CI.c.net_value.desc()).limit(5000)
            )
    # print(qs)
    
    conn = engine.connect()
    rows = conn.execute(qs)

    allstocks = []
    count = 0
    for r in rows:
        count += 1
        stocks = json.loads(r['view_rebalancing'])['holdings']
        allstocks.extend(stocks)

        print(r.name, r.net_value, r.screen_name)
        for d in stocks:
            print('\t', d['stock_symbol'], d['stock_name'], d['weight'])
    
    ava_w = sum(d['weight'] for d in allstocks)/count
    print(f'{count}个组合, 平均仓位{ava_w}')
    lk = lambda x:(x['stock_symbol'], x['stock_name'])
    for k, grp in groupby(sorted(allstocks, key=lk), key=lk):
        cg, wg = 0, 0.0
        for s in grp:
            cg += 1
            wg += s['weight']
        if cg * 50 >= count:
            print(f'{k[0]} {k[1]}，{cg} 个组合持有，共{wg:.2f}权重，均{wg/cg:.2f}')

    conn.close()


if __name__ == '__main__':
    import sys
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, 
        format='%(asctime)s %(levelname)s: %(message)s', datefmt='%H:%M:%S')

    engine = create_engine("postgres://stockbsd:stockbsd@localhost:5432/postgres")
    analy_cubes(engine, 40, 4, 2000)
    engine.dispose()
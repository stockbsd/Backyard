# python

import pandas as pd
import sqlite3
import pathlib
import click


capinfo = {
        '301719075442':[5632000.0, 5376000.],
        '666600716948':[2367200.0, 1791452.],
        }

def getType(code):
    cl = [('159','ETF'),('002','股票'), ('300', '股票'), ('511', '货币'),
            ('11','转债'), ('12', '转债'), ('51', 'ETF'),
            ('6', '股票'), ('0', '股票')]
    for prefix, tp in cl:
        if code.startswith(prefix):
            return tp
    else:
        return '其他'

def loadRec(fn, conn):
    print(fn.name)
    try:
        with open(fn, encoding='gb2312') as f:
            l = next(f)
            info = l.split(':', 1)[1]
            di = dict((k,float(v)) for k,v in (s.split(':') for s in info.split()))

            _,_1 = next(f), next(f)

            df = pd.read_csv(f, delim_whitespace=True, dtype={0:str, 13:str, 14:str, 16:str})
            df['日期'] = fn.name[:8]
            df.to_sql('his', conn, if_exists="append", index=False)

            dfc = pd.DataFrame({k:[v] for k,v in di.items()})
            dfc['日期'] = fn.name[:8]
            dfc['资金账号'] = df['资金帐号'].iloc[0]
            dfc.to_sql('cap', conn, if_exists="append", index=False)
    except Exception as e:
        pass #print(e)


def updateDB(path, conn):
    for cf in sorted(path.rglob('*.txt')):
        loadRec(cf, conn)

def analInvest(conn):
    dfc_all = pd.read_sql_query('select * from cap', conn)
    dfc_latest = dfc_all.loc[dfc_all['日期'] == dfc_all['日期'].max()]
    #print(dfc_latest)
    for ind, row in dfc_latest.iterrows():
        keystr = row['资金账号']
        total = row['资产']
        orig = capinfo[keystr][0]
        orig01 = capinfo[keystr][-1]
        print('{:s} {:.2f} {:.2f} {:.2f} {:.2%} {:.2f} {:.2%}'.format(keystr,
            orig, total, total-orig, total/orig-1, total-orig01, total/orig01-1))

    df_all  = pd.read_sql_query('select * from his', conn)
    df_all['分类'] = df_all['证券代码'].map(getType)

    df = df_all.loc[df_all['日期'] == df_all['日期'].max()]
    sumdf = df.groupby('分类')['最新市值', '浮动盈亏'].sum()
    sumdf = sumdf[sumdf['最新市值']>0]
    sumdf['浮盈比'] = sumdf.apply(lambda r: r['浮动盈亏']/r['最新市值'] , axis=1)
    sumdf['仓比'] = sumdf.apply(lambda r: r['最新市值']/total, axis=1)
    print(sumdf.to_string(formatters={'浮盈比':'{:+.2%}'.format, '仓比':'{:.2%}'.format}))

@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx):
    ctx.ensure_object(dict)
    if ctx.invoked_subcommand is None:
        analInvest(ctx.obj['con'])

@cli.command()
@click.argument("src")
@click.pass_context
def update(ctx, src):
    cd = pathlib.Path(src).absolute()
    updateDB(cd, ctx.obj['con'])

if __name__ == '__main__':
    execdir = pathlib.Path(__file__).absolute().parent
    conn = sqlite3.connect(execdir/'his.sqlite3')

    cli(obj={'con':conn})

    conn.close()

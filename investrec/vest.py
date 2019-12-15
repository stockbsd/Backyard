# python

import pandas as pd
# import sqlite3
import pathlib
import click
from sqlalchemy import create_engine

capinfo = {
        '301719075442':[5632000.0, 5376000.],
        '666600716948':[2367200.0, 1791452.],
        }

def getType(code):
    cl = [('159','ETF'),('002','股票'), ('300', '股票'), ('511', '货币'),
            ('11','转债'), ('12', '转债'), ('51', 'ETF'), ('15', '基金'),('16', '基金'),
            ('6', '股票'), ('0', '股票')]
    for prefix, tp in cl:
        if code.startswith(prefix):
            return tp
    else:
        return '其他'

def loadRec(fn, conn, bTest):
    try:
        with open(fn, encoding='gb2312') as f:
            l = next(f)
            info = l.split(':', 1)[1]
            di = dict((k,float(v)) for k,v in (s.split(':') for s in info.split()))

            _,_1 = next(f), next(f)

            df = pd.read_csv(f, delim_whitespace=True, dtype={0:str, 13:str, 14:str, 16:str})
            if len(df.columns)==17:
                df['冻结数量'] = df['交易冻结数量'] + df['异常冻结']
                df.drop(columns=['备注','交易冻结数量','异常冻结','客户代码'], inplace=True)
                df.rename({"股份余额": "证券数量", "可用股份": "可卖数量", "在途股份": "在途数量"}, 
                    axis=1, inplace=True)
            df.rename({"盈亏比例(%)":"盈亏比例"}, axis=1, inplace=True)

            df['日期'] = fn.name[:8] 
            if bTest:
                print(df.head(3))
            else:
                df.to_sql('his', conn, if_exists="append", index=False)

            dfc = pd.DataFrame({k:[v] for k,v in di.items() if k != '在途'})
            dfc['日期'] = fn.name[:8]
            dfc['资金账号'] = df['资金帐号'].iloc[0]
            if bTest:
                print(dfc.head(2))
            else:
                dfc.to_sql('cap', conn, if_exists="append", index=False)
    except Exception as e:
        print(f'{fn.name} 出错：{e}')
    else:
        print(f'{fn.name} 处理完毕 ')


def updateDB(path, conn, bRecursive, bTest):
    func = path.rglob if bRecursive else path.glob
    for cf in sorted(func('*.txt')):
        loadRec(cf, conn, bTest)

def analInvest(conn):
    dfc_all = pd.read_sql_query('select * from cap', conn)
    tday = dfc_all['日期'].max()
    print(tday)

    dfc_latest = dfc_all.loc[dfc_all['日期'] == tday, ['资金账号','资产']].set_index('资金账号')
    dfc_latest['原始'] = dfc_latest.index.map(lambda zh:capinfo[zh][0])
    dfc_latest['年初'] = dfc_latest.index.map(lambda zh:capinfo[zh][-1])
    dfc_latest.loc['Total',:] = dfc_latest.sum(axis=0)  #sum as a new row
    dfc_latest['总收益'] = dfc_latest['资产']-dfc_latest['原始']
    dfc_latest['总收益率'] = dfc_latest['资产']/dfc_latest['原始'] -1
    dfc_latest['年收益'] = dfc_latest['资产']-dfc_latest['年初'] 
    dfc_latest['年收益率'] = dfc_latest['资产']/dfc_latest['年初'] -1
    print(dfc_latest.to_string(formatters={'总收益率':'{:+.2%}'.format, '年收益率':'{:.2%}'.format}))

    total = dfc_latest.loc['Total','资产']
    df_all  = pd.read_sql_query('select * from his', conn)
    df_all['分类'] = df_all['证券代码'].map(getType)
    df = df_all.loc[df_all['日期'] == tday]
    sumdf = df.groupby('分类')['最新市值', '浮动盈亏'].sum()
    sumdf = sumdf[sumdf['最新市值']>0]
    sumdf.loc['汇总',:] = sumdf.sum(axis=0)
    sumdf['浮盈比'] = sumdf['浮动盈亏']/sumdf['最新市值'] #sumdf.apply(lambda r: r['浮动盈亏']/r['最新市值'] , axis=1)
    sumdf['仓比'] = sumdf['最新市值']/total #sumdf.apply(lambda r: r['最新市值']/total, axis=1)
    print(sumdf.to_string(formatters={'浮盈比':'{:+.2%}'.format, '仓比':'{:.2%}'.format}))

@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx):
    ctx.ensure_object(dict)
    if ctx.invoked_subcommand is None:
        analInvest(ctx.obj['con'])

@cli.command()
@click.argument("src")
@click.option('-r', "--recursive", is_flag=True)
@click.option('-t', "--test", is_flag=True)
@click.pass_context
def update(ctx, src, recursive, test):
    cd = pathlib.Path(src).absolute()
    updateDB(cd, ctx.obj['con'], recursive, test)

def main():
    pg_eng =  create_engine("postgres://stockbsd:stockbsd@192.168.1.91:5432/postgres")

    try:
        # execdir = pathlib.Path(__file__).absolute().parent
        # conn = sqlite3.connect(execdir/'his.sqlite3')
        conn = pg_eng.connect()
        conn.execute('set search_path to invest')

        cli(obj={'con':conn})
    except Exception as e:
        print(e)
    finally:
        conn.close()

    pg_eng.dispose()

if __name__ == '__main__':
    main()

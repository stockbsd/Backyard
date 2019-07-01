# python

import pandas as pd

orig = 5632000.0
orig190101 = 5376000.0

def getType(code):
    cl = [('159','ETF'),('002','股票'), ('300', '股票'), ('511', '货币'),
            ('11','转债'), ('12', '转债'), ('51', 'ETF'),
            ('6', '股票'), ('0', '股票')]
    for prefix, tp in cl:
        if code.startswith(prefix):
            return tp
    else:
        return '其他'

def loadRec(fn):
    print(fn)
    with open(fn, encoding='gb2312') as f:
        l = next(f)
        info = l.split(':', 1)[1]
        di = dict((k,float(v)) for k,v in (s.split(':') for s in info.split()))
        total = di['资产']
        print('{:.2f} {:.2f} {:.2f} {:.2%}'.format(total, orig, total-orig, total/orig-1))
        _,_1 = next(f), next(f)

        df = pd.read_csv(f, delim_whitespace=True, dtype={0:str, 13:str, 14:str, 16:str})
        df['分类'] = df['证券代码'].map(getType)
        sumdf = df.groupby('分类')['最新市值', '浮动盈亏'].sum()
        sumdf = sumdf[sumdf['最新市值']>0]
        sumdf['浮盈比'] = sumdf.apply(lambda r: r['浮动盈亏']/r['最新市值'] , axis=1)
        sumdf['仓比'] = sumdf.apply(lambda r: r['最新市值']/total, axis=1)
        print(sumdf)
        #for r in df.itertuples(index=False, name='Sec'):
        #    print(r[0], r[1], getType(r[0]))
        #print(df.head(1))

if __name__ == '__main__':
    import pathlib
    cd = pathlib.Path(".")
    for cf in sorted(cd.glob('*.txt')):
        loadRec(cf)
        print()

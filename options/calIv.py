import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

df = pd.read_csv('510050.txt', skiprows=2, skipfooter=1, delim_whitespace=True, encoding='gb2312', 
        index_col=0, parse_dates=[0])

df['pct_chg'] = df['收盘'].pct_change()
#df['log_ret'] = np.log(df['收盘']) - np.log(df['收盘'].shift(1))
df['log_ret'] = np.log(df.pct_chg + 1)
df['hv21'] = df['log_ret'].rolling(21).std() * (252**0.5)

print(df)
print(df.describe())
print(df.log_ret.idxmax(), df.log_ret.idxmin())

df['收盘'].plot()
df.hv21.plot(secondary_y=True)
plt.show()

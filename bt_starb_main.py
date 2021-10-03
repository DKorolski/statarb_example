import datetime
import backtrader as bt
from statarb_strategy import *
from datetime import datetime
import pandas as pd
from pandas_datareader import data as pdr
import matplotlib.pyplot as plt
import quantstats
from data_loader import *
from data_processor import *
import backtrader.indicators as btind
import yfinance as yf
from finam import Exporter, Market, LookupComparator,Timeframe
pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 10)
import talib
from talib import CMO
import argparse
import datetime
import numpy as np
from statsmodels.regression.linear_model import OLS
import statsmodels.tsa.stattools as ts
import statsmodels.api as sm
import argparse
import asyncio
import aiohttp
import aiomoex
import os
import pandas as pd
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


filepath ="C:\МА\Pytrade\src"
def moex_candles(security,interval,start,end):
    async def main():
        async with aiohttp.ClientSession() as session:
            data = await aiomoex.get_board_candles(session=session, engine='futures', market='forts', board='SPBFUT', security=security, interval=interval, start=start, end=end)
            df = pd.DataFrame(data)
            print(df)
            df.set_index('begin', inplace=True)
            df.columns=['Open','Close','High','Low','Value','Volume','end']
            df=df[['Open','Close','High','Low','Volume']]
            df['Adj Close']=df['Close']
            df.index.names = ['Date']
            print(df.head(), '\n')
            print(df.tail(), '\n')
            df.info()
            os.chdir(filepath)
            df.to_csv(security+'_'+start+'.csv')
    asyncio.run(main())

def moex_candles_stock(security,interval,start,end):
    async def main():
        async with aiohttp.ClientSession() as session:
            data = await aiomoex.get_board_candles(session=session,  security=security, interval=interval, start=start, end=end)
            df = pd.DataFrame(data)
            print(df)
            df.set_index('begin', inplace=True)
            df.columns=['Open','Close','High','Low','Value','Volume','end']
            df=df[['Open','Close','High','Low','Volume']]
            df['Adj Close']=df['Close']
            df.index.names = ['Date']
            print(df.head(), '\n')
            print(df.tail(), '\n')
            df.info()
            os.chdir(filepath)
            df.to_csv(security+'_'+start+'.csv')
    asyncio.run(main())

def hurst1(ts):
    """
    Returns the Hurst Exponent of the time series vector ts
    """
    # make sure we are working with an array, convert if necessary
    ts = np.asarray(ts)

    # Helper variables used during calculations
    lagvec = []
    tau = []
    # Create the range of lag values
    lags = range(2, 300)

    #  Step through the different lags
    for lag in lags:
        #  produce value difference with lag
        pdiff = np.subtract(ts[lag:],ts[:-lag])
        #  Write the different lags into a vector
        lagvec.append(lag)
        #  Calculate the variance of the difference vector
        tau.append(np.sqrt(np.std(pdiff)))

    #  linear fit to double-log graph
    m = np.polyfit(np.log10(np.asarray(lagvec)),
                   np.log10(np.asarray(tau).clip(min=0.0000000001)),
                   1)
    # return the calculated hurst exponent
    return m[0]*2.0


# Instantiate Cerebro engine
#cerebro = bt.Cerebro(optreturn=False)
cerebro = bt.Cerebro()

#data = pdr.get_data_yahoo('AAPL', start=datetime(2016, 2, 7), end=datetime(2021, 7, 28))
start_date = '2021-09-20'
fromdate=datetime.date(2021, 9, 20)
end_date = '2021-09-30'
interval=1
tickers =['SNGSP','SNGS']

security_y=tickers[0]
start=start_date
y=moex_candles_stock(security=security_y,interval=interval,start=start_date,end=end_date)
df_y=pd.read_csv(security_y+'_'+start+'.csv')
df_y=pd.DataFrame(df_y).set_index('Date')
security_x=tickers[1]
x=moex_candles_stock(security=security_x,interval=interval,start=start_date,end=end_date)
df_x=pd.read_csv(security_x+'_'+start+'.csv')
df_x=pd.DataFrame(df_x).set_index('Date')
df_x1 = pd.DataFrame(data=df_x, index=df_y.index).ffill()
filepath ="C:/Users/dk/data-pipeline/src/global-cargo-tracker/webapp"
os.chdir(filepath)
df_y.to_csv(f'moex_5m/{tickers[0]}.csv')
df_x1.to_csv(f'moex_5m/{tickers[1]}.csv')

   # Get the dates from the args
fromdate = datetime.datetime.strptime(start_date, '%Y-%m-%d')
todate = datetime.datetime.strptime(end_date, '%Y-%m-%d')

data = pd.read_csv(f"moex_5m/{tickers[0]}.csv", parse_dates=True, index_col=[0])
data1 = pd.read_csv(f"moex_5m/{tickers[1]}.csv", parse_dates=True, index_col=[0])
data=data[start_date:end_date]
data1=data1[start_date:end_date]
data.dropna()
data1.dropna()
data=data[data.index.isin(data1.index)]
data1=data1[data1.index.isin(data.index)]
print(data)

df1 = pd.DataFrame({'y':(data['Close']),'x':(data1['Close'])})
print(df1)
est = sm.OLS(df1.y,df1.x)
est = est.fit()
df1['hr'] = -est.params[0]
df1['spread'] = df1.y + (df1.x * df1.hr)
cadfx = ts.adfuller(df1.spread)
print ('Augmented Dickey Fuller test statistic =',cadfx[0])
print ('Augmented Dickey Fuller p-value =',cadfx[1])
print ("Hurst Exponent =",round(hurst1(df1.spread),2))
#print(hurst(df1.spread))
#Run OLS regression on spread series and lagged version of itself
spread_lag = df1.spread.shift(1)
spread_lag.iloc[0] = spread_lag.iloc[1]
spread_ret = df1.spread - spread_lag
spread_ret.iloc[0] = spread_ret.iloc[1]
spread_lag2 = sm.add_constant(spread_lag)
model = sm.OLS(spread_ret,spread_lag2)
res = model.fit()
halflife = round(-np.log(2) / res.params[1],0)
if halflife < 20:
    halflife=20
print  ('Halflife = ',halflife)
meanSpread = df1.spread.rolling(window=abs(int(halflife))).mean()
stdSpread = df1.spread.rolling(window=abs(int(halflife))).std()
df1['zScore'] = (df1.spread-meanSpread)/stdSpread
print(df1)
data =  data[data.index.isin(df1.index)]
print(data)

mz=(df1['zScore'].max())

data['zscore']=df1['zScore']

print(data)
data.dropna()
data.index.names = ['datetime']
data=data[['Open','High','Low','Close','Volume','zscore']]
#data=data[['open','high','low','close','zscore']]

print(data)
data.columns=['open','high','low','close','volume','zscore']
#data.columns=['open','high','low','close','zscore']
print(data)
#data=data['2021-09-29':'2021-09-29']
#data1=data1['2021-09-29':'2021-09-29']
class PandasData_zscore(bt.feeds.PandasData):
    lines = ('zscore',)
    params = (('zscore', 5),
)
data0 = PandasData_zscore(dataname=data,
                                fromdate=datetime.datetime(2021, 9, 29),
                                todate=datetime.datetime(2021, 9, 30)
)

#data0 = bt.feeds.PandasData(dataname=data)
data1 = bt.feeds.PandasData(dataname=data1,
                                fromdate=datetime.datetime(2021, 9, 29),
                                todate=datetime.datetime(2021, 9, 30)
)





# Add the 2nd data to cerebro
cerebro.adddata(data0)
cerebro.adddata(data1)

# Add strategy to Cerebro
cerebro.addstrategy(PairTradingStrategy)
cerebro.broker.setcash(300000)
#cerebro.broker.setcommission(commission=0.002)




# Default position size
cerebro.addsizer(bt.sizers.SizerFix, stake=3)
cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe_ratio')
cerebro.addanalyzer(bt.analyzers.PyFolio, _name='PyFolio')
cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DrawDown')
cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')


#cerebro.optstrategy(Test, trail=range(1, 10))  

if __name__ == '__main__':
    
    results=0
    startcash = 300000
    start_portfolio_value = cerebro.broker.getvalue()
    cerebro.broker.setcash(startcash)
    results = cerebro.run()
    strat = results[0]
    if results !=0:
        
        end_portfolio_value = cerebro.broker.getvalue()
        pnl = end_portfolio_value - start_portfolio_value
        portfolio_stats = strat.analyzers.getbyname('PyFolio')
        returns, positions, transactions, gross_lev = portfolio_stats.get_pf_items()
        returns.index = returns.index.tz_convert(None)
        print(f'Starting Portfolio Value: {start_portfolio_value:2f}')
        print(f'Final Portfolio Value: {end_portfolio_value:2f}')
        print(f'PnL: {pnl:.2f}')
        print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
        print('DrawDown:', strat.analyzers.DrawDown.get_analysis())
        print('Final Balance: %.2f' % cerebro.broker.getvalue())
       
        quantstats.reports.html(returns, output='stats.html', title='TEST')
        cerebro.plot(style='candlestick',iplot=False)
    
    optimized_runs=0
#    optimized_runs = cerebro.run()
    if optimized_runs !=0:
        final_results_list = []
        for run in optimized_runs:
            for strategy in run:
                PnL = round(strategy.broker.get_value() - 300000,2)
                sharpe = strategy.analyzers.sharpe_ratio.get_analysis()
                tradean= strategy.analyzers.trades.get_analysis()
                try:
                    final_results_list.append([strategy.params.fixage, 
                    strategy.params.trail, PnL, sharpe['sharperatio'],tradean.total['total'],tradean.won['total'],tradean.lost['total'],tradean.long['total']]) 
                    sort_by_sharpe = sorted(final_results_list, key=lambda x: x[7], 
                                reverse=True)
                except:
                    print('not enough data...')
        for line in sort_by_sharpe[:5]:
            
            print(line)

def parse_args():
    parser = argparse.ArgumentParser(description='MultiData Strategy')

    parser.add_argument('--data0', '-d0',
                        default=f"moex_1m/{tickers[0]}.csv",
    #                    default='A.csv',
                        help='1st data into the system')

    parser.add_argument('--data1', '-d1',
                        default=f"moex_1m/{tickers[1]}.csv",
                        help='2nd data into the system')

    parser.add_argument('--fromdate', '-f',
                        default='2016-06-17',
                        help='Starting date in YYYY-MM-DD format')

    parser.add_argument('--todate', '-t',
                        default='2021-09-17',
                        help='Starting date in YYYY-MM-DD format')
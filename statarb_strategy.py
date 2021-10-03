import backtrader as bt
from matplotlib.pyplot import plot
from pinescript import line2arr, barssince, valuewhen, na, nz
import numpy as np
import datetime
from datetime import time, timedelta




class PairTradingStrategy(bt.Strategy):
    params = dict(
        period=130,
        stake=1,
        qty1=1,
        qty2=1,
        printout=True,
        upper=2,
        lower=-2,
        up_medium=0.1,
        low_medium=-0.1,
        status=0,
        portfolio_value=80000,
    )

    def log(self, txt, dt=None):
        if self.p.printout:
            dt = dt or self.data.datetime[0]
            dt = bt.num2date(dt)
            print('%s, %s' % (dt.isoformat(), txt))

    def notify_order(self, order):
        if order.status in [bt.Order.Submitted, bt.Order.Accepted]:
            return  # Await further notifications

        if order.status == order.Completed:
            if order.isbuy():
                buytxt = 'BUY COMPLETE, %.2f' % order.executed.price
                self.log(buytxt, order.executed.dt)
            else:
                selltxt = 'SELL COMPLETE, %.2f' % order.executed.price
                self.log(selltxt, order.executed.dt)

        elif order.status in [order.Expired, order.Canceled, order.Margin]:
            self.log('%s ,' % order.Status[order.status])
            pass  # Simply log

        # Allow new orders
        self.orderid = None

    def __init__(self):
        # To control operation entries
        self.orderid = None
        self.qty1 = self.p.qty1
        self.qty2 = self.p.qty2
        self.upper_limit = self.p.upper
        self.lower_limit = self.p.lower
        self.up_medium = self.p.up_medium
        self.low_medium = self.p.low_medium
        self.status = self.p.status
        self.portfolio_value = self.p.portfolio_value
        self.x_dataclose =self.datas[1]
        self.y_dataclose =self.datas[0]
        self.zscore = self.datas[0].zscore

        # Signals performed with PD.OLS :
    #    self.transform = btind.OLS_TransformationN(self.data0, self.data1,
    #                                               period=self.p.period)
     #   df1 = pd.DataFrame({'y':(y['price']),'x':(x['price'])})
      #  print(df1)
   #     self.est = sm.OLS(self.y_dataclose, self.x_dataclose)
        print(' ')
    #    self.zscore = self.transform.zscore

        # Checking signals built with StatsModel.API :
        # self.ols_transfo = btind.OLS_Transformation(self.data0, self.data1,
        #                                             period=self.p.period,
        #                                             plot=True)

    def next(self):
        
        if self.orderid:
            return  # if an order is active, no new orders are allowed
        
        if self.p.printout:
            
            print('Self  len:', len(self))
            print('Data0 len:', len(self.data0))
            print('Data1 len:', len(self.data1))
            print('Data0 len == Data1 len:',
                  len(self.data0) == len(self.data1))

            print('Data0 dt:', self.data0.datetime.datetime())
            print('Data1 dt:', self.data1.datetime.datetime())

        print('status is', self.status)
        print('zscore is', self.zscore[0])

        # Step 2: Check conditions for SHORT & place the order
        # Checking the condition for SHORT
        if (self.zscore[0] > self.upper_limit) and (self.status ==0 ) and (self.zscore[-1]<self.upper_limit):
           
            # Calculating the number of shares for each stock
       #     value = 0.5 * self.portfolio_value  # Divide the cash equally
          #  y = int(value / (self.y_dataclose))  # Find the number of shares for Stock1
           # x = int(value / (self.x_dataclose))  # Find the number of shares for Stock2
     #       print('y + self.qty1 is', y + self.qty2)
      #      print('x + self.qty2 is', x + self.qty1)
            

            # Placing the order
            self.log('SELL CREATE %s, price = %.2f, qty = %d' % ("RIU1", self.y_dataclose[0], self.qty1))
            self.sell(data=self.datas[0], size=(self.qty1))  # Place an order for buying y + qty2 shares
            self.log('BUY CREATE %s, price = %.2f, qty = %d' % ("RIZ1", self.x_dataclose[0], self.qty2))
            self.buy(data=self.datas[1], size=(self.qty2))  # Place an order for selling x + qty1 shares

            # Updating the counters with new value
      #      self.qty1 = y  # The new open position quantity for Stock1 is x shares
       #     self.qty2 = x  # The new open position quantity for Stock2 is y shares

            self.status = 1  # The current status is "short the spread"

            # Step 3: Check conditions for LONG & place the order
            # Checking the condition for LONG
        elif (self.zscore[0] < self.lower_limit) and (self.status == 0) and (self.zscore[-1] > self.lower_limit) :

            # Calculating the number of shares for each stock
      #      value = 0.5 * self.portfolio_value  # Divide the cash equally
    #        y = int(value / (self.y_dataclose)) 
     #       x = int(value / (self.x_dataclose))  # Find the number of shares for Stock1
             # Find the number of shares for Stock2
       #     print('x + self.qty2 is', x + self.qty2)
        #    print('y + self.qty1 is', y + self.qty1)

            # Place the order
            self.log('BUY CREATE %s, price = %.2f, qty = %d' % ("RIU1", self.y_dataclose[0], self.qty1))
            self.buy(data=self.datas[0], size=(self.qty1))  # Place an order for buying x + qty1 shares
            self.log('SELL CREATE %s, price = %.2f, qty = %d' % ("RIZ1", self.x_dataclose[0], self.qty2))
            self.sell(data=self.datas[1], size=(self.qty2))  # Place an order for selling y + qty2 shares

            # Updating the counters with new value
      #      self.qty2 = x  # The new open position quantity for Stock1 is x shares
       #     self.qty1 = y  # The new open position quantity for Stock2 is y shares
            self.status = 2  # The current status is "long the spread"


            # Step 4: Check conditions for No Trade
            # If the z-score is within the two bounds, close all
        
        elif ((self.status==2) and  (self.zscore[0] > 0 and self.zscore[-1] < 0)):
            self.log('CLOSE  %s, price = %.2f' % ("RIU1", self.y_dataclose[0]))
            self.close(self.datas[0], size=(self.qty1))
            self.log('CLOSE  %s, price = %.2f' % ("RIZ1", self.x_dataclose[0]))
            self.close(self.datas[1], size=(self.qty2))
            self.status = 0
        elif ((self.status==1 ) and (self.zscore[0] < 0 and self.zscore[-1] > 0) ):
            self.log('CLOSE  %s, price = %.2f' % ("RIU1", self.y_dataclose[0]))
            self.close(self.datas[0], size=(self.qty1))
            self.log('CLOSE  %s, price = %.2f' % ("RIZ1", self.x_dataclose[0]))
            self.close(self.datas[1], size=(self.qty2))
            self.status = 0


    def stop(self):
        print('==================================================')
        print('Starting Value - %.2f' % self.broker.startingcash)
        print('Ending   Value - %.2f' % self.broker.getvalue())
        print('==================================================')

               

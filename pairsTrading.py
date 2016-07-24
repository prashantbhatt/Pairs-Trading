# from datetime import datetime, timedelta
import time
import pandas as pd
from pandas.tseries.offsets import BDay
import numpy as np
from statsmodels.tsa.stattools import coint
import mysql.connector
import unicodedata
from datetime import date,timedelta

start_time = time.clock()

# -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-% Define the strategic variables %-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.

open_position_threshold = 2.5
open_position,close_position = 'O', 'C'
coint_pvalue_threshold = 0.01
change_in_zscore = .2

total_per_trade_capital = 1000000                         #  ₹ 10 Lac
trade_capital_per_stock = total_per_trade_capital/2       

Bank_account    = 10000000                                   #  ₹ 1 Cr.
Minimum_Balance =  1000000                                 #  ₹ 10 Lac is to be maintained for absorbing the loss, if any, as
                                                          #  we take the positions so that we don't miss a good one 
normal_exit,force_exit,expired,pnl_exit = 'n', 'f', 'e','p'

Maximum_hold_period = 30                    

ratio_ucl = 5
ratio_lcl = .20

pnl_look_back_days = 6
bearable_loss = 1000
too_much_loss = 10000
# -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.% Future Expiry Dates for the year %.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.
expiry_thursdays = [(2012,1,26), (2012,2,23), (2012,3,29), (2012,4,26), (2012,5,31), (2012,6,28),
                    (2012,7,26), (2012,8,30), (2012,9,27), (2012,10,25), (2012,11,29), (2012,12,27),
                    (2013,1,31), (2013,2,28), (2013,3,28), (2013,4,25), (2013,5,30), (2013,6,27),
                    (2013,7,25), (2013,8,29), (2013,9,26), (2013,10,31), (2013,11,28), (2013,12,26),
                    (2014,1,29), (2014,2,27), (2014,3,27), (2014,4,24), (2014,5,29), (2014,6,26),
                    (2014,7,31), (2014,8,28), (2014,9,25), (2014,10,30), (2014,11,27), (2014,12,25),                    
                    (2015,1,29), (2015,2,26), (2015,3,26), (2015,4,30), (2015,5,28), (2015,6,25),
                    (2015,7,30), (2015,8,27), (2015,9,24), (2015,10,29), (2015,11,26), (2015,12,31), (2016,1,28),
                    (2016,2,25),(2016,3,31),(2016,4,28),(2016,5,26),(2016,6,30),(2016,7,28),(2016,8,25),(2016,9,29),
                    (2016,10,27),(2016,11,24),(2016,12,29)]
# -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.

# -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.% Make SQL Server connection %.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.

conn = mysql.connector.connect(user="root",password="cynosure_0190",host='localhost',database="Pairs_Trading")
mycursor = conn.cursor()

# -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.
# Ask the user for a start date and create a list for all the days the trading test will run

user_trade_start_date = pd.to_datetime(raw_input("Please enter the date to start trade strategy test"
                                                 "{format yyyy-mm-dd}: "))

user_trade_end_date = pd.to_datetime(raw_input("Please enter the end date for trade strategy test"
                                                 "{format yyyy-mm-dd}: "))

# -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-% Read the data from csv %-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.

Future = pd.read_csv("FuturesData.csv",index_col = 'Date', 
                      date_parser=lambda x: pd.to_datetime(x,format='%d/%m/%Y'))

# take data of year before and after the start date from the csv data
# Futures = Futures.loc[(user_trade_start_date - BDay(260)).date() :(user_trade_start_date + BDay(260)).date()]
# Futures = Future.loc[(user_trade_start_date - BDay(260)).date() :Future.index[-1]]

Futures = Future.loc[(user_trade_start_date - BDay(260)).date() : user_trade_end_date.date()]
# type(Futures.index) should be pandas.tseries.index.DatetimeIndex

# create a list for all the days the trading test will run
strategy_test_duration = Futures.index[Futures.index >= user_trade_start_date]

# print strategy_test_duration

# Create the Metric dataframes
# metric_start_index = user_trade_start_date - BDay(7)
# metric_index = Futures.index[Futures.index >= metric_start_index]
# print "metric_index: %s " % metric_index

pos_metric = pd.DataFrame(index = strategy_test_duration)
pnl_metric = pd.DataFrame(index = strategy_test_duration)
pnl_on_exit_metric = pd.DataFrame(index = strategy_test_duration)
#--------------------------------------------------------------x-----------------------------------------------------

# -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.
# Read the file for industry wise division of stock futures and generate a dictionary of industry wise ticker 
# symbols

ls = []
# file_read = open("IndustryDivision.txt", "r")
file_read = open("IndustryDiv.txt", "r")
for lines in file_read.readlines():
    lines = lines.rstrip()
    x = lines.split()
    ls.append(x)

industry_names = []
industry_wis_ticker = {}

l = 0
for k in ls:
    industry_names.append(k.pop(0))
    industry_wis_ticker[industry_names[l]]=ls[l]
    l += 1
# print industry_wis_ticker                                                          #for unit testing

# -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.% Define functions to be called in the code below %-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-. 

#-----------------------------------------Function to remove the unicode on SQL fetch--------------------------------    

def get_ExpiryDate(Date,ExpiryDate_list):
#     print "Date: %s" % Date
    
#     if Date.year == 2015:
#         trade_date_month = Date.month
# #         print "date list read %s" %ExpiryDate_list[trade_date_month - 1]
#         ExpiryDate = pd.to_datetime(ExpiryDate_list[trade_date_month],format= '%Y,%m,%d')
#         CurrntmnthExpiryDate = pd.to_datetime(ExpiryDate_list[trade_date_month-1],format= '%Y,%m,%d')
#         if Date > CurrntmnthExpiryDate.date():
#             ExpiryDate = pd.to_datetime(ExpiryDate_list[trade_date_month + 1],format= '%Y,%m,%d')    
# #         print ExpiryDate
#         return ExpiryDate.date()
#     else:
#         print "The year we are testing is 2015, you sent the wrong trade date"

    for e,i in enumerate(ExpiryDate_list):
        if i[0] == Date.year and i[1] == Date.month:
            if (i[2] - (Date.day)) > 0:
                ed = ExpiryDate_list[e+1]
                s = str(ed[0]) + ',' + str(ed[1]) + ',' + str(ed[2])
                expiry_date = pd.to_datetime(str(s),format= '%Y,%m,%d')
                return expiry_date.date()
            else:
                ed = ExpiryDate_list[e+2]
                s = str(ed[0]) + ',' + str(ed[1]) + ',' + str(ed[2])
                expiry_date = pd.to_datetime(str(s),format= '%Y,%m,%d')
                return expiry_date.date()
#
#--------------------------------------------------------------x-----------------------------------------------------


#-----------------------------------------Function to remove the unicode on SQL fetch---------------- ----------------    
def uncoded_list_of_rows(rows):
    rows_as_lists = []

    for i in rows:
        lst=[]
        for j in range(len(i)):
            if not isinstance(i[j],str):
                lst.append(i[j])
            else:
                lst.append(i[j].decode('unicode_escape').encode('ascii','ignore'))
        rows_as_lists.append(lst)
    return rows_as_lists
#--------------------------------------------------------------x-----------------------------------------------------

#----------------------------------Functions to check if consecutive zscore meet threshold too-----------------------

def consecutive_zscores_meet_positive_threshold(lst):
    lst = np.array(lst)
    meets_threshold = lst > open_position_threshold
    if False in meets_threshold:
        return False
    else:
        return True        
                                
def consecutive_zscores_meet_negative_threshold(lst):
    lst = np.array(lst)
    meets_threshold = lst < -open_position_threshold 
    if False in meets_threshold:
        return False
    else:
        return True        
#--------------------------------------------------------------x-----------------------------------------------------

#--------------------------------Function to check if any previous position is open for a normal exit----------------    
def position_is_open(Date,Indust,StkA,StkB,open_position):
    
#     print "Date which reached the position_open function %s" %Date
    
    is_open_query = ("SELECT DateofTrade,Industry,ShortStk,LongStk,PositionFlag,"
                     "ShortPrice,ShortQty,LongPrice,LongQty from FutureLots "
                     "WHERE DateofTrade <= %s and Industry = %s and ShortStk = %s and LongStk = %s" 
                     " and PositionFlag = %s")
    is_open_data = (Date,Indust,StkA,StkB,open_position)
    
    mycursor.execute(is_open_query,is_open_data)
    
    fetch_long_short_pair = mycursor.fetchall()
    
#     print "fetch result :%s" % fetch_long_short_pair
    
    if not mycursor.rowcount:
#         print "There were no open rows for %s, %s <= %s:" % (StkA,StkB,Date)
        return False,False
    else:
        list_of_row = uncoded_list_of_rows(fetch_long_short_pair)
        return True,list_of_row

#--------------------------------------------------------------x-----------------------------------------------------


#--------------------------------Function to check if any stock from the pair being traded was 
#----------------------------------------- traded before on the same date--------------------------------------------

def stocks_traded_today(Date,Indust,StkA,StkB):
    
    traded_today_query = ("SELECT DateofTrade,Industry,ShortStk,LongStk"
                          " from FutureLots "
                          "WHERE DateofTrade = %s and Industry = %s and (ShortStk in (%s,%s) or LongStk in (%s,%s))" )
    traded_today_data = (Date,Indust,StkA,StkB,StkA,StkB)
    
    mycursor.execute(traded_today_query,traded_today_data)
    
    fetch_data = mycursor.fetchall()
    
#     print "fetch result :%s" % fetch_long_short_pair
    
    if not mycursor.rowcount:
#         print "There were no open rows for %s, %s <= %s:" % (StkA,StkB,Date)
        return False
    else:
#         list_of_row = uncoded_list_of_rows(fetch_data)
        return True
#--------------------------------Function to get rows which fulfill one criteria of normal exit --------------------    

def can_exit(Date,Indust,StkA,StkB,open_position,trade_date_zscore):
    if trade_date_zscore < 0:
        
        can_exit_query = ("SELECT A.DateofTrade,A.Industry, A.ShortStk, A.LongStk, A.PositionFlag, " 
                          "A.ShortPrice, A.ShortQty, A.LongPrice, A.LongQty, A.EntryZscore "  
                          "from FutureLots A "
                          "WHERE A.DateofTrade <= %s and A.Industry = %s and A.ShortStk = %s and A.LongStk = %s and"
                          " A.PositionFlag = %s and %s < 0 and (A.EntryZscore - %s) <= -%s")
        
        can_exit_data = (Date,Indust,StkA,StkB,open_position,trade_date_zscore,trade_date_zscore,change_in_zscore)
        
        mycursor.execute(can_exit_query,can_exit_data)
        
        fetched_rows = mycursor.fetchall()
        
        if not mycursor.rowcount:
#             print "Cannot Exit %s, %s" % (StkA,StkB)
            return False,False
        else:
            list_of_row = uncoded_list_of_rows(fetched_rows)
#             print "Can Exit %s, %s" % (StkA,StkB)
            return True,list_of_row
        
    elif trade_date_zscore > 0:
        
        can_exit_query = ("SELECT A.DateofTrade,A.Industry, A.ShortStk, A.LongStk, A.PositionFlag, " 
                          "A.ShortPrice, A.ShortQty, A.LongPrice, A.LongQty, A.EntryZscore "  
                          "from FutureLots A "
                          "WHERE A.DateofTrade <= %s and A.Industry = %s and A.ShortStk = %s and A.LongStk = %s and"
                          " A.PositionFlag = %s and %s > 0 and (A.EntryZscore - %s) >= %s")
        
        can_exit_data = (Date,Indust,StkA,StkB,open_position,trade_date_zscore,trade_date_zscore,change_in_zscore)
        
        mycursor.execute(can_exit_query,can_exit_data)
        
        fetched_rows = mycursor.fetchall()
        
        if not mycursor.rowcount:
#             print "Cannot Exit %s, %s" % (StkA,StkB)
            return False,False
        else:
            list_of_row = uncoded_list_of_rows(fetched_rows)
#             print "Can Exit %s, %s" % (StkA,StkB)
            return True,list_of_row
#--------------------------------------------------------------x-----------------------------------------------------

#-----------------------------Get rows which are held for too long to forcefully exit them---------------------------

 
def positions_held_too_long(Date,Indust,StkA,StkB,open_position,max_hld_prd):

   
    history_date = (Date - BDay(max_hld_prd)).date()
#     print "date_of_trade: %s and history_date %s" % (Date, history_date)
    
    is_held_long_query = ("SELECT DateofTrade,Industry,ShortStk,LongStk,PositionFlag,ShortPrice,"
                          "ShortQty,LongPrice,LongQty from FutureLots "
                          "WHERE Industry = %s and ShortStk = %s and LongStk = %s and PositionFlag = %s"
                          " and DateofTrade <= %s")
    
    is_held_long_data = (Indust,StkA,StkB,open_position,history_date)
    
    mycursor.execute(is_held_long_query,is_held_long_data)
    
    fetch_longtime_held_pair = mycursor.fetchall()
    
    if not mycursor.rowcount:
        message = "Not held long"
        return False,False
        if StkA == 'BHARATFORG' and StkB == 'HAVELLS':
            print "ShortStk = 'BHARATFORG' and LongStk = 'HAVELLS':"
            print "Date which reached the position_open function %s" %Date
            print "history_date %s" % history_date
            print "NOT LONG HELD"

    else:
        longtime_held_rows = uncoded_list_of_rows(fetch_longtime_held_pair)
        return True,longtime_held_rows
        if StkA == 'BHARATFORG' and StkB == 'HAVELLS':
            print "ShortStk = 'BHARATFORG' and LongStk = 'HAVELLS':"
            print "Date which reached the position_open function %s" %Date
            print "history_date %s" % history_date
            print "Long_held %, %" %(True,longtime_held_rows)

#---------------------------------------------------------x----------------------------------------------------------
def unexited_expiry_positions(Indust,StkA,StkB,open_position,Date):
    
    is_expired_query = ("SELECT DateofTrade, Industry, ShortStk, LongStk, PositionFlag, " 
                        "ShortPrice, ShortQty, LongPrice, LongQty "  
                        "from FutureLots "
                        "WHERE Industry = %s and ShortStk = %s and LongStk = %s and"
                        " PositionFlag = %s and ExpiryDate = %s ")

    is_expired_data = (Indust,StkA,StkB,open_position,Date)

    mycursor.execute(is_expired_query,is_expired_data)

    expired_rows = mycursor.fetchall()

    if not mycursor.rowcount:
    #             print "Cannot Exit %s, %s" % (StkA,StkB)
        return False,False
    else:
        expired_rowlist = uncoded_list_of_rows(expired_rows)
    #             print "Can Exit %s, %s" % (StkA,StkB)
        return True,expired_rowlist

#---------------------------------------------For Unit testing select one industry ----------------------------------

# industry_wis_tickers_unit_test = {}
# industry_wis_tickers_unit_test['Personal-&-Household-Goods'] = industry_wis_ticker['Personal-&-Household-Goods']
# industry_wis_tickers_unit_test['Sample'] = ['GLENMARK','HINDUNILVR'] 

#set the flag for first time run to true, turn it False after the first run
first_time_run = True

# ----------------------------------------------------%Main-para%---------------------------------------------------

for industry,organisations in industry_wis_ticker.items():
# for industry,organisations in industry_wis_tickers_unit_test.items():
    print "industry: %s \n" % industry
    if first_time_run:
        print " ********************************************************************************************** "
        print "Going to perform a test of trade strategy for %d days" % len(strategy_test_duration)
        print " -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-. "
        print "The start date of testing trade strategy is %s " % user_trade_start_date.date()
        print " -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-. "
        print "We test the cointegration for 260 days before each trade day"
        print " -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-. "
                            
    Stocklabels = organisations
#     print "Stocklabels %s" % Stocklabels
#     print "length of Stocklabels %d" %len(Stocklabels)                                          # for unit testing
    for a in range(len(Stocklabels)):
#         for b in range(len(Stocklabels)):
#         print "inside a loop $a:%s" % a
        for b in range(a+1,len(Stocklabels)):
            S1 = Stocklabels[a]
            S2 = Stocklabels[b]
#             print "S1 = %s and S2 = %s:" % (S1, S2)    
            #             if S1 == 'BHARATFORG' and S2 == 'HAVELLS':
#                 print "ShortStk = 'BHARATFORG' and LongStk = 'HAVELLS':"
#                 print "inside a and b loop $a: %s,b: %s" %(a,b)
            two_consecutive_zscores = []
            if S1 != S2:                                                       # Skip the pairs of same stock
                
                # Create a column for this pair in the metric dataframe and fill zeros for now
                colname = S1 + "-" + S2
                pos_metric.loc[:,colname] = 0
                pnl_metric.loc[:,colname] = 0
                pnl_on_exit_metric.loc[:, colname] = 0
                counter1_longtime_held_positions = 0
                counter2_longtime_held_positions = 0
                
                #read the list of dates; cointegration test has to be conducted for every date read, looking 260 days in history 
#                 print "These are selected after a,b loop: %s,%s" %  (S1,S2)
#                 l = strategy_test_duration[0:5]                               # this line for unit test

                date_index_list = []
    
                for current_date in strategy_test_duration:
#                  for current_date in l:                                         # this line for unit test
#                     if S1 == 'BHARATFORG' and S2 == 'HAVELLS':
#                         print "******************Checkpoint 1:new day*************************"
#                         print "current date %s: " %current_date
                        
                    date_of_trade = current_date.date()
                    # Set a flag to keep tab on test for cointegration
                    pair_is_cointegrated = False
                    
                    date_index_list.append(date_of_trade)
                    
                    counter1_longtime_held_positions += 1            
                    counter2_longtime_held_positions += 1 
                    
                    coint_test_start_date = (date_of_trade - BDay(260)).date()
                    
                    StkPrice_S1 = 0
                    StkPrice_S2 = 0
                    
                    if first_time_run:
                        print "Cointegration test start date for trading strategy test is: %s" %coint_test_start_date
                        print " -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-. "
                        print "Lay back as we calculate the returns you would have made!"
                        print " ********************************************************************************************** "
                        first_time_run = False
     
                    FuturesData = Futures.loc[coint_test_start_date:date_of_trade,[S1,S2]]
#                     print FuturesData.iloc[:,0:5].head()                                 #for unit testing

                    # check if any data is fetched
                    S1_has_data = any(FuturesData[S1][FuturesData[S1] > 0].index)
                    S2_has_data = any(FuturesData[S2][FuturesData[S2] > 0].index)


                    if S1_has_data and S2_has_data :
                        startrow_A = min(FuturesData[S1][FuturesData[S1] > 0].index)
                        startrow_B = min(FuturesData[S2][FuturesData[S2] > 0].index)

                        # set the start row to an index from wherer both stocks have data
                        if startrow_A > startrow_B:
                            startrow = startrow_A
                        else:
                            startrow = startrow_B

                        # end row is till the last row of the total data we have
                        endrow = FuturesData.iloc[-1].index

                        # form a data dictionary for the two stocks 
                        data_dict = {S1: FuturesData.loc[startrow:FuturesData.index[-1].date(), S1],
                                     S2: FuturesData.loc[startrow:FuturesData.index[-1].date(), S2]}

#                         if S1 == 'BHARATFORG' and S2 == 'HAVELLS':
#                             print "******************Checkpoint 2:HSa some data***********************"
#                             print "counter1_longtime_held_positions %s" % counter1_longtime_held_positions            
#                             print "counter2_longtime_held_positions %s" % counter2_longtime_held_positions             
#  -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.
                        # checking if rolling mean of 60 days would be possible for the fetched data data
                        if len(data_dict[S1]) > 62 and len(data_dict[S2]) > 62 :

#                             if S1 == 'BHARATFORG' and S2 == 'HAVELLS':
#                                 print "******************Checkpoint 3:Has more than 60 rows atleast with non zero data******"
#                                 print "counter1_longtime_held_positions %s" % counter1_longtime_held_positions            
#                                 print "counter2_longtime_held_positions %s" % counter2_longtime_held_positions             
                            score = 0.0
                            pvalue = 0.0
                            # Decision on cointegration
                            result = coint(data_dict[S1], data_dict[S2])         #this statement tests for cointegration
                            score = result[0]
                            pvalue = float(result[1])
                            
                            if pvalue < coint_pvalue_threshold:
                                pair_is_cointegrated = True
#                                 if S1 == 'BHARATFORG' and S2 == 'HAVELLS':
#                                 print "****Has cointegration for today*********"
#                                 print "counter1_longtime_held_positions %s" % counter1_longtime_held_positions            
#                                 print "counter2_longtime_held_positions %s" % counter2_longtime_held_positions             
                            else:
                                pair_is_cointegrated = False
#                                 if S1 == 'BHARATFORG' and S2 == 'HAVELLS':
#                                     print "******************Checkpoint : No cointegration for today*********"
#                                     print "Lets Move ahead"            
                                
#  -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-. 

#  -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.% Here starts the trading part %-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.
    ########################    

                            # create a dataframe to calculate the price ratio and moving z-score
                            price_and_z = pd.DataFrame(data_dict)

                            # add a column having ratios of prices to the above dataframe
                            price_and_z.loc[:,'ratios'] = (
                                                    pd.Series(price_and_z[S1]/price_and_z[S2],index=price_and_z.index)
                                                          )
                            # add a column having "60 days rolling mean of ratios of prices" to the above dataframe
                            price_and_z.loc[ : ,'mov_avg_60days'] = ( 
                                                        price_and_z['ratios'].rolling(window=60,center=False).mean()
                                                                    )
                            # add a column having "60 days rolling std dev of ratios of prices" to the above dataframe    
                            price_and_z.loc[ : ,'std_dev_60days'] = (
                                                        price_and_z['ratios'].rolling(window=60,center=False).std()
                                                                    )
                            # add a column having "60 days rolling z-score of ratios of prices" to the above dataframe    
                            price_and_z.loc[ : ,'zscore'] = (
                                                        ((price_and_z['ratios'] -  price_and_z['mov_avg_60days'])/ 
                                                                                price_and_z['std_dev_60days'])
                                                            )
                            StkPrice_S1 = float(price_and_z.loc[date_of_trade][S1])
                            StkPrice_S2 = float(price_and_z.loc[date_of_trade][S2])
                            if StkPrice_S1 == 0 or StkPrice_S2 == 0:
#                                 print "S1 = %s ; S2 = %s " % (S1,S2)
                                continue
                            trade_date_ratioS1_S2 = float(price_and_z.ix[price_and_z.index[-1]]['ratios'])
                            trade_date_ratioS2_S1 = float(1/trade_date_ratioS1_S2)

    #                             print "while defining StkPrice_S1 %s" %StkPrice_S1
    #                             print "while defining StkPrice_S2 %s" %StkPrice_S2
                            # Get the previous one days zscores 
                            zscore_1day_before = float(price_and_z.ix[price_and_z.index[-2]]['zscore'])
                            trade_date_zscore = float(price_and_z.ix[price_and_z.index[-1]]['zscore'])
                            two_consecutive_zscores = [zscore_1day_before,trade_date_zscore]
    #
                            qty_S1 = int(trade_capital_per_stock / StkPrice_S1)
                            qty_S2 = int(trade_capital_per_stock / StkPrice_S2)
    # ----------------------------------------------------------------------------------------------------------------------------

# -----------------------------------------------Exit pairs expiring today ---------------------------------------------------
                            shrt_positions_held_for_longtime = False    
                            shrt_positions_held_for_longtime = positions_held_too_long(date_of_trade,industry,S1,S2,
                                                                                   open_position,Maximum_hold_period)[0] 
                            long_positions_held_for_longtime = False
                            long_positions_held_for_longtime = positions_held_too_long(date_of_trade,industry,S2,S1,
                                                                                   open_position,Maximum_hold_period)[0] 

                            if (shrt_positions_held_for_longtime and counter1_longtime_held_positions >= 30) :

    #                             if S1 == 'BHARATFORG' and S2 == 'HAVELLS':
    #                                 print "******************Checkpoint 5: indide if --short force exit*********"
    #                                 print "counter1_longtime_held_positions %s" % counter1_longtime_held_positions            
    #                                 print "counter2_longtime_held_positions %s" % counter2_longtime_held_positions             

                                longheld_rows = positions_held_too_long(date_of_trade,industry,S1,S2,open_position,
                                                                           Maximum_hold_period)[1]
                                for ro in longheld_rows:

                                    Short_Stk_Entry_Price = float(ro[5])
                                    Short_quantity = float(ro[6])
                                    Long_Stk_Entry_Price = float(ro[7])
                                    Long_quantity = float(ro[8])

                                    GainLoss_from_short = float((Short_Stk_Entry_Price - StkPrice_S1)* Short_quantity)
                                    GainLoss_from_long = float((StkPrice_S2 - Long_Stk_Entry_Price) * Long_quantity)
                                    pnl = float(GainLoss_from_short + GainLoss_from_long)
                                    expr = float(StkPrice_S1/StkPrice_S2)
                                    longheld_query = ("UPDATE FutureLots set PositionFlag = %s, PosCloseDate = %s,"
                                                    "PosClosePriceShort = %s, PosClosePriceLong = %s,"
                                                    "ExitZscore = %s,PnL = %s,ExitType = %s,ExPR = %s "
                                                    "WHERE DateofTrade = %s and Industry = %s and ShortStk = %s "
                                                    "and LongStk = %s and PositionFlag = %s")
                                    update_longheld_data = (close_position,date_of_trade,StkPrice_S1,StkPrice_S2,
                                                         trade_date_zscore,pnl,force_exit,expr,ro[0], ro[1], S1, S2,
                                                         open_position)

                                    mycursor.execute(longheld_query, update_longheld_data)
                                    conn.commit()
                                    pnl_metric.loc[date_of_trade, colname] = pnl
                                    pnl_on_exit_metric.loc[date_of_trade, colname] = pnl
    #                                 print "stocks %s, %s" %(S1,S2)
                                    print "force_exit-short"

                                    amount_invested_in_short = Short_Stk_Entry_Price * Short_quantity 
                                    amount_invested_in_Long  = Long_Stk_Entry_Price * Long_quantity
                                    Total_amount_invested = amount_invested_in_short + amount_invested_in_Long
                                    Bank_account +=   Total_amount_invested + pnl
                                    counter1_longtime_held_positions = 0
                                    print Bank_account

                            if (long_positions_held_for_longtime and counter2_longtime_held_positions >= 30) :
    #                             if S1 == 'BHARATFORG' and S2 == 'HAVELLS':
    #                                 print "******************Checkpoint 5: indide if --long force exit*********"
    #                                 print "counter1_longtime_held_positions %s" % counter1_longtime_held_positions            
    #                                 print "counter2_longtime_held_positions %s" % counter2_longtime_held_positions             


                                longheld_roz = positions_held_too_long(date_of_trade,industry,S2,S1,open_position,
                                                                           Maximum_hold_period)[1]
                                for roz in longheld_roz:

                                    Short_Stk_Entry_Price = float(roz[5])
                                    Short_quantity = float(roz[6])
                                    Long_Stk_Entry_Price = float(roz[7])
                                    Long_quantity = float(roz[8])

                                    GainLoss_from_short = float((Short_Stk_Entry_Price - StkPrice_S2)* Short_quantity)
                                    GainLoss_from_long = float((StkPrice_S1 - Long_Stk_Entry_Price) * Long_quantity)
                                    pnl = float(GainLoss_from_short + GainLoss_from_long)
                                    expr = float(StkPrice_S2/StkPrice_S1)
                                    longheld_query = ("UPDATE FutureLots set PositionFlag = %s, PosCloseDate = %s,"
                                                    "PosClosePriceShort = %s, PosClosePriceLong = %s,"
                                                    "ExitZscore = %s,PnL = %s,ExitType = %s,ExPR = %s "
                                                    "WHERE DateofTrade = %s and Industry = %s and ShortStk = %s "
                                                    "and LongStk = %s and PositionFlag = %s")
                                    update_longheld_data = (close_position,date_of_trade,StkPrice_S2,StkPrice_S1,
                                                         trade_date_zscore,pnl,force_exit,expr,roz[0], roz[1], S2, S1,
                                                         open_position)

                                    mycursor.execute(longheld_query, update_longheld_data)
                                    conn.commit()
                                    pnl_metric.loc[date_of_trade, colname] = pnl
                                    pnl_on_exit_metric.loc[date_of_trade, colname] = pnl
    #                                 print "stocks %s, %s" %(S2,S1)
                                    print "force_exit-long"

                                    amount_invested_in_short = Short_Stk_Entry_Price * Short_quantity 
                                    amount_invested_in_Long  = Long_Stk_Entry_Price * Long_quantity
                                    Total_amount_invested = amount_invested_in_short + amount_invested_in_Long
                                    Bank_account +=   Total_amount_invested + pnl
                                    counter2_longtime_held_positions = 0
                                    print Bank_account

    # ----------------------------------------------------------------------------------------------------------------------------

#                             if ((date_of_trade == ExpiryDate) and 
                            if unexited_expiry_positions(industry,S1,S2,open_position,date_of_trade)[0]:

                                exp_rows = unexited_expiry_positions(industry,S1,S2,open_position,date_of_trade)[1]

                                for exp_roz in exp_rows:

                                    Short_Stk_Entry_Price = float(exp_roz[5])
                                    Short_quantity = float(exp_roz[6])
                                    Long_Stk_Entry_Price = float(exp_roz[7])
                                    Long_quantity = float(exp_roz[8])

    #                                 print "stocks %s, %s" %(S1,S2)
    #                                 print "Short_Stk_Entry_Price: %s " % Short_Stk_Entry_Price
    #                                 print "Short_quantity %s" % Short_quantity
    #                                 print "Long_Stk_Entry_Price %s" % Long_Stk_Entry_Price
    #                                 print "Long_quantity %s" % Long_quantity
    #                                 print "StkPrice_S1 %s:" % StkPrice_S1
    #                                 print "StkPrice_S2 %s" % StkPrice_S2

                                    GainLoss_from_short = float((Short_Stk_Entry_Price - StkPrice_S1)* Short_quantity)
                                    GainLoss_from_long = float((StkPrice_S2 - Long_Stk_Entry_Price) * Long_quantity)
                                    pnl = float(GainLoss_from_short + GainLoss_from_long)
                                    expr = float(StkPrice_S1/StkPrice_S2)
    #                                 print "GainLoss_from_short %s" % GainLoss_from_short
    #                                 print "GainLoss_from_long %s" % GainLoss_from_long
    #                                 print "pnl %s" %pnl

                                    expire_ro_query = ("UPDATE FutureLots set PositionFlag = %s, PosCloseDate = %s,"
                                                       "PosClosePriceShort = %s, PosClosePriceLong = %s,"
                                                       "ExitZscore = %s,"
                                                       " PnL = %s, ExitType = %s,ExPR = %s "
                                                       "WHERE ExpiryDate = %s and Industry = %s and ShortStk = %s "
                                                       "and LongStk = %s and PositionFlag = %s")
                                    expiry_ro_data = (close_position,date_of_trade,StkPrice_S1,StkPrice_S2,
                                                      trade_date_zscore,pnl,expired,expr,date_of_trade,
                                                      industry, S1, S2, open_position)

                                    mycursor.execute(expire_ro_query, expiry_ro_data)
                                    conn.commit()
                                    pnl_metric.loc[date_of_trade, colname] = pnl
                                    pnl_on_exit_metric.loc[date_of_trade, colname] = pnl
    #                                 print "stocks %s, %s" %(S1,S2)
                                    print "position_expired"
                                    amount_invested_in_short = Short_Stk_Entry_Price * Short_quantity 
                                    amount_invested_in_Long  = Long_Stk_Entry_Price * Long_quantity
                                    Total_amount_invested = amount_invested_in_short + amount_invested_in_Long
                                    Bank_account +=   Total_amount_invested + pnl
    #                                 print  "amount_invested_in_short %s"  % amount_invested_in_short 
    #                                 print  "amount_invested_in_Long %s"  % amount_invested_in_Long
    #                                 print  " Total_amount_invested %s"  %  Total_amount_invested
                                    print "Bank_account %s" % Bank_account

#                             if ((date_of_trade == ExpiryDate) and
                            if unexited_expiry_positions(industry,S2,S1,open_position,date_of_trade)[0]:

                                exp_rows = unexited_expiry_positions(industry,S2,S1,open_position,date_of_trade)[1]

                                for exp_row in exp_rows:

                                    Short_Stk_Entry_Price = float(exp_row[5])
                                    Short_quantity = float(exp_row[6])
                                    Long_Stk_Entry_Price = float(exp_row[7])
                                    Long_quantity = float(exp_row[8])
    #                                 print "stocks %s, %s" % (S2,S1)
    #                                 print "Short_Stk_Entry_Price: %s " % Short_Stk_Entry_Price
    #                                 print "Short_quantity %s" % Short_quantity
    #                                 print "Long_Stk_Entry_Price %s" % Long_Stk_Entry_Price
    #                                 print "Long_quantity %s" % Long_quantity
    #                                 print "StkPrice_S1 %s:" % StkPrice_S1
    #                                 print "StkPrice_S2 %s" % StkPrice_S2

                                    GainLoss_from_short = float((Short_Stk_Entry_Price - StkPrice_S2)* Short_quantity)
                                    GainLoss_from_long = float((StkPrice_S1 - Long_Stk_Entry_Price) * Long_quantity)
                                    pnl = float(GainLoss_from_short + GainLoss_from_long)
                                    expr = float(StkPrice_S2/StkPrice_S1)
    #                                 print "GainLoss_from_short %s" % GainLoss_from_short
    #                                 print "GainLoss_from_loss %s" % GainLoss_from_long
    #                                 print "pnl %s" %pnl

                                    expire_ro_query = ("UPDATE FutureLots set PositionFlag = %s, PosCloseDate = %s,"
                                                       "PosClosePriceShort = %s, PosClosePriceLong = %s,"
                                                       "ExitZscore = %s,"
                                                       " PnL = %s, ExitType = %s,ExPR = %s "
                                                       "WHERE ExpiryDate = %s and Industry = %s and ShortStk = %s "
                                                       "and LongStk = %s and PositionFlag = %s")
                                    expiry_ro_data = (close_position,date_of_trade,StkPrice_S2,StkPrice_S1,
                                                      trade_date_zscore,pnl,expired,expr,date_of_trade,
                                                      industry, S2, S1, open_position)

                                    mycursor.execute(expire_ro_query, expiry_ro_data)
                                    conn.commit()
                                    pnl_metric.loc[date_of_trade, colname] = pnl
                                    pnl_on_exit_metric.loc[date_of_trade, colname] = pnl
    #                                 print "stocks %s, %s" %(S2,S1)
                                    print "position_expired"
                                    amount_invested_in_short = Short_Stk_Entry_Price * Short_quantity 
                                    amount_invested_in_Long  = Long_Stk_Entry_Price * Long_quantity
                                    Total_amount_invested = amount_invested_in_short + amount_invested_in_Long
                                    Bank_account +=   Total_amount_invested + pnl
    #                                 print  "amount_invested_in_short %s"  % amount_invested_in_short 
    #                                 print  "amount_invested_in_Long %s"  % amount_invested_in_Long
    #                                 print  " Total_amount_invested %s"  %  Total_amount_invested
                                    print "Bank_account %s" % Bank_account


    # ------------------------This will exit the positions normally if they meet the exit criteria------------------------------

                            if (position_is_open(date_of_trade,industry,S1,S2,open_position)[0] and
                                    can_exit(date_of_trade,industry,S1,S2,open_position,trade_date_zscore)[0]):

                                list_of_rows = can_exit(date_of_trade,industry,S1,S2,open_position,trade_date_zscore)[1]
                                
                                for row in list_of_rows:

                                    Short_Stk_Entry_Price = float(row[5])
                                    Short_quantity = float(row[6])
                                    Long_Stk_Entry_Price = float(row[7])
                                    Long_quantity = float(row[8])

                                    GainLoss_from_short = float((Short_Stk_Entry_Price - StkPrice_S1)* Short_quantity)
                                    GainLoss_from_long = float((StkPrice_S2 - Long_Stk_Entry_Price) * Long_quantity)
                                    pnl = float(GainLoss_from_short + GainLoss_from_long)
                                    expr = float(StkPrice_S1/StkPrice_S2)
                                    
                                    if  (Short_Stk_Entry_Price/Long_Stk_Entry_Price) >  (StkPrice_S1/StkPrice_S2):
                                        update_query = ("UPDATE FutureLots set PositionFlag = %s, PosCloseDate = %s,"
                                                        "PosClosePriceShort = %s, PosClosePriceLong = %s,"
                                                        "ExitZscore = %s,PnL = %s,ExitType = %s,ExPR = %s "
                                                        "WHERE DateofTrade = %s and Industry = %s and ShortStk = %s "
                                                        "and LongStk = %s and PositionFlag = %s")
                                        update_query_data = (close_position,date_of_trade,StkPrice_S1,StkPrice_S2,
                                                         trade_date_zscore,pnl,normal_exit,expr,row[0], row[1], S1, S2,
                                                         open_position)

                                        mycursor.execute(update_query, update_query_data)
                                        conn.commit()

                                        pnl_metric.loc[date_of_trade, colname] = pnl
                                        pnl_on_exit_metric.loc[date_of_trade, colname] = pnl
        #                                 print "stocks %s, %s" %(S1,S2)
                                        print "position_exit-short"
                                        amount_invested_in_short = Short_Stk_Entry_Price * Short_quantity 
                                        amount_invested_in_Long  = Long_Stk_Entry_Price * Long_quantity
                                        Total_amount_invested = amount_invested_in_short + amount_invested_in_Long
                                        Bank_account +=   Total_amount_invested + pnl
                                        print Bank_account

                            if (position_is_open(date_of_trade,industry,S2,S1,open_position)[0] and                          
                                    can_exit(date_of_trade,industry,S2,S1,open_position,trade_date_zscore)[0]):

                                list_of_rows = can_exit(date_of_trade,industry,S2,S1,open_position,trade_date_zscore)[1]
                                
                                if  (Short_Stk_Entry_Price/Long_Stk_Entry_Price) > (StkPrice_S2/StkPrice_S1):
                                    for row in list_of_rows:
                                        Short_Stk_Entry_Price = float(row[5])
                                        Short_quantity = float(row[6])
                                        Long_Stk_Entry_Price = float(row[7])
                                        Long_quantity = float(row[8])
                                        GainLoss_from_short = (Short_Stk_Entry_Price - StkPrice_S2)* Short_quantity
                                        GainLoss_from_long = (StkPrice_S1 - Long_Stk_Entry_Price) * Long_quantity
                                        pnl = GainLoss_from_short + GainLoss_from_long                                    
                                        expr = float(StkPrice_S2/StkPrice_S1)
                                        update_query = ("UPDATE FutureLots set PositionFlag = %s, PosCloseDate = %s,"
                                                        "PosClosePriceShort = %s, PosClosePriceLong = %s,"
                                                        "ExitZscore = %s,PnL = %s,ExitType = %s,ExPR = %s "
                                                        "WHERE DateofTrade = %s and Industry = %s and ShortStk = %s "
                                                        "and LongStk = %s and PositionFlag = %s")
                                        update_query_data = (close_position,date_of_trade,StkPrice_S2,StkPrice_S1,
                                                             trade_date_zscore,pnl,normal_exit,expr,row[0], row[1], S2, S1,
                                                             open_position)

                                        mycursor.execute(update_query, update_query_data)
                                        conn.commit()
                                        pnl_metric.loc[date_of_trade, colname] = pnl
                                        pnl_on_exit_metric.loc[date_of_trade, colname] = pnl
        #                                 print "stocks %s, %s" %(S2,S1)
                                        print "position_exit-long"
                                        amount_invested_in_short = Short_Stk_Entry_Price * Short_quantity 
                                        amount_invested_in_Long  = Long_Stk_Entry_Price * Long_quantity
                                        Total_amount_invested = amount_invested_in_short + amount_invested_in_Long
                                        Bank_account +=   Total_amount_invested + pnl
                                        print Bank_account
    # ----------------------------------------------------x---------------------------------------------------------------------
    # Check whether two consecutive ticks are favourable to open a position
                            if pair_is_cointegrated:   
                                two_tick_consecutive_descrease = False
                                if ((trade_date_zscore > open_position_threshold) and
                                        consecutive_zscores_meet_positive_threshold(two_consecutive_zscores)):
                                    truth_list = []

                                    for r in range(len(two_consecutive_zscores)):
                                        if r != len(two_consecutive_zscores)-1:
                                            truth_list.append(two_consecutive_zscores[r] > two_consecutive_zscores[r+1])
                                        if False in truth_list:
                                            two_tick_consecutive_descrease = False
                                        else:
                                            two_tick_consecutive_descrease = True 

                                two_tick_consecutive_increase = False
                                if ((trade_date_zscore < -open_position_threshold) and
                                        consecutive_zscores_meet_negative_threshold(two_consecutive_zscores)):
                                    truth_list = []

                                    for s in range(len(two_consecutive_zscores)):
                                        if s != len(two_consecutive_zscores)-1:
                                            truth_list.append(two_consecutive_zscores[s] < two_consecutive_zscores[s+1])
                                    if False in truth_list:
                                        two_tick_consecutive_increase = False
                                    else:
                                        two_tick_consecutive_increase = True 
#     # ----------------------------------------------------x---------------------------------------------------------------------

    # ----------------------------------------------------This will take the positions------------------------------------------
                            ExpiryDate = get_ExpiryDate(date_of_trade,expiry_thursdays)
                            

    
                            if pair_is_cointegrated:
                                stocks_already_traded_sameday = False    
                                stocks_already_traded_sameday = stocks_traded_today(date_of_trade,industry,S1,S2) 
#                                 if S1 == 'HINDUNILVR' and S2 == 'BATAINDIA':
#                                     print "date_of_trade: %s" % date_of_trade
#                                     epr1 = StkPrice_S1/StkPrice_S2
#                                     print "epr1(S1/S2): %s" % epr1    
#                                     epr2 = StkPrice_S2/StkPrice_S1
#                                     print "epr2(S2/S1): %s" % epr2    
#                                     print "trade_date_ratio: %s" % trade_date_ratio
#                                     print ("""position_is_open(date_of_trade,industry,S1,S2,
#                                         open_position)[0] : %s""" % position_is_open(date_of_trade,industry,S1,S2,open_position)[0])
#                                     print ("""position_is_open(date_of_trade,industry,S2,S1,
#                                         open_position)[0] : %s""" % position_is_open(date_of_trade,industry,S2,S1,open_position)[0])
#                                     print "enough_data %s" % enough_data
#                                     print "trade_date_zscore: %s" % trade_date_zscore
                                    
                                enough_data = True
                                if (date_of_trade.year == user_trade_end_date.date().year and
                                            date_of_trade.month >= user_trade_end_date.date().month):
                                    enough_data = False
#                                     print "enough_data: %s" % enough_data
                                # check if all conditions to open a position or to exit are met
                                
                                if ((trade_date_zscore > open_position_threshold)  
                                         and two_tick_consecutive_descrease 
                                         and not position_is_open(date_of_trade,industry,S1,S2,open_position)[0] 
                                         and not position_is_open(date_of_trade,industry,S2,S1,open_position)[0]
                                         and not stocks_already_traded_sameday
                                         and (trade_date_ratioS1_S2 < ratio_ucl)
                                         and (trade_date_ratioS1_S2 > ratio_lcl)
                                         and enough_data):
                                    print "preliminary conditions met to take short position"
                                    if (Bank_account - total_per_trade_capital) > Minimum_Balance:
                                        
                                        epr = float(StkPrice_S1/StkPrice_S2)
                                        open_short_query = ("INSERT INTO FutureLots "
                                                            "(DateofTrade,Industry,ShortStk, LongStk, PositionFlag, ShortPrice,"
                                                            "ShortQty,LongPrice,LongQty,EntryZscore,ExpiryDate,Pvalue,EPR)"
                                                            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                                        open_short_data = (date_of_trade,industry,S1,S2,open_position,StkPrice_S1,qty_S1,
                                                            StkPrice_S2,qty_S2,trade_date_zscore,ExpiryDate,pvalue,epr)

                                        mycursor.execute(open_short_query,open_short_data)

                                        conn.commit()
    #                                     print "stocks %s, %s" %(S1,S2)
                                        print "short_position_taken"  

                                        Bank_account -= (qty_S1 * StkPrice_S1 +  qty_S2 * StkPrice_S2)
                                        print Bank_account
                                    else:
                                        print "Low Bank Balance"
                                elif ((trade_date_zscore < -open_position_threshold) and 
                                            two_tick_consecutive_increase and
                                             not position_is_open(date_of_trade,industry,S1,S2,open_position)[0] and 
                                             not position_is_open(date_of_trade,industry,S2,S1,open_position)[0]
                                             and not stocks_already_traded_sameday
                                             and (trade_date_ratioS2_S1 < ratio_ucl)
                                             and (trade_date_ratioS2_S1 > ratio_lcl)
                                             and enough_data):

                                    print "preliminary conditions met to take long"
                                    if (Bank_account - total_per_trade_capital) > Minimum_Balance:
                                        epr = float(StkPrice_S2/StkPrice_S1)
                                        open_long_query = ("INSERT INTO FutureLots "
                                                           "(DateofTrade,Industry,ShortStk, LongStk, PositionFlag, ShortPrice,"
                                                           "ShortQty,LongPrice,LongQty,EntryZscore,ExpiryDate,Pvalue,EPR)"
                                                           " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

                                        open_long_data = (date_of_trade,industry,S2,S1,open_position,StkPrice_S2,qty_S2,
                                                          StkPrice_S1,qty_S1,trade_date_zscore,ExpiryDate,pvalue,epr)

                                        mycursor.execute(open_long_query,open_long_data)

                                        conn.commit()
    #                                     print "stocks %s, %s" %(S2,S1)
                                        print "long_position_taken"

                                        Bank_account -= (qty_S1 * StkPrice_S1 +  qty_S2 * StkPrice_S2)
                                        print Bank_account    
                                    else:
                                        print "Low Bank Balance"

# Create metrics to guage the performance 

                            if position_is_open(date_of_trade,industry,S1,S2,open_position)[0]:
# position metric
                                pos_metric.loc[date_of_trade, colname] = 1

                                rozz = position_is_open(date_of_trade,industry,S1,S2,open_position)[1]

                                consecutive_pnls = []

                                for roo in rozz:
                                    pnl = 0
                                    Short_Stk_Entry_Price = float(roo[5])
                                    Short_quantity = float(roo[6])
                                    Long_Stk_Entry_Price = float(roo[7])
                                    Long_quantity = float(roo[8])

                                    GainLoss_from_short = float((Short_Stk_Entry_Price - StkPrice_S1)* Short_quantity)
                                    GainLoss_from_long = float((StkPrice_S2 - Long_Stk_Entry_Price) * Long_quantity)
                                    pnl = float(GainLoss_from_short + GainLoss_from_long)
        #pnl metric              
                                if pnl_metric.loc[date_of_trade, colname] == 0:            
                                    pnl_metric.loc[date_of_trade, colname] = pnl

                                for ds in range(-pnl_look_back_days,0):
#                                     print "ds:%s " %ds
                                    if len(date_index_list) >= pnl_look_back_days :
                                        consecutive_pnls.append(float(pnl_metric.ix[date_index_list[ds]][colname]))
                                
#                                 print "consecutive_pnls : %s" %consecutive_pnls   
                                
                                pnl_truth_list =  np.array(consecutive_pnls) < - bearable_loss

                                if ((False not in pnl_truth_list) and 
                                    len(consecutive_pnls) == pnl_look_back_days) or pnl < -too_much_loss:
#                                      or pnl < - bearable_loss
                                    expr = float(StkPrice_S1/StkPrice_S2)
                                    
                                    consec_pnl_query = ("UPDATE FutureLots set PositionFlag = %s, PosCloseDate = %s,"
                                                        "PosClosePriceShort = %s, PosClosePriceLong = %s,"
                                                        "ExitZscore = %s,PnL = %s,ExitType = %s,ExPR = %s "
                                                        "WHERE DateofTrade = %s and Industry = %s and ShortStk = %s "
                                                        "and LongStk = %s and PositionFlag = %s")
                                    consec_pnl_data = (close_position,date_of_trade,StkPrice_S1,StkPrice_S2,
                                                       trade_date_zscore,pnl,pnl_exit,expr,roo[0], roo[1], S1, S2,
                                                       open_position)

                                    mycursor.execute(consec_pnl_query, consec_pnl_data)
                                    conn.commit()
                                    
                                    pnl_on_exit_metric.loc[date_of_trade, colname] = pnl
        #                                 print "stocks %s, %s" %(S1,S2)
                                    print "pnl_exit-short"

                                    amount_invested_in_short = Short_Stk_Entry_Price * Short_quantity 
                                    amount_invested_in_Long  = Long_Stk_Entry_Price * Long_quantity
                                    Total_amount_invested = amount_invested_in_short + amount_invested_in_Long
                                    Bank_account +=   Total_amount_invested + pnl
#                                     counter1_longtime_held_positions = 0
                                    print Bank_account

                            elif position_is_open(date_of_trade,industry,S2,S1,open_position)[0]:

                                pos_metric.loc[date_of_trade, colname] = 1
                                rozz = position_is_open(date_of_trade,industry,S2,S1,open_position)[1]
                                consecutive_pnls = []
                        
                                for roo in rozz:
                                    pnl = 0
                                    Short_Stk_Entry_Price = float(roo[5])
                                    Short_quantity = float(roo[6])
                                    Long_Stk_Entry_Price = float(roo[7])
                                    Long_quantity = float(roo[8])

                                    GainLoss_from_short = float((Short_Stk_Entry_Price - StkPrice_S2)* Short_quantity)
                                    GainLoss_from_long = float((StkPrice_S1 - Long_Stk_Entry_Price) * Long_quantity)
                                    pnl = float(GainLoss_from_short + GainLoss_from_long)

                                if pnl_metric.loc[date_of_trade, colname] == 0 :
                                    pnl_metric.loc[date_of_trade, colname] = pnl

                                for dl in range(-pnl_look_back_days,0):
#                                     print "dl: %s" %dl
                                    if len(date_index_list) >= pnl_look_back_days:
                                        consecutive_pnls.append(float(pnl_metric.ix[date_index_list[dl]][colname]))
                                
#                                 print "consecutive_pnls : %s" %consecutive_pnls   

                                pnl_truth_list =  np.array(consecutive_pnls) < -bearable_loss

                                if ((False not in pnl_truth_list) and 
                                    len(consecutive_pnls) == pnl_look_back_days) or pnl < -too_much_loss :
#                                     or pnl < -bearable_loss
                                    expr = float(StkPrice_S2/StkPrice_S1)
                                    
                                    consec_pnl_query = ("UPDATE FutureLots set PositionFlag = %s, PosCloseDate = %s,"
                                                        "PosClosePriceShort = %s, PosClosePriceLong = %s,"
                                                        "ExitZscore = %s,PnL = %s,ExitType = %s,ExPR = %s "
                                                        "WHERE DateofTrade = %s and Industry = %s and ShortStk = %s "
                                                        "and LongStk = %s and PositionFlag = %s")
                                    consec_pnl_data = (close_position,date_of_trade,StkPrice_S2,StkPrice_S1,
                                                       trade_date_zscore,pnl,pnl_exit,expr,roo[0], roo[1], S2, S1,
                                                       open_position)

                                    mycursor.execute(consec_pnl_query, consec_pnl_data)
                                    conn.commit()
                                    
                                    pnl_on_exit_metric.loc[date_of_trade, colname] = pnl
        #                                 print "stocks %s, %s" %(S1,S2)
                                    print "pnl_exit-long"

                                    amount_invested_in_short = Short_Stk_Entry_Price * Short_quantity 
                                    amount_invested_in_Long  = Long_Stk_Entry_Price * Long_quantity
                                    Total_amount_invested = amount_invested_in_short + amount_invested_in_Long
                                    Bank_account +=   Total_amount_invested + pnl
#                                     counter1_longtime_held_positions = 0
                                    print Bank_account

# ----------------------------------------------------x---------------------------------------------------------------------

# Finally close the SQL cursor and connection     
mycursor.close()
conn.close()
print "--Process Completed--"

pos_metric.loc[:,'no_of_open_positions'] = 0
pnl_metric.loc[:,'pnl_per_day'] = 0
pnl_on_exit_metric.loc[:,'pnl_on_exit_per_day'] = 0
for current_date in strategy_test_duration:
    date_of_trade = current_date.date()
    pos_metric.loc[date_of_trade,'no_of_open_positions'] = sum(pos_metric.loc[date_of_trade])
    pnl_metric.loc[date_of_trade,'pnl_per_day'] = sum(pnl_metric.loc[date_of_trade])
    pnl_on_exit_metric.loc[date_of_trade,'pnl_on_exit_per_day'] = sum(pnl_on_exit_metric.loc[date_of_trade])
writer = pd.ExcelWriter("F:\Final project\Codes and files\metric_sheet.xlsx", engine='xlsxwriter')
pos_metric.to_excel(writer, 'positions_per_day')
pnl_metric.to_excel(writer, 'pnl_per_day')
pnl_on_exit_metric.to_excel(writer,'pnl_on_exit')
writer.save()

print (time.clock() - start_time)/60, "minutes taken to complete"
# ----------------------------------------------------=======X=======-------------------------------------------------------
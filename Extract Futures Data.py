import pandas as pd
from nsepy import get_history
from datetime import date
import calendar

ticker = ["DATE", "ABIRLANUVO", "ACC", "ADANIENT", "ADANIPORTS", "ADANIPOWER", "AJANTPHARM", "ALBK", "AMARAJABAT",
          "AMBUJACEM",
          "ANDHRABANK", "APOLLOHOSP", "APOLLOTYRE", "ARVIND", "ASHOKLEY", "ASIANPAINT", "AUROPHARMA", "AXISBANK",
          "BAJAJ-AUTO", "BAJFINANCE", "BANKBARODA", "BANKINDIA", "BATAINDIA", "BEL", "BEML", "BHARATFORG", "BHARTIARTL",
          "BHEL", "BIOCON", "BOSCHLTD", "BPCL", "BRITANNIA", "CADILAHC", "CAIRN", "CANBK", "CASTROLIND", "CEATLTD",
          "CENTURYTEX", "CESC", "CIPLA", "COALINDIA", "COLPAL", "CONCOR", "CROMPGREAV", "CUMMINSIND", "DABUR", "DHFL",
          "DISHTV", "DIVISLAB", "DLF", "DRREDDY", "EICHERMOT", "ENGINERSIN", "EXIDEIND", "FEDERALBNK", "GAIL",
          "GLENMARK", "GMRINFRA", "GODREJCP", "GODREJIND", "GRANULES", "GRASIM", "HAVELLS", "HCLTECH", "HDFC",
          "HDFCBANK", "HDIL", "HEROMOTOCO", "HEXAWARE", "HINDALCO", "HINDPETRO", "HINDUNILVR", "HINDZINC", "IBREALEST",
          "IBULHSGFIN", "ICICIBANK", "ICIL", "IDBI", "IDEA", "IDFC", "IFCI", "IGL", "INDIACEM", "INDUSINDBK",
          "INFRATEL", "INFY", "IOB", "IOC", "IRB", "ITC", "JETAIRWAYS", "JINDALSTEL", "JISLJALEQS", "JPASSOCIAT",
          "JSWENERGY", "JSWSTEEL", "JUBLFOOD", "JUSTDIAL", "KOTAKBANK", "KPIT", "KSCL", "KTKBANK", "L&TFH",
          "LICHSGFIN", "LT", "LUPIN", "M&M", "M&MFIN", "MARICO", "MARUTI", "MCDOWELL-N", "MCLEODRUSS", "MINDTREE",
          "MOTHERSUMI", "MRF", "NCC", "NHPC", "NMDC", "NTPC", "OFSS", "OIL", "ONGC", "ORIENTBANK", "PAGEIND",
          "PCJEWELLER", "PETRONET", "PFC", "PIDILITIND", "PNB", "POWERGRID", "PTC", "RCOM", "RECLTD",
          "RELCAPITAL", "RELIANCE", "RELINFRA", "RPOWER", "SAIL", "SBIN", "SIEMENS", "SKSMICRO", "SOUTHBANK", "SRF",
          "SRTRANSFIN", "STAR", "SUNPHARMA", "SUNTV", "SYNDIBANK", "TATACHEM", "TATACOMM", "TATAELXSI", "TATAGLOBAL",
          "TATAMOTORS", "TATAMTRDVR", "TATAPOWER", "TATASTEEL", "TCS", "TECHM", "TITAN", "TORNTPHARM", "TV18BRDCST",
          "TVSMOTOR", "UBL", "UCOBANK", "ULTRACEMCO", "UNIONBANK", "UNITECH", "UPL", "VEDL", "VOLTAS", "WIPRO",
          "WOCKPHARMA", "YESBANK", "ZEEL"]

fut_dict = {}

key_list = ["jan_data", "feb_data", "mar_data", "apr_data", "may_data", "jun_data", "jul_data", "aug_data", "sep_data",
            "oct_data", "nov_data", "dec_data"]

last_thursdays = [(2015, 1, 29), (2015, 2, 26), (2015, 3, 26), (2015, 4, 30), (2015, 5, 28), (2015, 6, 25),
                  (2015, 7, 30), (2015, 8, 27), (2015, 9, 24), (2015, 10, 29), (2015, 11, 26), (2015, 12, 31)]

y = 2015
# for m in range(1, 13):

for m in range(1, 3):
    futures = pd.DataFrame(columns=ticker)

    nifty_fut = get_history(symbol=ticker[1],
                            start=date(y, m, 1),
                            end=date(y, m, calendar.monthrange(y, m)[1]),
                            futures=True, expiry_date=date(last_thursdays[m-1][0], last_thursdays[m-1][1],
                                                           last_thursdays[m-1][2]))[["Close"]]

    nifty_fut.reset_index(inplace=True)

    futures[[ticker[0]]] = nifty_fut[[0]]

    for i in range(1, len(ticker), 1):

        nifty_fut = get_history(symbol=ticker[i],
                                start=date(y, m, 1),
                                end=date(y, m, calendar.monthrange(y, m)[1]),
                                futures=True, expiry_date=date(last_thursdays[m-1][0], last_thursdays[m-1][1],
                                                               last_thursdays[m-1][2]))[["Close"]]
        nifty_fut.reset_index(inplace=True)
        futures[[ticker[i]]] = nifty_fut[["Close"]]

        print(ticker[i]), m

    fut_dict[key_list[m - 1]] = futures

x = 0
for x in range(len(key_list)):

    if key_list[x] in fut_dict:
        fut_dict[key_list[x]].to_csv("futstk_" + key_list[x] + ".csv", index=False, header=True)





#
# nifty_fut.to_csv("nifty_fut.csv", index=False, header=False)
#
# nifty_fut.to_csv("nifty_fut.csv", index=True, header=True)
#
# new_data = pd.DataFrame(nifty_fut[['Date','Close']])
#
# new_data.columns = ['Date','TCS']


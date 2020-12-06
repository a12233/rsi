from pprint import pprint
from finsymbols import symbols
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.techindicators import TechIndicators 
from pandas import DataFrame, Series
import csv
import sqlite3
import yfinance as yf
# import talib 
import pandas as pd
import numpy as np
from functools import lru_cache
from polygon import RESTClient
import pandas_datareader as pdr

# for alpha vantage
API_KEY = '5YWG8X1KMLO1AZLM'
sp500List = [['MMM'], ['ABT'], ['ABBV'], ['ABMD'], ['ACN'], ['ATVI'], ['ADBE'], ['AMD'], ['AAP'], ['AES'], ['AFL'], ['A'], ['APD'], ['AKAM'], ['ALK'], ['ALB'], ['ARE'], ['ALXN'], ['ALGN'], ['ALLE'], ['LNT'], ['ALL'], ['GOOGL'], ['GOOG'], ['MO'], ['AMZN'], ['AMCR'], ['AEE'], ['AAL'], ['AEP'], ['AXP'], ['AIG'], ['AMT'], ['AWK'], ['AMP'], ['ABC'], ['AME'], ['AMGN'], ['APH'], ['ADI'], ['ANSS'], ['ANTM'], ['AON'], ['AOS'], ['APA'], ['AIV'], ['AAPL'], ['AMAT'], ['APTV'], ['ADM'], ['ANET'], ['AJG'], ['AIZ'], ['T'], ['ATO'], ['ADSK'], ['ADP'], ['AZO'], ['AVB'], ['AVY'], ['BKR'], ['BLL'], ['BAC'], ['BK'], ['BAX'], ['BDX'], ['BRK-B'], ['BBY'], ['BIO'], ['BIIB'], ['BLK'], ['BA'], ['BKNG'], ['BWA'], ['BXP'], ['BSX'], ['BMY'], ['AVGO'], ['BR'], ['BF-B'], ['CHRW'], ['COG'], ['CDNS'], ['CPB'], ['COF'], ['CAH'], ['KMX'], ['CCL'], ['CARR'], ['CTLT'], ['CAT'], ['CBOE'], ['CBRE'], ['CDW'], ['CE'], ['CNC'], ['CNP'], ['CERN'], ['CF'], ['SCHW'], ['CHTR'], ['CVX'], ['CMG'], ['CB'], ['CHD'], ['CI'], ['CINF'], ['CTAS'], ['CSCO'], ['C'], ['CFG'], ['CTXS'], ['CLX'], ['CME'], ['CMS'], ['KO'], ['CTSH'], ['CL'], ['CMCSA'], ['CMA'], ['CAG'], ['CXO'], ['COP'], ['ED'], ['STZ'], ['COO'], ['CPRT'], ['GLW'], ['CTVA'], ['COST'], ['CCI'], ['CSX'], ['CMI'], ['CVS'], ['DHI'], ['DHR'], ['DRI'], ['DVA'], ['DE'], ['DAL'], ['XRAY'], ['DVN'], ['DXCM'], ['FANG'], ['DLR'], ['DFS'], ['DISCA'], ['DISCK'], ['DISH'], ['DG'], ['DLTR'], ['D'], ['DPZ'], ['DOV'], ['DOW'], ['DTE'], ['DUK'], ['DRE'], ['DD'], ['DXC'], ['EMN'], ['ETN'], ['EBAY'], ['ECL'], ['EIX'], ['EW'], ['EA'], ['EMR'], ['ETR'], ['EOG'], ['EFX'], ['EQIX'], ['EQR'], ['ESS'], ['EL'], ['ETSY'], ['EVRG'], ['ES'], ['RE'], ['EXC'], ['EXPE'], ['EXPD'], ['EXR'], ['XOM'], ['FFIV'], ['FB'], ['FAST'], ['FRT'], ['FDX'], ['FIS'], ['FITB'], ['FE'], ['FRC'], ['FISV'], ['FLT'], ['FLIR'], ['FLS'], ['FMC'], ['F'], ['FTNT'], ['FTV'], ['FBHS'], ['FOXA'], ['FOX'], ['BEN'], ['FCX'], ['GPS'], ['GRMN'], ['IT'], ['GD'], ['GE'], ['GIS'], ['GM'], ['GPC'], ['GILD'], ['GL'], ['GPN'], ['GS'], ['GWW'], ['HAL'], ['HBI'], ['HIG'], ['HAS'], ['HCA'], ['PEAK'], ['HSIC'], ['HSY'], ['HES'], ['HPE'], ['HLT'], ['HFC'], ['HOLX'], ['HD'], ['HON'], ['HRL'], ['HST'], ['HWM'], ['HPQ'], ['HUM'], ['HBAN'], ['HII'], ['IEX'], ['IDXX'], ['INFO'], ['ITW'], ['ILMN'], ['INCY'], ['IR'], ['INTC'], ['ICE'], ['IBM'], ['IP'], ['IPG'], ['IFF'], ['INTU'], ['ISRG'], ['IVZ'], ['IPGP'], ['IQV'], ['IRM'], ['JKHY'], ['J'], ['JBHT'], ['SJM'], ['JNJ'], ['JCI'], ['JPM'], ['JNPR'], ['KSU'], ['K'], ['KEY'], ['KEYS'], ['KMB'], ['KIM'], ['KMI'], ['KLAC'], ['KHC'], ['KR'], ['LB'], ['LHX'], ['LH'], ['LRCX'], ['LW'], ['LVS'], ['LEG'], ['LDOS'], ['LEN'], ['LLY'], ['LNC'], ['LIN'], ['LYV'], ['LKQ'], ['LMT'], ['L'], ['LOW'], ['LUMN'], ['LYB'], ['MTB'], ['MRO'], ['MPC'], ['MKTX'], ['MAR'], ['MMC'], ['MLM'], ['MAS'], ['MA'], ['MKC'], ['MXIM'], ['MCD'], ['MCK'], ['MDT'], ['MRK'], ['MET'], ['MTD'], ['MGM'], ['MCHP'], ['MU'], ['MSFT'], ['MAA'], ['MHK'], ['TAP'], ['MDLZ'], ['MNST'], ['MCO'], ['MS'], ['MOS'], ['MSI'], ['MSCI'], ['MYL'], ['NDAQ'], ['NOV'], ['NTAP'], ['NFLX'], ['NWL'], ['NEM'], ['NWSA'], ['NWS'], ['NEE'], ['NLSN'], ['NKE'], ['NI'], ['NSC'], ['NTRS'], ['NOC'], ['NLOK'], ['NCLH'], ['NRG'], ['NUE'], ['NVDA'], ['NVR'], ['ORLY'], ['OXY'], ['ODFL'], ['OMC'], ['OKE'], ['ORCL'], ['OTIS'], ['PCAR'], ['PKG'], ['PH'], ['PAYX'], ['PAYC'], ['PYPL'], ['PNR'], ['PBCT'], ['PEP'], ['PKI'], ['PRGO'], ['PFE'], ['PM'], ['PSX'], ['PNW'], ['PXD'], ['PNC'], ['POOL'], ['PPG'], ['PPL'], ['PFG'], ['PG'], ['PGR'], ['PLD'], ['PRU'], ['PEG'], ['PSA'], ['PHM'], ['PVH'], ['QRVO'], ['PWR'], ['QCOM'], ['DGX'], ['RL'], ['RJF'], ['RTX'], ['O'], ['REG'], ['REGN'], ['RF'], ['RSG'], ['RMD'], ['RHI'], ['ROK'], ['ROL'], ['ROP'], ['ROST'], ['RCL'], ['SPGI'], ['CRM'], ['SBAC'], ['SLB'], ['STX'], ['SEE'], ['SRE'], ['NOW'], ['SHW'], ['SPG'], ['SWKS'], ['SLG'], ['SNA'], ['SO'], ['LUV'], ['SWK'], ['SBUX'], ['STT'], ['STE'], ['SYK'], ['SIVB'], ['SYF'], ['SNPS'], ['SYY'], ['TMUS'], ['TROW'], ['TTWO'], ['TPR'], ['TGT'], ['TEL'], ['FTI'], ['TDY'], ['TFX'], ['TER'], ['TXN'], ['TXT'], ['TMO'], ['TIF'], ['TJX'], ['TSCO'], ['TT'], ['TDG'], ['TRV'], ['TFC'], ['TWTR'], ['TYL'], ['TSN'], ['UDR'], ['ULTA'], ['USB'], ['UAA'], ['UA'], ['UNP'], ['UAL'], ['UNH'], ['UPS'], ['URI'], ['UHS'], ['UNM'], ['VFC'], ['VLO'], ['VAR'], ['VTR'], ['VRSN'], ['VRSK'], ['VZ'], ['VRTX'], ['VIAC'], ['V'], ['VNT'], ['VNO'], ['VMC'], ['WRB'], ['WAB'], ['WMT'], ['WBA'], ['DIS'], ['WM'], ['WAT'], ['WEC'], ['WFC'], ['WELL'], ['WST'], ['WDC'], ['WU'], ['WRK'], ['WY'], ['WHR'], ['WMB'], ['WLTW'], ['WYNN'], ['XEL'], ['XRX'], ['XLNX'], ['XYL'], ['YUM'], ['ZBRA'], ['ZBH'], ['ZION'], ['ZTS']]


# sp500 = symbols.get_sp500_symbols()
# for i in sp50: 
#     sp500List.append(i['symbol'][:-1])
# pprint(sp500List)

# ts = TimeSeries(key=API_KEY, output_format='pandas')
#print(data.head())
# for symbol in sp500List:
#     data, meta_data = ti.get_rsi(symbol=symbol, interval='daily')
#     pprint(data.tail(3))

def test(): 
    sp500List = [['MMM'], ['ABT'], ['ABBV'], ['ABMD'], ['ACN'], ['ATVI'], ['ADBE'], ['AMD'], ['AAP'], ['AES'], ['AFL'], ['A'], ['APD'], ['AKAM'], ['ALK'], ['ALB'], ['ARE'], ['ALXN'], ['ALGN'], ['ALLE'], ['LNT'], ['ALL'], ['GOOGL'], ['GOOG'], ['MO'], ['AMZN'], ['AMCR'], ['AEE'], ['AAL'], ['AEP'], ['AXP'], ['AIG'], ['AMT'], ['AWK'], ['AMP'], ['ABC'], ['AME'], ['AMGN'], ['APH'], ['ADI'], ['ANSS'], ['ANTM'], ['AON'], ['AOS'], ['APA'], ['AIV'], ['AAPL'], ['AMAT'], ['APTV'], ['ADM'], ['ANET'], ['AJG'], ['AIZ'], ['T'], ['ATO'], ['ADSK'], ['ADP'], ['AZO'], ['AVB'], ['AVY'], ['BKR'], ['BLL'], ['BAC'], ['BK'], ['BAX'], ['BDX'], ['BRK-B'], ['BBY'], ['BIO'], ['BIIB'], ['BLK'], ['BA'], ['BKNG'], ['BWA'], ['BXP'], ['BSX'], ['BMY'], ['AVGO'], ['BR'], ['BF-B'], ['CHRW'], ['COG'], ['CDNS'], ['CPB'], ['COF'], ['CAH'], ['KMX'], ['CCL'], ['CARR'], ['CTLT'], ['CAT'], ['CBOE'], ['CBRE'], ['CDW'], ['CE'], ['CNC'], ['CNP'], ['CERN'], ['CF'], ['SCHW'], ['CHTR'], ['CVX'], ['CMG'], ['CB'], ['CHD'], ['CI'], ['CINF'], ['CTAS'], ['CSCO'], ['C'], ['CFG'], ['CTXS'], ['CLX'], ['CME'], ['CMS'], ['KO'], ['CTSH'], ['CL'], ['CMCSA'], ['CMA'], ['CAG'], ['CXO'], ['COP'], ['ED'], ['STZ'], ['COO'], ['CPRT'], ['GLW'], ['CTVA'], ['COST'], ['CCI'], ['CSX'], ['CMI'], ['CVS'], ['DHI'], ['DHR'], ['DRI'], ['DVA'], ['DE'], ['DAL'], ['XRAY'], ['DVN'], ['DXCM'], ['FANG'], ['DLR'], ['DFS'], ['DISCA'], ['DISCK'], ['DISH'], ['DG'], ['DLTR'], ['D'], ['DPZ'], ['DOV'], ['DOW'], ['DTE'], ['DUK'], ['DRE'], ['DD'], ['DXC'], ['EMN'], ['ETN'], ['EBAY'], ['ECL'], ['EIX'], ['EW'], ['EA'], ['EMR'], ['ETR'], ['EOG'], ['EFX'], ['EQIX'], ['EQR'], ['ESS'], ['EL'], ['ETSY'], ['EVRG'], ['ES'], ['RE'], ['EXC'], ['EXPE'], ['EXPD'], ['EXR'], ['XOM'], ['FFIV'], ['FB'], ['FAST'], ['FRT'], ['FDX'], ['FIS'], ['FITB'], ['FE'], ['FRC'], ['FISV'], ['FLT'], ['FLIR'], ['FLS'], ['FMC'], ['F'], ['FTNT'], ['FTV'], ['FBHS'], ['FOXA'], ['FOX'], ['BEN'], ['FCX'], ['GPS'], ['GRMN'], ['IT'], ['GD'], ['GE'], ['GIS'], ['GM'], ['GPC'], ['GILD'], ['GL'], ['GPN'], ['GS'], ['GWW'], ['HAL'], ['HBI'], ['HIG'], ['HAS'], ['HCA'], ['PEAK'], ['HSIC'], ['HSY'], ['HES'], ['HPE'], ['HLT'], ['HFC'], ['HOLX'], ['HD'], ['HON'], ['HRL'], ['HST'], ['HWM'], ['HPQ'], ['HUM'], ['HBAN'], ['HII'], ['IEX'], ['IDXX'], ['INFO'], ['ITW'], ['ILMN'], ['INCY'], ['IR'], ['INTC'], ['ICE'], ['IBM'], ['IP'], ['IPG'], ['IFF'], ['INTU'], ['ISRG'], ['IVZ'], ['IPGP'], ['IQV'], ['IRM'], ['JKHY'], ['J'], ['JBHT'], ['SJM'], ['JNJ'], ['JCI'], ['JPM'], ['JNPR'], ['KSU'], ['K'], ['KEY'], ['KEYS'], ['KMB'], ['KIM'], ['KMI'], ['KLAC'], ['KHC'], ['KR'], ['LB'], ['LHX'], ['LH'], ['LRCX'], ['LW'], ['LVS'], ['LEG'], ['LDOS'], ['LEN'], ['LLY'], ['LNC'], ['LIN'], ['LYV'], ['LKQ'], ['LMT'], ['L'], ['LOW'], ['LUMN'], ['LYB'], ['MTB'], ['MRO'], ['MPC'], ['MKTX'], ['MAR'], ['MMC'], ['MLM'], ['MAS'], ['MA'], ['MKC'], ['MXIM'], ['MCD'], ['MCK'], ['MDT'], ['MRK'], ['MET'], ['MTD'], ['MGM'], ['MCHP'], ['MU'], ['MSFT'], ['MAA'], ['MHK'], ['TAP'], ['MDLZ'], ['MNST'], ['MCO'], ['MS'], ['MOS'], ['MSI'], ['MSCI'], ['MYL'], ['NDAQ'], ['NOV'], ['NTAP'], ['NFLX'], ['NWL'], ['NEM'], ['NWSA'], ['NWS'], ['NEE'], ['NLSN'], ['NKE'], ['NI'], ['NSC'], ['NTRS'], ['NOC'], ['NLOK'], ['NCLH'], ['NRG'], ['NUE'], ['NVDA'], ['NVR'], ['ORLY'], ['OXY'], ['ODFL'], ['OMC'], ['OKE'], ['ORCL'], ['OTIS'], ['PCAR'], ['PKG'], ['PH'], ['PAYX'], ['PAYC'], ['PYPL'], ['PNR'], ['PBCT'], ['PEP'], ['PKI'], ['PRGO'], ['PFE'], ['PM'], ['PSX'], ['PNW'], ['PXD'], ['PNC'], ['POOL'], ['PPG'], ['PPL'], ['PFG'], ['PG'], ['PGR'], ['PLD'], ['PRU'], ['PEG'], ['PSA'], ['PHM'], ['PVH'], ['QRVO'], ['PWR'], ['QCOM'], ['DGX'], ['RL'], ['RJF'], ['RTX'], ['O'], ['REG'], ['REGN'], ['RF'], ['RSG'], ['RMD'], ['RHI'], ['ROK'], ['ROL'], ['ROP'], ['ROST'], ['RCL'], ['SPGI'], ['CRM'], ['SBAC'], ['SLB'], ['STX'], ['SEE'], ['SRE'], ['NOW'], ['SHW'], ['SPG'], ['SWKS'], ['SLG'], ['SNA'], ['SO'], ['LUV'], ['SWK'], ['SBUX'], ['STT'], ['STE'], ['SYK'], ['SIVB'], ['SYF'], ['SNPS'], ['SYY'], ['TMUS'], ['TROW'], ['TTWO'], ['TPR'], ['TGT'], ['TEL'], ['FTI'], ['TDY'], ['TFX'], ['TER'], ['TXN'], ['TXT'], ['TMO'], ['TIF'], ['TJX'], ['TSCO'], ['TT'], ['TDG'], ['TRV'], ['TFC'], ['TWTR'], ['TYL'], ['TSN'], ['UDR'], ['ULTA'], ['USB'], ['UAA'], ['UA'], ['UNP'], ['UAL'], ['UNH'], ['UPS'], ['URI'], ['UHS'], ['UNM'], ['VFC'], ['VLO'], ['VAR'], ['VTR'], ['VRSN'], ['VRSK'], ['VZ'], ['VRTX'], ['VIAC'], ['V'], ['VNT'], ['VNO'], ['VMC'], ['WRB'], ['WAB'], ['WMT'], ['WBA'], ['DIS'], ['WM'], ['WAT'], ['WEC'], ['WFC'], ['WELL'], ['WST'], ['WDC'], ['WU'], ['WRK'], ['WY'], ['WHR'], ['WMB'], ['WLTW'], ['WYNN'], ['XEL'], ['XRX'], ['XLNX'], ['XYL'], ['YUM'], ['ZBRA'], ['ZBH'], ['ZION'], ['ZTS']]
    ti = TechIndicators(key=API_KEY, output_format='pandas')
    ts = TimeSeries(key=API_KEY, output_format='pandas')
    data, meta_data = ts.get_daily(symbol='AMZN',outputsize='compact')
    lastPrice = data.head(1)['4. close']
    pprint(lastPrice)

    data, meta_data = ti.get_wma(symbol='AMZN', interval='daily', time_period=200,series_type='close')
    lastWma = data.tail(1).WMA
    pprint(lastPrice/lastWma)

# def testTALib(): 
#     close = numpy.random.random(100)
#     output = talib.SMA(close)
#     return output


def writeCSV():
    fields = ['Symbol',	'Current Price',	'Date',	'Time',	'Change',	'Open',	'High',	'Low	Volume',	'Trade Date',	'Purchase Price',	'Quantity',	'Commission',	'High Limit',	'Low Limit',	'Comment']  

    with open('test.csv', 'w') as f: 
        # using csv.writer method from CSV package 
        write = csv.writer(f) 
        
        write.writerow(fields) 
        write.writerows(sp500List) 

def testYfinance(): 
    symbol = 'AAPL'
    cache = {}
    conn = sqlite3.connect('aapl.db')
    c = conn.cursor()
    # c.execute('''CREATE TABLE IF NOT EXISTS stocks
    #          (symbol,data) ''')
    c.execute(f"""SELECT count(*) FROM sqlite_master WHERE type=\'table\' AND name=\'{symbol}\';""")
    flag = c.fetchall() 
    if flag[0][0] == 0:
        data = yf.Ticker('AAPL').history(period="max")
        data.to_sql(symbol, conn, schema=None, if_exists='fail', index=True, index_label=None, chunksize=None, dtype=None, method=None)
    else: 
        df = pdr.get_data_yahoo('AAPL', 2014, '20200926')
        print(df.head())

        # df = pd.read_sql(f"""SELECT * FROM {symbol}""", conn)
        df['Date'] = pd.to_datetime(df['Date'])
        print (df['Date'].dtype)
        df['year'] = df['Date'].dt.year
        df['month'] = df['Date'].dt.month
        df['day'] = df['Date'].dt.day
        df['week_day'] = df['Date'].dt.dayofweek
        df['week_day_name'] = df['Date'].dt.strftime('%A')
        df['close'] = df['Close']
        # df = df.asfreq('BM')
        # what about NaNs
        df.isnull().sum()
        df.ffill(inplace=True)  # to avoid problems with NaNs.
        # using close prices
        prices = df.close.copy()
        print(df)
        # we convert to DataFrame to make easy store more series.
        results_storage = prices.to_frame().copy()
        # print(results_storage)

        results_storage['year'] = df['year']
        results_storage['month'] = df['month']
        results_storage['day'] = df['day']
        results_storage['week_day'] = df['week_day']
        results_storage['week_day_name'] = df['week_day_name']   
        # results_storage['year'] = prices.index.year
        # results_storage['month'] = prices.index.month
        # results_storage['day'] = prices.index.day
        # results_storage['week_day'] = prices.index.dayofweek
        # results_storage['week_day_name'] = prices.index.strftime('%A')
        # print(results_storage)
        approach3 = results_storage.asfreq('BM').set_index(['year','month']).close.pct_change()
        print(approach3.tail(10))
    conn.close()
    return 



def getSP200DMA(): 
    cache = {}
    conn = sqlite3.connect('example.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS stocks
             (symbol,twoHundredDayAverage) ''')
    for i in sp500List: 
        symbol = str(i[0])
        c.execute('select * from stocks where symbol = ?',(symbol,))
        rows = c.fetchall() 
        if not rows:
            tick = yf.Ticker(symbol)
            twoHundredDMA = tick.info.get('twoHundredDayAverage')
            cache[symbol] = str(twoHundredDMA)
            c.execute("INSERT INTO stocks VALUES (?,?)",(symbol,str(twoHundredDMA),))
            conn.commit()
        else: 
            pass 
    
    print(cache)
    conn.close()
def testPolygon(): 
    key = "AKAMCR3KUKCLZS12NRIA"

    # RESTClient can be used as a context manager to facilitate closing the underlying http session
    # https://requests.readthedocs.io/en/master/user/advanced/#session-objects

    with RESTClient(key) as client:
        for s in sp500List: 
            stock = s[0]
            resp = client.stocks_equities_daily_open_close(stock[0], "2018-03-02")
            print(f"On: {resp.from_} {stock[0]} opened at {resp.open} and closed at {resp.close}")

if __name__ == "__main__":
    # getSP200DMA()
    # getSP200DMA()
    # testYfinance()
    today = '20200926'  # to make static this script.
    tckr = 'BBAS3.SA'  # Banco do Brasil SA
    # download data
    data = pdr.get_data_yahoo(tckr, 2014, today)
    data = data.asfreq('B')  # add frequency needed for some pandas functionalities releated with offsets
    data.columns = data.columns.map(lambda col: col.lower())
    data.head()  # first values
    print(data.tail())  # last values

    # what about NaNs
    data.isnull().sum()
    data.ffill(inplace=True)  # to avoid problems with NaNs.

    # using close prices
    prices = data.close.copy()
    # we convert to DataFrame to make easy store more series.
    results_storage = prices.to_frame().copy()


    # extract some date information
    results_storage['year'] = prices.index.year
    results_storage['month'] = prices.index.month
    results_storage['day'] = prices.index.day
    results_storage['week_day'] = prices.index.dayofweek
    results_storage['week_day_name'] = prices.index.strftime('%A')
    print(results_storage.tail(10))
    approach3 = results_storage.asfreq('BY')\
                            .set_index(['year'])\
                            .close\
                            .pct_change()
    print(approach3.tail(10))    

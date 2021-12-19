import pandas as pd
import csv
import urllib3
import gspread
import warnings
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor
from time import time, sleep
from datetime import datetime, timedelta
import subprocess
from math import ceil
from finviz.screener import Screener
import json
import re
import gc
import ctypes


warnings.filterwarnings('ignore')

def cv(ticker):
    while True:
        try:
            return http.request('GET',
                        'https://valuescout.co/spreadsheet/' + ticker + '?email=email@email.com&v=425')
            break
        except Exception as e:
            print(e)
            sleep(5)
            continue


def fin (ticker):
    while True:
        try:
            download = http.request(
                'GET',
                'https://www.valuespreadsheet.com/app/morningstar.php?ticker='+ ticker + '&page=' + 'financials'
                )
            decoded_content = download.data.decode('utf-8')
            tablesfin = pd.read_html(decoded_content)
            if tablesfin != None:
                #ld[0]['values'] = [tablesfin[0].columns.values.tolist()] + tablesfin[0].values.tolist()
                data = [tablesfin[0].columns.values.tolist()] + tablesfin[0].values.tolist()
                return data
            break
        except Exception as e:
            if str(e) != 'Document is empty' and str(e) != 'No tables found':
                print(e)
                sleep(5)
                continue
            print(e)
            return [[None]]
            break


def stat(ticker):
    while True:
        try:
            download = http.request(
                'GET',
                'https://www.valuespreadsheet.com/app/morningstar.php?ticker='+ ticker + '&page=' + 'stats'
                )
            decoded_content = download.data.decode('utf-8')
            tablesstat = pd.read_html(decoded_content)
            if tablesstat != None:
                #ld[1]['values'] = [tablesstat[1].columns.values.tolist()] + tablesstat[1].values.tolist()
                #ld[2]['values'] = [tablesstat[3].columns.values.tolist()] + tablesstat[3].values.tolist()
                #ld[3]['values'] = [tablesstat[5].columns.values.tolist()] + tablesstat[5].values.tolist()
                data = [[tablesstat[1].columns.values.tolist()] + tablesstat[1].values.tolist(),
                        [tablesstat[3].columns.values.tolist()] + tablesstat[3].values.tolist(),
                        [tablesstat[5].columns.values.tolist()] + tablesstat[5].values.tolist()]
            return data
            break
        except Exception as e:
            if str(e) != 'Document is empty' and str(e) != 'No tables found':
                print(e)
                sleep(5)
                continue
            return [[[None]], [[None]], [[None]]]
            break


def finviz(tickerlist, tickers=None):
    columns = ['0','65', '1','7','9','16','59','8','29','27','21','19','32','20']
    stock_list =list(Screener(table='Custom', custom=columns, tickers=tickers))
    #listofdictoftickers = [{x:j[x] for x in j if x != 'No.'} for i in tickerlist for j in list(stock_list) if i == j['Ticker']]

    #return listofdictoftickers + [{'Ticker':i, 'P/E': '-', 'PEG':'-', 'RSI': '-', 'Fwd P/E': '-', 'Inst Trans': '-',
    #                           'Insider Trans': '-', 'Sales past 5Y': '-', 'EPS past 5Y': '-',
    #                           'ROA': '-', 'EPS next 5Y': '-'}
    #                           for i in set(tickerlist)-{j['Ticker'] for j in listofdictoftickers}]
    m = []
    for i in tickerlist:
        f = 0
        for j in stock_list:
            if i == re.sub(r'\.', '-', j['Ticker']):
                m.append(j)
                #m.append({i:('0' if j[i] == '-' else j[i]) for i in j})
                f = 1
                stock_list.remove(j)
                break
        if f ==0:
            m.append({'Ticker':i, 'Price': '-', 'P/E': '-', 'PEG':'-', 'RSI': '-', 'EPS': '-', 'Fwd P/E': '-', 'Inst Trans': '-',
                       'Insider Trans': '-', 'Sales past 5Y': '-', 'EPS past 5Y': '-',
                       'ROA': '-', 'EPS next 5Y': '-'})
            #m.append({'Ticker':i, 'Price': '0', 'P/E': '0', 'PEG':'0', 'RSI': '0', 'EPS': '0', 'Fwd P/E': '0', 'Inst Trans': '0',
            #           'Insider Trans': '0', 'Sales past 5Y': '0', 'EPS past 5Y': '0',
            #           'ROA': '0', 'EPS next 5Y': '0'})
    return m
#print(finviz(['AA','FB','ASDF'],['AA','FB']))
#exit()


def tipranks(tickerlist):
    r = []
    for i in range(ceil(len(tickerlist)/1000)):
        while True:
            try:
                r = r + json.loads(http.request('GET', f'https://www.tipranks.com/api/stocks/stockAnalysisOverview/?tickers={",".join(tickerlist[i*1000:i*1000+1000])}').data) 
                break
            except Exception as e:
                print(f'Tipranks: {e}')
                sleep(5)
                continue
    r = [{x:i[x] for x in i if x in {'ticker', 'priceTarget'}} for i in r ]
    m = []
    for i in tickerlist:
        f = 0
        for j in r:
            if i == j['ticker']:
                #m.append(j['priceTarget'])
                if j['priceTarget'] == None:
                    m.append('-')
                    #m.append(0)
                else:
                    m.append(j['priceTarget'])
                f = 1
                r.remove(j)
                break
        if f ==0:
            m.append('-')
            #m.append(0)
    return m
#print(tipranks(['ATCC','FB','ASDF']))
#exit()


def zacks(tickerlist):
    l = []
    for i in range(ceil(len(tickerlist)/1000)):
        while True:
            try:
                r = json.loads(http.request('GET', f'https://quote-feed.zacks.com/index.php?t={",".join(tickerlist[i*1000:i*1000+1000])}').data) 
                l = l + [{j:r[i][j] for j in r[i] if j in {'ticker', 'zacks_rank_text'}} for i in r ]
                break
            except Exception as e:
                print(f'Zacks : {e}')
                sleep(5)
                continue
    r = [i for i in l if i !={}]
    m = []
    for i in tickerlist:
        f = 0
        for j in r:
            if i == j['ticker']:
                if j['zacks_rank_text'] == '':
                    m.append('-')
                    #m.append(0)
                else:
                    m.append(j['zacks_rank_text'])
                f = 1
                r.remove(j)
                break
        if f ==0:
            m.append('-')
            #m.append(0)
    return m
#print(zacks(['AAC','FB','ASDF']))
#exit()

def tradingview(tickerlist):
    while True:
        try:
            r = json.loads(http.request('POST', 'https://scanner.tradingview.com/america/scan',
                            body=json.dumps({"filter":[{"left":"name","operation":"nempty"},
                            {"left":"exchange","operation":"in_range","right":["AMEX","NASDAQ","NYSE"]}],
                            "options":{"lang":"en"},"markets":["america"],"symbols":{"query":{"types":[]},"tickers":[]},
                            "columns":["name","Recommend.All","enterprise_value_ebitda_ttm"],"sort":{"sortBy":"name","sortOrder":"asc"},"range":[0,20000]})).data)
            break
        except Exception as e:
            print(f'Tradingview : {e}')
            sleep(5)
            continue
    r = [i['d'] for i in r['data'] if i['d'][0] in tickerlist]
    m = []
    for i in tickerlist:
        f = 0
        for j in r:
            if i == j[0]:
                m.append(j[1:])
                f = 1
                r.remove(j)
                break
        if f == 0:
            m.append([None, '-'])
#            m.append([None, '0'])
    return m
#print([[0 if i[1] == None else i[1]] for i in tradingview(['AAA','AACI', 'FB', 'ASDF'])])
#exit()

def wallstreetzen(page):
    while True:
        try:
            r = http.request('GET', f'https://www.wallstreetzen.com/api/screener/companies?t=2&p={page+1}&s=t&sd=asc')
            break
        except Exception as e:
            print(f'wallstreetzen : {e}')
            sleep(5)
            continue
    return r


def wallstreetzenscore(tickerlist):
    wallstreetzendata = []
    while True:
        try:
            totalcompany = json.loads(http.request('GET', 'https://www.wallstreetzen.com/api/screener/companies/count?').data)['totalFilteredCompanies']
            break
        except Exception as e:
            print(f'wallstreetzen Total Company : {e}')
            sleep(5)
            continue
    with ThreadPoolExecutor(max_workers=12) as executor:
        for i in range(ceil(totalcompany/100)):
            wallstreetzendata.append(executor.submit(wallstreetzen,i))
    wallstreetzendata  = [json.loads(i.result().data.decode('utf-8')) for i in wallstreetzendata]
    r = [{k:wallstreetzendata[i]['companies'][j][k] for k in wallstreetzendata[i]['companies'][j]
            if k in {'ticker','z'}} for i in range(len(wallstreetzendata)) for j in wallstreetzendata[i]['companies']
            if wallstreetzendata[i]['companies'][j]['ticker'] in tickerlist]
    m = []
    for i in tickerlist:
        f = 0
        for j in r:
            if i == j['ticker']:
                m.append(j['z'])
                f = 1
                r.remove(j)
                break
        if f ==0:
            m.append(0)
    return m


def cnn(ticker):
    while True:
        try:
            r = http.request('GET', f'https://markets.money.cnn.com/research/quote/forecasts.asp?symb={ticker}')
            break
        except Exception as e:
            print(f'CNN: {e}')
            continue
    return r


def marketbeat(ticker):
    while True:
        try:
            r = http.request('GET', f'https://www.marketbeat.com/stocks/NASDAQ/{ticker}/')
            break
        except Exception as e:
            print(f'Marketbeat: {e}')
            continue
    return r


def yahoo(ticker):
    while True:
        try:
            tick = re.sub(r'\.', '-', ticker)
            r = http.request('GET',
                       f'https://query2.finance.yahoo.com/v10/finance/quoteSummary/{tick}?modules=upgradeDowngradeHistory%2CfinancialData%2CdefaultKeyStatistics')
            break
        except Exception as e:
            print(f'Yahoo: {e}')
            continue
    return r

def yahooevtoebitda(ticker):
    while True:
        try:
            tick = re.sub(r'\.', '-', ticker)
            r = http.request('GET',
                       f'https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{tick}?padTimeSeries=true&type=trailingEnterprisesValueEBITDARatio&merge=false&period1=493590046&period2={int(time())}')
            break
        except Exception as e:
            print(f'Yahoo: {e}')
            break
        except Exception as e:
            print(f'Yahoo: {e}')
            continue
    return r

def walletinvestor(ticker):
    while True:
        try:
            tick = re.sub(r'\.', '-', ticker)
            r = http.request('GET', f'https://walletinvestor.com/stock-forecast/{tick}-stock-prediction')
            break
        except Exception as e:
            print(f'Walletinvestor: {e}')
            continue
    return r


def combinesinglecalling(tickerlist):
    ld = []
    for i in range(len(tickerlist)):
        tl = []
        tim = time()
        with ThreadPoolExecutor(max_workers=12) as executor:
            tl.append(executor.submit(cnn, tickerlist[i]))
            tl.append(executor.submit(marketbeat, tickerlist[i]))
            tl.append(executor.submit(yahoo, tickerlist[i]))
            tl.append(executor.submit(yahooevtoebitda, tickerlist[i]))
            tl.append(executor.submit(walletinvestor,tickerlist[i].lower()))
        tl = [i.result().data.decode('utf-8') for i in tl]
        for j in range(len(tl)):
            if j == 0:
                try:
                    text = re.findall('median estimate represents.*', tl[j])[0]
                    cnnv = re.sub(r',', '', text[text.find(start:='Data">')+len(start):text.find('%</span>')])
                except Exception as e:
                    cnnv = '-'
            elif j == 1:
                try:
                    text = re.findall('bold m-0.*', tl[j])[0]
                    mbv = text[text.find(start:='bold m-0\'>')+len(start):text.find('out of 5 stars')]
                except Exception as e:
                    mbv = '-'
            elif j == 2:
                while True:
                    try:
                        yd =  json.loads(tl[j])
                        try:
                            yadv1 = f"{yd['quoteSummary']['result'][0]['upgradeDowngradeHistory']['history'][0]['firm']} : to {yd['quoteSummary']['result'][0]['upgradeDowngradeHistory']['history'][0]['toGrade']}"
                        except Exception as e:
                            yadv1 = '-'
                        try:
                            yadv2 = f"{yd['quoteSummary']['result'][0]['upgradeDowngradeHistory']['history'][1]['firm']} : to {yd['quoteSummary']['result'][0]['upgradeDowngradeHistory']['history'][1]['toGrade']}"
                        except:
                            yadv2 = '-'
                        try:
                            ytprice = yd['quoteSummary']['result'][0]['financialData']['targetMeanPrice']['raw']
                        except:
                            ytprice = '-'
                       # try:
                       #     evtoebitda = yd['quoteSummary']['result'][0]['defaultKeyStatistics']['enterpriseToEbitda']['raw']
                       # except:
                       #     evtoebitda = '-'
                        break
                    except Exception as e:
                        print(f'Yahoo Combine: {e}')
                        yadv1 = '-'
                        yadv2 = '-'
                        ytprice = '-'
                        break
            elif j == 3:
                while True:
                    try:
                        yd =  json.loads(tl[j])
                        try:
                            evtoebitda = yd['timeseries']['result'][0]['trailingEnterprisesValueEBITDARatio'][-1]['reportedValue']['raw']
                        except:
                            evtoebitda = '-'
                        break
                    except Exception as e:
                        print(f'Yahoo EV/ EBITDA Combine: {e}')
                        evtoebitda = '-'
                        break
            elif j == 4:
                try:
                    text = re.findall(f'/stock-forecast/{tickerlist[i].lower()}-stock-prediction/data/0.*', tl[j])[0]
                    walletinvestorv = text[text.find(start:='>')+len(start):text.find(' USD')]
                except Exception as e:
                    #print(f'WalletInvestor Combine: {e}')
                    walletinvestorv = '-'
            else:
                pass
        ld.append({'ticker':tickerlist[i], 'cnnf': cnnv, 'ytp': ytprice , 'mbv': mbv, 'yadv1': yadv1, 'yadv2': yadv2, 'evtoebitda': evtoebitda, 'wif': walletinvestorv})
        #print({'ticker':tickerlist[i], 'cnnf': cnnv, 'ytp': ytprice , 'mbv': mbv, 'yadv1': yadv1, 'yadv2': yadv2, 'evtoebitda': evtoebitda, 'wif':walletinvestorv})
        print(f'Combine {i+1} {tickerlist[i]}: {time() - tim}')
        if (i+1) % 500 == 0:
            import gc
            collected = gc.collect()
            print(f'Garbage collector: collected {collected} objects')
    return ld

#combinesinglecalling(['AAPL', 'TSLA', 'AGMX', 'AGMH'])

now = datetime.now()
yesterday = (now - timedelta(days = 1)).strftime('%b%d')
tomorrow = (now + timedelta(days = 1)).strftime('%b%d')

flag = 1
while flag:
    pytonProcess = subprocess.check_output("ps -ef | grep .py",shell=True).decode()
    pytonProcess = pytonProcess.split('\n')

    for process in pytonProcess:
        if yesterday in process and 'python3 /home/spreadsheet1981/mypython/stockscore.py' in process:
            #print(process)
            flag = 1
            sleep(60 * 15)
            break
        else:
            flag = 0
    if flag == 0:
        break

#http = urllib3.PoolManager(num_pools=60, cert_reqs='CERT_NONE')

key = '1asdfghsdfghjkkjfghjk' # Google sheet key 
keyv = '2345ujhgfdghjklxcvbnm' # Google sheet key

def googsheet(key=key,keyv=keyv):
    while True:
        try:
            gc = gspread.service_account(filename = '/home/spreadsheet1981/mypython/gkeys.json')
            wb = gc.open_by_key(key)
            wbv = gc.open_by_key(keyv)
            return wb, wbv
            break
        except Exception as e:
            sleep(15)
            print(e)
            continue


def read_tickerlist(wbk,worksheet_name,nth_column, slice_start):
    while True:
        wb, wbv = googsheet()
        try:
            if wbk == 1:
                tickerlist = wb.worksheet(worksheet_name).col_values(nth_column)
            else:
                tickerlist = wbv.worksheet(worksheet_name).col_values(nth_column)
            tickerlist = tickerlist[slice_start:]
            #tickerlist = ['s','FB','AACP','asdfghjkl']
            #tickerlist = ['symbol','FB', "AAPL", "GOOG", "GOOGL","TSLA"]
            #tickerlist = ['symbol', 'TSLA']
            del wb, wbv
            import gc
            gc.collect()
            return tickerlist
            break
        except Exception as e:
           sleep(15)
           print(e)
           continue

http = urllib3.PoolManager(num_pools=60, cert_reqs='CERT_NONE')
def score(tickerlist=read_tickerlist(2, 'All Tickers', 1, 1)):
    wb, wbv = googsheet()
   # http = urllib3.PoolManager(num_pools=60, cert_reqs='CERT_NONE')
    tickerscorelist = []
    csv_data = []
    fin_data = []
    stat_data = []

    while True:
        try:
            with ThreadPoolExecutor(max_workers=5) as executor:
                executor.submit(wb.worksheet('Dashboard').update, 'A4', [['=Data_IEX!A2']], value_input_option='USER_ENTERED')
                executor.submit(wb.worksheet('Data').batch_clear, ["A12:L"])
                break
        except Exception as e:
            print(e)
            sleep(15)
            continue
    while True:
        try:
            with ThreadPoolExecutor(max_workers=5) as executor:
                executor.submit(wb.worksheet('Data').update, "A12", [['=ARRAYFORMULA(Data_IEX!A352:L)']], value_input_option='USER_ENTERED')
            break
        except Exception as e:
            print(e)
            sleep(15)
            continue

    count = 0
    tickerlen = len(tickerlist)
    while count * 20 < tickerlen:
        with ThreadPoolExecutor(max_workers=12) as executor:
            for ticker in tickerlist[count * 20 : (count + 1) * 20]:
                s = time()
                tick = re.sub(r'\.', '-', ticker)
                csv_data.append(executor.submit(cv, tick))
                fin_data.append(executor.submit(fin, tick))
                stat_data.append(executor.submit(stat, tick))

        csv_data = [ list(csv.reader(i.result().data.decode('utf-8').splitlines(), delimiter=',')) for i in csv_data]
        fin_data = [i.result() for i in fin_data]
        stat_data =  [i.result() for i in stat_data]
        tab_data = [[{'range':'A1',
                    'values': csv_data[i]},
                    {'range':'A352',
                    'values': fin_data[i]},
                    {'range':'A381',
                    'values': stat_data[i][0]},
                    {'range':'A415',
                    'values': stat_data[i][1]},
                    {'range':'A445',
                    'values': stat_data[i][2]}]
                    for i in range(0, len(csv_data))]

        print(time() - s)

        s = time()
        for i in range(0, len(tickerlist[count * 20 : (count + 1) * 20])):
            print(f'Working Score {count * 20 + i + 1}: {tickerlist[count * 20 + i]}')
            score = None
            while True:
                try:
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        #executor.submit(wb.worksheet('Data').batch_clear, ["A12:L"])
                        executor.submit(wb.worksheet('Data_IEX').clear)
                    break
                except Exception as e:
                    print(e)
                    sleep(15)
                    continue

            while True:
                try:
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        executor.submit(wb.worksheet('Data_IEX').batch_update, tab_data[i], value_input_option='USER_ENTERED')
                    break
                except Exception as e:
                    print(e)
                    sleep(15)
                    continue

            while True:
                try:
                    score = wb.worksheet('Dashboard').acell('A14').value
                    break
                except Exception as e:
                    print(e)
                    sleep(15)
                    continue
            tickerscorelist.append(score)
            if time() - s > 98:
                try:
                    sleep(100 - time() - s)
                    s = time()
                except:
                    s =  time()
        count =  count + 1
        csv_data = []
        fin_data = []
        stat_data = []

    del count, csv_data, fin_data, stat_data, tab_data
    import gc
    gc.collect()

    while True:
        try:
            wbv.worksheet('Score').batch_clear(["B3:C"])
            break
        except Exception as e:
            print(f'Clear Score Sheet: {e}')
            sleep(15)
            continue

    while True:
        try:
            wbv.worksheet('Score').update('B3',[[tickerlist[i], tickerscorelist[i]] for i in range(0, len( tickerlist))], value_input_option='USER_ENTERED')
            break
        except Exception as e:
            print(f'Update Ticker Score: {e}')
            sleep(15)
            continue

    now = datetime.now()
    d1 = now.strftime("%Y-%m-%d %H:%M:%S")

    while True:
        try:
            wbv.worksheet('Score').update('E1', [['Last Updated (UTC)' ,d1]], value_input_option='USER_ENTERED')
            break
        except Exception as e:
            print(e)
            sleep(15)
            continue

    while True:
        try:
            with ThreadPoolExecutor(max_workers=5) as executor:
                executor.submit(wb.worksheet('Data').batch_clear, ["A12:L"])
                executor.submit(wb.worksheet('Data_IEX').clear)
            break
        except Exception as e:
            print(e)
            sleep(15)
            continue

    while True:
        try:
            wb.worksheet('Dashboard').update('A4', [['AAPL']], value_input_option='USER_ENTERED')
            break
        except Exception as e:
            print(e)
            sleep(15)
            continue

    while True:
        try:
            with ThreadPoolExecutor(max_workers=5) as executor:
                executor.submit(wb.worksheet('Data').batch_update, [{'range':'A12',
                                                                        'values': [['=Importhtml("https://www.valuespreadsheet.com/app/morningstar.php?ticker="&F7&":"&E7&"&page=financials","table",1)']],
                                                                        },
                                                                    {'range':'A41',
                                                                        'values': [['=Importhtml("https://www.valuespreadsheet.com/app/morningstar.php?ticker="&F7&":"&E7&"&page=stats","table",2)']],
                                                                        },
                                                                    {'range':'A75',
                                                                        'values': [['=Importhtml("https://www.valuespreadsheet.com/app/morningstar.php?ticker="&F7&":"&E7&"&page=stats","table",4)']],
                                                                        },
                                                                    {'range':'A105',
                                                                        'values': [['=Importhtml("https://www.valuespreadsheet.com/app/morningstar.php?ticker="&F7&":"&E7&"&page=stats","table",6)']],
                                                                        },
                                                                    ],
                                value_input_option='USER_ENTERED')
                executor.submit(wb.worksheet('Data_IEX').update, 'A1', '=IMPORTDATA("https://valuescout.co/spreadsheet/"&Dashboard!A4&"?email="&Dashboard!G3&"&v="&Data!B5)', value_input_option='USER_ENTERED')
            break
        except Exception as e:
            print(e)
            sleep(15)
            continue

    del tickerscorelist

def part1():
    wb, wbv = googsheet()
    tickerlist = read_tickerlist(2,'All Tickers', 1, 1)
    finv = finviz(tickerlist)
    tv = tradingview(tickerlist)

    while True:
        try:
            wb.worksheet('All in One').batch_clear(["A3:A", "AI3:AI", "Y3:Y", "K3:K", "AA3:AA", "C3:E", "H3:H", "M3:R", "U3:V", "E1"])
            break
        except Exception as e:
            print(f'All in One Sheet clear 1: {e}')
            sleep(15)
            continue

    while True:
        try:
            wb.worksheet('All in One').batch_update([
                                                    {'range':'A3','values': [[i] for i in tickerlist]},
                                                    {'range':'AI3','values': [[i[0]] for i in tv]},
                                                    #{'range':'W3','values': [[i[1]] for i in tv]},
                                                    #{'range':'W3','values': [[0 if i[1] == None else i[1]] for i in tv]},
                                                    {'range':'Y3','values': [[i] for i in tipranks(tickerlist)]},
                                                    {'range':'K3','values': [[i] for i in zacks(tickerlist)]},
                                                    {'range':'AA3','values': [[i] for i in wallstreetzenscore(tickerlist)]},
                                                    {'range':'C3','values': [[i['P/E']] for i in finv]},
                                                    {'range':'D3','values': [[i['Price']] for i in finv]},
                                                    {'range':'E3','values': [[i['PEG']] for i in finv]},
                                                    {'range':'H3','values': [[i['RSI']] for i in finv]},
                                                    {'range':'M3','values': [[i['EPS']] for i in finv]},
                                                    {'range':'N3','values': [[i['Fwd P/E']] for i in finv]},
                                                    {'range':'O3','values': [[i['Inst Trans']] for i in finv]},
                                                    {'range':'P3','values': [[i['Insider Trans']] for i in finv]},
                                                    {'range':'Q3','values': [[i['Sales past 5Y']] for i in finv]},
                                                    {'range':'R3','values': [[i['EPS past 5Y']] for i in finv]},
                                                    {'range':'U3','values': [[i['ROA']] for i in finv]},
                                                    {'range':'V3','values': [[i['EPS next 5Y']] for i in finv]},
                                                    {'range':'D1','values': [['Completed Part 1', datetime.now().strftime("%Y-%m-%d %H:%M:%S")]]}
                                                    ],
                                                    value_input_option='USER_ENTERED')
            break
        except Exception as e:
            print(f'Upload to Sheet Part 1: {e}')
            sleep(15)
            continue

    del finv, tv, wb, wbv


def part2():
    wb, wbv = googsheet()
    tickerlist = read_tickerlist(1,'All in One', 1, 2)
    combine = combinesinglecalling(tickerlist)

    while True:
        try:
            wb.worksheet('All in One').batch_clear(["I3:I", "AJ3:AJ", "L3:L", "S3:T", "W3:W", "Z3:Z", "H1"])
            break
        except Exception as e:
            print(f'All in One Sheet clear 2: {e}')
            sleep(15)
            continue


    while True:
        try:
            wb.worksheet('All in One').batch_update([
                                                    {'range':'I3','values': [[i['cnnf']] for i in combine]},
                                                    {'range':'AJ3','values': [[i['ytp']] for i in combine]},
                                                    {'range':'L3','values': [[i['mbv']] for i in combine]},
                                                    {'range':'S3','values': [[i['yadv1']] for i in combine]},
                                                    {'range':'T3','values': [[i['yadv2']] for i in combine]},
                                                    {'range':'W3','values': [[i['evtoebitda']] for i in combine]},
                                                    {'range':'Z3','values': [[i['wif']] for i in combine]},
                                                    {'range':'G1','values': [['Completed Part 2', datetime.now().strftime("%Y-%m-%d %H:%M:%S")]]}
                                                    ],
                                                    value_input_option='USER_ENTERED')
            break
        except Exception as e:
            print(f'Upload to Sheet Part 1: {e}')
            sleep(15)
            continue

    del combine




def part3():
    wb, wbv = googsheet()
    while True:
        try:
            wb.worksheet('All in One').batch_clear(["BK3:BK", "B3:B"])
            break
        except Exception as e:
            print(f'All in One Sheet clear 3: {e}')
            sleep(15)
            continue

    while True:
        try:
            sleep(60)
            val = [[sum(i)/66]for i in wb.worksheet('All in One').get("AK3:BI", value_render_option = 'UNFORMATTED_VALUE')]
            wb.worksheet('All in One').batch_update([
                                                    {'range':'BK3','values': val},
                                                    {'range':'B3','values': val},
                                                    ],
                                                    value_input_option='USER_ENTERED')
            del val
            break
        except Exception as e:
            print(f'Value for Sum: {e}')
            sleep(15)
            continue



def copypaste():
    wb, wbv = googsheet()
    while True:
        try:
            wbv.worksheet('Valuation All in One').batch_clear(["B3:BL"])
            break
        except Exception as e:
            print(f'Clear Valuation All in One Sheet: {e}')
            sleep(15)
            continue


    while True:
        try:
            val = wb.worksheet('All in One').get_all_values()

            wbv.worksheet('Valuation All in One').update('B1', val, value_input_option='USER_ENTERED')
            wbv.worksheet('Watchlist').update('B3',[i for i in  val[2:] if float(i[-1]) >= 0.5], value_input_option='USER_ENTERED')
            del val
            break
        except Exception as e:
            print(f'Upload to Sheet Valuation All in One: {e}')
            sleep(15)
            continue


now = datetime.now()
d1 = now.strftime("%Y-%m-%d %H:%M:%S")
wb, wbv = googsheet()
while True:
    try:
        with ThreadPoolExecutor(max_workers=5) as executor:
            executor.submit(wbv.worksheet('All Tickers').update, 'E1', [['Update Started (UTC)',d1, 'Update Status',  'Updating...']], value_input_option='USER_ENTERED')
        break
    except Exception as e:
        print(e)
        sleep(15)
        continue
del wb, wbv
libc = ctypes.CDLL("libc.so.6")
libc.malloc_trim(0)

http = urllib3.PoolManager(num_pools=60, cert_reqs='CERT_NONE')
part = part1()
import gc
del part, http
gc.collect()
libc.malloc_trim(0)

http = urllib3.PoolManager(num_pools=60, cert_reqs='CERT_NONE')
part = part2()
del part, http
import gc
gc.collect()
libc.malloc_trim(0)

part = part3()
del part
import gc
gc.collect()

copypaste = copypaste()
del copypaste
import gc
gc.collect()
libc.malloc_trim(0)

http = urllib3.PoolManager(num_pools=60, cert_reqs='CERT_NONE')
score = score()
del score, http
import gc
gc.collect()
libc.malloc_trim(0)

wb, wbv = googsheet()
while True:
    try:
        wbv.worksheet('All Tickers').update('I1', [['Update Completed']], value_input_option='USER_ENTERED')
        break
    except Exception as e:
        print(e)
        sleep(15)
        continue

pytonProcess = subprocess.check_output("ps -ef | grep .py",shell=True).decode()
pytonProcess = pytonProcess.split('\n')

for process in pytonProcess:
    if tomorrow  in process and 'python3 /home/spreadsheet1981/mypython/stockscore.py' in process:
        #print(process)
        exit()

if now + timedelta(minutes = 25, hours = 23) > datetime.now() or datetime.today().weekday() >= 5:
    subprocess.call(["sudo", "shutdown"])

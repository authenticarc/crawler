import pandas as pd
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from concurrent import futures
# from multiprocessing import Queue as mq
from queue import Queue
from pyhive import hive
from datetime import datetime
import cloudscraper
import time

## Open Hive connection
conn = hive.connect(host = 'dolphin.go.akamai-access.com', port = 10000)
cursor = conn.cursor()
# res  = mq()

## Get addresses
sql = '''select address from dws.user_assets where chain = 'eth' group by address'''
cursor.execute(sql)
addresses = [i[0] for i in cursor.fetchall()]
conn.close()


def crawler2hive(hive_table_name, cursor, df):
    # 若不存在则创建表
            
    cols = ''.join([i + ' string,' for i in df.columns]).rstrip(',')
            
    creator = '''create table if not exists tmp.{}(\n{})\nROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'\nSTORED AS textfile
    '''.format(hive_table_name, cols)
    cursor.execute(creator)
    cursor.fetch_logs()
    
    sql = '''insert into table tmp.{} values\n'''.format(hive_table_name)
    for _, rows in df.iterrows():
        # print(rows.values)
        sql += "({}),".format(','.join(['"' + i.__str__() + '"' for i in rows.values]))

    sql = sql.rstrip(",")
    
    try:
        cursor.execute(sql)
        cursor.fetch_logs()
    except:
        pass

class NansenCrawler:
    def __init__(self, address):
        
        self.__address = address
        self.__chain = [
            "thor", "okc", "moonbeam", "moonriver", "eth", "fantom",
            "dfk","matic2","evmos-evm","harmony","bsc","boba","canto",
            "metis","dogechain","astar","avax","aurora","optimism","arbitrum"
        ]
        self.__link = 'https://api-dev.nansen.ai/portfolio/wallet/{}/{}'
        self.__headers = {
            'ape-secret': 'U2FsdGVkX18qemvFd2Wclw6QF3iDXpMfqzSrorLaIVU/9sQ11a1r4PIV2ktDDU89YSyVCh6wzwlDEjWUoDVpG7xVau1pXqjac58ehqNfkOiLxOIv8aVZ1N0zx1ONW8alaLFybopfh/TsqyF8uQrj1g==',
            'passcode': 'A63uGa8775Ne89wwqADwKYGeyceXAxmHL'
        }
        self.__q = Queue()
        self.res_q = Queue()
    
    def make_request(self, chain):
        scraper = cloudscraper.create_scraper(browser={
            'browser': 'chrome',
            'platform': 'darwin',
            'mobile': False
        })
        time.sleep(5)
        
        try:
            resp = scraper.get(self.__link.format(chain, self.__address), headers=self.__headers)
            if resp.status_code != 200:
                print(resp.status_code, chain)
                return [resp.status_code, chain]
            else:
                print(chain, '\t', self.__address, 'success')
                return [resp.status_code, chain, resp.json()]
        except:
            return [0, chain]
    
    def extract_info(self, x):
        ret = x.result()
        if ret[0] == 200:
            if len(ret[2]) != 0:
                # print([ret[1], self.__address, resp.json()[0]])
                for i in ret[2]:
                    self.res_q.put([
                        ret[1], # chain
                        self.__address, # address 
                        i['address'], # contract    
                        i['symbol'], # symbol
                        i['price'], # price
                        i['createdAt'], # createdAt
                        i['modifiedAt'],  # modifiedAt
                        i['balance'], # balance
                        datetime.now().today().strftime('%Y-%m-%d') # time
                    ])
        else:
            time.sleep(3)
            self.__q.put(ret[1])
        
    def __call__(self):
        executor = ThreadPoolExecutor(len(self.__chain))
        tasks = []
        for chain in self.__chain:
            exe = executor.submit(self.make_request, chain)
            exe.add_done_callback(self.extract_info)
            tasks.append(exe)
            
        while self.__q.empty() == False:
            exe = executor.submit(self.make_request, self.__q.get())
            exe.add_done_callback(self.extract_info)
            tasks.append(exe)
        
        for future in futures.as_completed(tasks): 
            future.result() 
            
        df = []
        
        if self.res_q.empty() != True:
            while self.res_q.empty() == False:
                df.append(self.res_q.get())
                df = pd.DataFrame(df, columns = [
                    'chain', 'address', 'contract', 'symbol', 
                    'price', 'createdAt', 'modifiedAt', 'balance', 'crawl_time'])

            conn = hive.connect(host = 'dolphin.go.akamai-access.com', port = 10000)
            cursor = conn.cursor()
            crawler2hive('nansen_assets', cursor, df)
            conn.close()
        else:
            pass
        
        return 'done'
    
def wrapper(address):
    nc = NansenCrawler(address)
    ret = nc()
    return ret

if __name__ == '__main__':
    df = []
    
    with ProcessPoolExecutor() as future:
        tasks = []
        for address in addresses:
            tasks.append(future.submit(wrapper, address))

        for future in as_completed(tasks):
            data = future.result()

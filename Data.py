from pybit import usdt_perpetual
from datetime import datetime , timedelta
from queue import Queue
import pandas as pd
import os

class Collector():
    
    def __init__(self):
        
        self.url = "https://api.bybit.com"
        self.api = usdt_perpetual.HTTP(endpoint=self.url)
        self.dataFolder = "Data"
        self.filename = "dataFile"
        self.request_limit = 50
        self.day = 10

    def request(self, frame, from_time, limit=50) :
        response = self.api.query_kline(symbol="BTCUSDT",interval=frame
                                        ,from_time=from_time,limit=limit)
        if response["ret_msg"] == "OK" and response['result'] :
            return response["result"]
        else:
            print(f'no more response : {response["result"]}')
            return 0

    def getNewData(self , interval):
        
        if not os.path.isdir(self.dataFolder):
            
            os.mkdir(self.dataFolder)
        
        if not os.path.isfile(f"{self.dataFolder}/{self.filename}{interval}"):
            print("File not exists . creating ... ")
            timestamp = round(datetime.timestamp(datetime.now()-timedelta(self.day)))
            dataframe = pd.DataFrame()

            while True:
                tmp = self.request(interval , timestamp)
                if tmp == 0 :
                    break 
                df = pd.DataFrame.from_dict(tmp)
                print(tmp[0]['open_time'],tmp[-1]['open_time'])
                dataframe = pd.concat([dataframe,df ])
                timestamp = tmp[-1]['open_time']+1
            dataframe.rename(columns={'open_time':'time','open':'Open','high':'High','low':'Low','close':'Close'},inplace=True)
            dataframe.sort_values('time',inplace=True)
            dataframe.reset_index(inplace=True)
            dataframe.drop(columns=['index','symbol','interval','volume','turnover','period','id','start_at'],inplace=True)
            dataframe.drop(index=dataframe.index[-1],inplace=True,axis=1)
            dataframe.to_csv(f"{self.dataFolder}/{self.filename}{interval}", index=False , line_terminator='\n')
            print(f"created new file : {self.dataFolder}/{self.filename}{interval}")
        else :
            
            with open(f"{self.dataFolder}/{self.filename}{interval}", 'r') as file:
            
                timestamp = int(file.readlines()[-1].split(',')[0])+1
                
            df = pd.DataFrame()
            
            while True:

                data = self.request(interval , timestamp)

                if data !=0 :
                    
                    timestamp = data[-1]['open_time']+1
                    tmp = pd.DataFrame.from_dict(data)
                    df = pd.concat([df,tmp])
                    
                else:
                    break
                
            if len(df.index) > 0 :
                df.rename(columns={'open_time':'time','open':'Open','high':'High','low':'Low','close':'Close'},inplace=True)
                df.sort_values('time',inplace=True)
                df.reset_index(inplace=True)
                df.drop(columns=['index','symbol','interval','volume','turnover','period','id','start_at'],inplace=True)
                df.drop(index=df.index[-1],inplace=True,axis=1)
                length = len(df.index)
                with open(f"{self.dataFolder}/{self.filename}{interval}" , 'a') as file:
                    df.to_csv(file , header=False , index=False ,line_terminator='\n')   
            print(f"{length} new data added to file :{self.dataFolder}/{self.filename}{interval}")
            
            
if __name__ == '__main__':    
    a = Collector()
    a.getNewData(5)

import pandas as pd
from datetime import datetime
class Ssl():
    
    def __init__(self):
        self.dataFolder = "Data"
        self.filename = "dataFile"
        self.lastkandel = None

    
    def apply(self, interval , maH , maL):
        df = pd.read_csv(f"{self.dataFolder}/{self.filename}{interval}")
        df['smaHigh']=df['High'].rolling(maH).mean()
        df['smaLow']=df['Low'].rolling(maL).mean()
        df['sig'] = df.apply(lambda row : self.condition(row) , axis=1)
        # df['time'] = [datetime.fromtimestamp(x) for x in df['time']]
        df.dropna(inplace=True)
        # df.set_index('time',inplace=True)
        return df
        
    def condition(self, row):
        if row['Close'] > row['smaHigh'] :
            self.lastkandel = 1
            return 1 
        elif row['Close'] < row['smaLow'] :
            self.lastkandel = -1
            return -1
        else:
            return self.lastkandel
if __name__ == '__main__':
    ssl = Ssl()
    a = ssl.apply(30, 50, 50)
    print(a)
    
    
    
import ssl_channel , Data
from datetime import datetime , timedelta
import bot.bot as bt
import time
from time import sleep

dataFolder = "Data"
filename = "dataFile"
interval = 5
filepath = f"{dataFolder}/{filename}{interval}"

data = Data.Collector()
data.getNewData(interval)
ssl = ssl_channel.Ssl()

with open(filepath, 'r') as file:
            
     lts = int(file.readlines()[-1].split(',')[0])
     
next_ts = datetime.timestamp(datetime.fromtimestamp(lts) + timedelta(minutes=interval*2 , seconds=3))

bt.run()

while True :
    if time.time() >= next_ts :

        print(f"{datetime.now()}: new {interval} min kandel ({time.time()}) {next_ts}")
        bt.send(f"{datetime.now()}: new {interval} min kandel ({time.time()}) {next_ts}")
        
        data.getNewData(interval)
        df = ssl.apply(interval, 2,2)
        lts = int(df['time'][df.index[-1]])
        next_ts = datetime.timestamp(datetime.fromtimestamp(lts) + timedelta(minutes=interval*2 , seconds=10))
        print(f'next_ts {next_ts}')
        if df['sig'][df.index[-1]] != df['sig'][df.index[-2]]:
            
            bt.send("ssl 2 2 \n 5 min kandel")
            print("ssl 2 2 \n 5 min kandel")
    else:
        bt.send('waiting')
        print("waiting")
        sleep(60)

        
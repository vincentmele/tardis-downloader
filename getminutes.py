import sys
import os
import yapic.json as json
import requests
from datetime import datetime, timedelta

from joblib import Parallel, delayed
from multiprocessing import Process, Manager, JoinableQueue, freeze_support
from config import Config

c = Config(config="config.yaml")

parallel = int(c.parallel)
start_date = datetime.strptime(str(c.start_date), "%Y-%m-%d")
start_offset = int(c.start_offset)
end_date = datetime.strptime(str(c.end_date), "%Y-%m-%d")
outputdir = str(c.output_dir)
exchange = str(c.exchange)
channel = str(c.channel)
symbol = str(c.symbol)
auth_key = str(c.deribit.key)



if not os.path.exists(outputdir):
    os.makedirs(outputdir)

def saver(q, filename):
    with open(os.path.join(outputdir, filename), 'w') as out:
        while True:
            val = q.get()
            if val is None: break
            out.write(val)
        q.task_done()
        # Finish up
        q.task_done()


def addID(message):
    if exchange == "deribit":
        if channel == "book":
            tp = str(message['params']['data']['type'][0])
            id = str(message['params']['data']['change_id']) + tp.encode().hex()
        elif channel == "trades":
            id = str(message['params']['data'][0]['timestamp'])
    message['_id'] = id
    return message


def get_data_feeds(date_str, offset, exchange, auth_key):
    filters = [
#        {"channel": "trades", "symbols": ["BTC-PERPETUAL", "ETH-PERPETUAL"]}
        {"channel": channel, "symbols": [symbol]}
    ]
    qs_params = {"from": date_str, "offset": offset, "filters": json.dumps(filters)}

    headers = {"Authorization": auth_key}

    url = f"https://api.tardis.dev/v1/data-feeds/{exchange}"

    response = requests.get(url, headers=headers, params=qs_params, stream=True)

    lines = []
    for line in response.iter_lines():
        if len(line) <= 1:
          continue
        try:
            parts = line.decode("utf-8").split(" ")
            message = json.loads(parts[1])
            message = addID(message)
            lines.append(json.dumps(message) + "\n")
        except:
            continue    
    q.put(''.join(lines))
    print(f"wrote {output_filename}_{offset}  {round(offset/1440,2)}")
    
#   for line in response.iter_lines():
#        # empty lines in response are being used as markers
#        # for disconnect events that occurred when collecting the data
#        if len(line) <= 1:
#           continue
#
#        parts = line.decode("utf-8").split(" ")
#        local_timestamp = parts[0]
#        message = json.loads(parts[1])
#        # local_timestamp string marks message arrival timestamp
#        # message is a message dict as provided by exchange real-time stream
#        print(local_timestamp, message)

if __name__ == '__main__':
    freeze_support()

    date_generated = [start_date + timedelta(days=x) for x in range(0, (end_date-start_date).days)]
    for date in date_generated:
        m = Manager()
        q = m.Queue()
        output_filename = channel + "-" + symbol + "-" + date.strftime("%Y-%m-%d") + ".txt"
        p = Process(target=saver, args=(q, output_filename))
        p.start()
        Parallel(n_jobs=parallel)(delayed(get_data_feeds)(date, i, exchange, auth_key) for i in range (start_offset,1441))
        q.put(None) # Poison pill
        p.join()

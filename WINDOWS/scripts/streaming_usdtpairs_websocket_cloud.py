import websocket
import json
import pandas as pd

path = '/home/tinmarian96/tradingbot/'

stream = "wss://stream.binance.com:9443/ws/!miniTicker@arr"

def on_message(ws, message):
    msg = json.loads(message)
    symbol = [x for x in msg if x['s'].endswith('USDT')]
    df = pd.DataFrame(symbol)[['E','s','c']]
    df.E = pd.to_datetime(df.E, unit='ms').astype(str)
    df.c = df.c.astype(float)
    for row in range(len(df)):
        data = df[row:row+1]
        data.to_csv(
            path_or_buf = path+data['s'].values[0],
            mode='a',
            index=False,
            header=False
        )
        print(data)

ws = websocket.WebSocketApp(stream, on_message=on_message)
ws.run_forever()
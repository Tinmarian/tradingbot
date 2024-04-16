from unicorn_binance_websocket_api import BinanceWebSocketApiManager
import pandas as pd
import time
from datetime import datetime, date
import os


bws = BinanceWebSocketApiManager(exchange="binance.com")
bws.create_stream(['trade'], ['bnbusdt'], output = "UnicornFy")

i = 0
df = pd.DataFrame()
while True:

    oldest_data_from_stream_buffer = bws.pop_stream_data_from_stream_buffer()
    if oldest_data_from_stream_buffer:        
        df = pd.DataFrame([oldest_data_from_stream_buffer])
        if df.columns[0] == 'result':
            print(df.columns)
        else:
            df['Tiempo_evento'] = pd.to_datetime(df.event_time, unit='ms').item().strftime('%H:%M:%S.%f')
            df['Tiempo_trade'] = pd.to_datetime(df.trade_time, unit='ms').item().strftime('%H:%M:%S.%f')
            df = df.assign(Tiempo_actual=datetime.utcnow())
            df.Tiempo_actual = df.Tiempo_actual.item().strftime('%H:%M:%S.%f')
            df = df.assign(Año_actual = pd.to_datetime(df.Tiempo_actual).item().strftime('%y'))
            df = df.assign(Año_trade = pd.to_datetime(df.trade_time, unit='ms').item().strftime('%y'))
            df = df.assign(Mes_actual = pd.to_datetime(df.Tiempo_actual).item().strftime('%m'))
            df = df.assign(Mes_trade = pd.to_datetime(df.trade_time, unit='ms').item().strftime('%m'))
            df = df.assign(Dia_actual = pd.to_datetime(df.Tiempo_actual).item().strftime('%d'))
            df = df.assign(Dia_trade = pd.to_datetime(df.trade_time, unit='ms').item().strftime('%d'))
            df = df.assign(Hora_actual = pd.to_datetime(df.Tiempo_actual).item().strftime('%H'))
            df = df.assign(Hora_trade = pd.to_datetime(df.trade_time, unit='ms').item().strftime('%H'))
            df = df.assign(Minuto_Actual = pd.to_datetime(df.Tiempo_actual).item().strftime('%M'))
            df = df.assign(Minuto_trade = pd.to_datetime(df.trade_time, unit='ms').item().strftime('%M'))
            df = df.assign(Segundo_Actual = pd.to_datetime(df.Tiempo_actual).item().strftime('%S'))
            df = df.assign(Segundo_trade = pd.to_datetime(df.trade_time, unit='ms').item().strftime('%S'))
            df = df.assign(Milisegundo_Actual = pd.to_datetime(df.Tiempo_actual).item().strftime('%f'))
            df = df.assign(Milisegundo_trade = pd.to_datetime(df.trade_time, unit='ms').item().strftime('%f'))
            df = df.loc[:,[
                            'Año_actual', 'Año_trade', 'Mes_actual', 'Mes_trade', 'Dia_actual', 'Dia_trade',  'Hora_actual', 'Hora_trade', 
                            'Minuto_Actual', 'Minuto_trade', 'Segundo_Actual', 'Segundo_trade', 'Milisegundo_Actual', 'Milisegundo_trade', 
                            'Tiempo_evento', 'Tiempo_trade', 'Tiempo_actual', 'buyer_order_id', 'seller_order_id', 'trade_id',
                            'symbol', 'price', 'quantity', 'is_market_maker']]
            df.columns = [
                            'Anio_actual', 'Anio_trade', 'Mes_actual', 'Mes_trade', 'Dia_actual', 'Dia_trade',  'Hora_actual', 
                            'Hora_trade', 'Minuto_Actual', 'Minuto_trade', 'Segundo_Actual', 'Segundo_trade','Milisegundo_Actual', 
                            'Milisegundo_trade', "Tiempo_Evento", "Tiempo_Trade", "Tiempo_Actual", "ID_pedido_comprador", 
                            "ID_pedido_vendedor", "ID_trade", "Simbolo", "Precio", "Cantidad", 'is_market_maker'
                            ]
            df = df.assign(Total_USD = float(df.Precio)*float(df.Cantidad))
            mes = df['Mes_actual'][0]
            dia = df['Dia_actual'][0]
            hora = df['Hora_actual'][0]
            minuto = df['Minuto_Actual'][0]
            par = df.Simbolo[0]
            if i == 0:
                os.mkdir(f'/home/tinmarian96/tradingbot/csvs/mes_{mes}/dia_{dia}/hora_{hora}')
                df.to_csv(                                    
                    path_or_buf = f'/home/tinmarian96/tradingbot/csvs/mes_{mes}/dia_{dia}/hora_{hora}/{par}.csv',
                    index=False,
                    sep=',',
                    encoding='utf-8',
                    mode='a',
                    header=True
                    )
            else:
                if os.path.exists(f'/home/tinmarian96/tradingbot/csvs/mes_{mes}/dia_{dia}/hora_{hora}'):
                    df.to_csv(                                    
                    path_or_buf = f'/home/tinmarian96/tradingbot/csvs/mes_{mes}/dia_{dia}/hora_{hora}/{par}.csv',
                    index=False,
                    sep=',',
                    encoding='utf-8',
                    mode='a',
                    header=False
                    )
                else:
                    os.mkdir(f'/home/tinmarian96/tradingbot/csvs/mes_{mes}/dia_{dia}/hora_{hora}')
                    df.to_csv(                                    
                        path_or_buf = f'/home/tinmarian96/tradingbot/csvs/mes_{mes}/dia_{dia}/hora_{hora}/{par}.csv',
                        index=False,
                        sep=',',
                        encoding='utf-8',
                        mode='a',
                        header=True
                        )
            i = i + 1
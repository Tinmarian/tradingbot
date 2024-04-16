def MACD(df):
    df['EMA12'] = df.Precio.ewm(span=12).mean()
    df['EMA26'] = df.Precio.ewm(span=26).mean()
    df['MACD'] = df.EMA12 - df.EMA26
    df['Signal'] = df.MACD.ewm(span=9).mean()
    print('Indicadores agregados')

def señal_MACD(df):
    buy, sell = [],[]
    for i in range(7,len(df)):
        if df.MACD.iloc[i] > df.Signal.iloc[i] and df.MACD.iloc[i-1] < df.Signal.iloc[i-1]:
            buy.append(i)
        elif df.MACD.iloc[i] < df.Signal.iloc[i] and df.MACD.iloc[i-1] > df.Signal.iloc[i-1]:
            sell.append(i)
    return buy, sell


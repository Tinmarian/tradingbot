import pandas as pd
import os
from sqlalchemy import create_engine
import psycopg2

cripto_engine = create_engine('postgresql+psycopg2://postgres:GuruSat.3@34.29.240.149/criptos')
updown_engine = create_engine('postgresql+psycopg2://postgres:GuruSat.3@34.29.240.149/ups_downs')

path = '/home/tinmarian96/tradingbot/' 
pairs = os.listdir(path)
pairs = pd.DataFrame(pairs, columns=['pairs'])

downs = pairs[pairs['pairs'].str.contains('DOWN')]
ups = pairs[pairs['pairs'].str.contains('UP')]
ups_downs = pd.concat([ups, downs], ignore_index=True)
ups_downs = ups_downs[ups_downs['pairs'].str.contains('SUPERUSDT')==False]

for x in ups_downs['pairs']:
    pairs = pairs[pairs['pairs'].str.contains(x)==False]
    df = pd.read_csv(path+x,names=['Tiempo','Simbolo','Precio'])
    os.remove(path+x)
    df.to_sql(x,updown_engine,index=False)
for x in pairs['pairs']:
    df = pd.read_csv(path+x,names=['Tiempo','Simbolo','Precio'])
    os.remove(path+x)
    df.to_sql(x,cripto_engine,index=False)
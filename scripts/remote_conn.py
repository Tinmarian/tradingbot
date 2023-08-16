def connection():
    import os
    from google.cloud.sql.connector import Connector
    from sqlalchemy import create_engine, text
    import pg8000.native
    import pandas as pd

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/tinma/OneDrive/Escritorio/application_default_credentials.json'

    connector = Connector()

    def getconn() -> pg8000.native.Connection:
        conn: pg8000.native.Connection = connector.connect(
            "semiotic-pact-388619:us-central1:tradingbot",
            "pg8000",
            user="postgres",
            password="GuruSat.3",
            db="criptos"
        )
        return conn

    connection = create_engine('postgresql+pg8000://',creator=getconn)

    return connection
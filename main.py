from pandas_etl.pandas_spotify import Pandas_ETL

if __name__ == '__main__':
    etl = Pandas_ETL()
    print(etl.transform_load())
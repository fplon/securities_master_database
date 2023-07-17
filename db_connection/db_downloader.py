# import psycopg2 as pg
import pandas as pd
import logging
import datetime as dt

from config.db_config import CONN_CONF
from db_connection.db_connector import DatabaseConnector


logger = logging.getLogger(__name__)

class DatabaseDownloader(DatabaseConnector):

    def __init__(self) -> None: 
        super().__init__()
        self.update_date = dt.datetime.now().strftime('%Y-%m-%d')


    def get_db_table(self, table_name: str) -> pd.DataFrame: 

        return pd.read_sql_table(table_name, con=self.engine)
    
    
    def get_db_table_from_query(self, query: str, params: list | None = None) -> pd.DataFrame:

        return pd.read_sql_query(sql=query, params=params, con=self.engine)
    

    def get_db_exchanges(self) -> pd.DataFrame: 

        return self.get_db_table('exchange') 
    

    def get_db_fund_watchlist(self) -> pd.DataFrame:

        return self.get_db_table('fund_watchlist')
    

    def get_db_earnings_qtr(self) -> pd.DataFrame: 

        return self.get_db_table('earnings_qtr')
    

    def get_db_earnings_yr(self) -> pd.DataFrame: 

        return self.get_db_table('earnings_yr')
    

    def get_db_earnings_trend(self) -> pd.DataFrame: 

        return self.get_db_table('earnings_trend')
    

    def get_db_balance_sheet_qtr(self) -> pd.DataFrame: 

        return self.get_db_table('balance_sheet_qtr')
    

    def get_db_balance_sheet_yr(self) -> pd.DataFrame: 

        return self.get_db_table('balance_sheet_yr') 
    

    def get_db_income_statement_qtr(self) -> pd.DataFrame:

        return self.get_db_table('income_statement_qtr')
    

    def get_db_income_statement_yr(self) -> pd.DataFrame: 

        return self.get_db_table('income_statement_qtr')
    

    def get_db_cash_flow_qtr(self) -> pd.DataFrame: 

        return self.get_db_table('cash_flow_qtr')
    

    def get_db_cash_flow_yr(self) -> pd.DataFrame: 

        return self.get_db_table('cash_flow_yr')
    

    def get_db_fundamentals_snapshot(self) -> pd.DataFrame: 

        return self.get_db_table('fundamentals_snapshot')


    def get_db_indices(self) -> pd.DataFrame: 

        # db_indices_df.set_index('id', inplace = True) # TODO?
        return self.get_db_table('index')
    
    
    def map_db_index_id_from_index_code(self, index_ticker: str | list) -> str | list:

        if isinstance(index_ticker, str):
            index_ticker = [index_ticker]

        index_ticker =  ','.join([f'\'{ticker}\'' for ticker in index_ticker])
        query = f'SELECT id FROM index WHERE ticker IN ({index_ticker})'
        return self.get_db_table_from_query(query=query)
    
    # TODO: refactor this shite
    def get_db_constituents(self, index_id: int | list | None) -> pd.DataFrame: 

        table = 'index_constituents'
        if isinstance(index_id, int): 
            index_id = [index_id]

        if index_id is None: 
            return self.get_db_table(table)
        elif isinstance(index_id, list):
            index_id = ','.join([f'\'{id}\'' for id in index_id])
            query = f'SELECT * FROM {table} WHERE index_id IN ({index_id})'
            return self.get_db_table_from_query(query=query)

    def get_db_instruments(self, exchange_id: int | None = None) -> pd.DataFrame: 

        if exchange_id is None: 
            return self.get_db_table('instrument')
        else: 
            query = f'SELECT * FROM instrument WHERE exchange_id = {exchange_id}'
            return self.get_db_table_from_query(query=query)
        # db_instruments_df.set_index('id', inplace = True) # TODO? 


    def get_db_price(self, query_params: dict) -> pd.DataFrame:

        query = self._generate_price_sql_query(query_params)
        return self.get_db_table_from_query(query=query)
    
    
    def _generate_price_sql_query(self, query_params) -> str:

        instrument_id = query_params.get('instrument_id', None)
        price_date = query_params.get('price_date', None)
        include_ticker = query_params.get('include_ticker', False)

        select_query = 'SELECT * FROM daily_price'
        instrument_filter = f' WHERE instrument_id = {instrument_id}'
        date_filter = f' WHERE price_date = \'{price_date}\''
        ticker_join = ''

        if include_ticker: 
            ticker_join = ' LEFT JOIN instrument i ON (dp.instrument_id = i.id)'
            select_query = 'SELECT dp.*, i.ticker FROM daily_price dp'


        if (instrument_id is None) & (price_date is None): 
            query = f'{select_query}{ticker_join}'
        elif (instrument_id is not None) & (price_date is None):
            query = f'{select_query}{ticker_join}{instrument_filter}'
        elif (instrument_id is None) & (price_date is not None):
            query = f'{select_query}{ticker_join}{date_filter}'
        else:
            query = (
                f'{select_query}{ticker_join}{date_filter}'
                f'{instrument_filter.replace("WHERE", "AND")}'
            )

        return query 
    
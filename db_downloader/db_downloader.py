# import psycopg2 as pg
import pandas as pd
import logging
from sqlalchemy import create_engine

from config.db_config import CONN_CONF

logger = logging.getLogger(__name__)


class DatabaseConnector:

    def __init__(self, conn_config: dict = CONN_CONF) -> None: 
        self.database, self.user, self.password, self.host = conn_config.values()
        self.engine = self._init_engine()

    def __enter__(self): 
        return self

    def __exit__(self, type, value, traceback) -> None: 
        self._dispose_engine()
        self.engine = None

    def _init_engine(self) : 
        return create_engine(
            f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}/{self.database}'
            )
    
    def _dispose_engine(self):
        self.engine.dispose()


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
        return self.get_db_table('benchmark_index')


    def get_db_instruments(self, exchange_id: int | None = None) -> pd.DataFrame: 

        if exchange_id is None: 
            return self.get_db_table('instrument')
        else: 
            query = f'SELECT * FROM instrument WHERE exchange_id = {exchange_id}'
            return self.get_db_table_from_query(query=query)
        # db_instruments_df.set_index('id', inplace = True) # TODO? 

    def get_db_price(
        self, instrument_id: int | None = None, 
        price_date: str | None = None, 
        include_ticker: bool = False) -> pd.DataFrame:

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

        return self.get_db_table_from_query(query=query)

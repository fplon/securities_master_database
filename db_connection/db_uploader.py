import pandas as pd
import numpy as np
import datetime as dt
import logging

from db_connection.db_connector import DatabaseConnector
from utils.dates import get_prev_bd
from utils.strings import get_snake_case_from_camel_case


logger = logging.getLogger(__name__)

# TODO: add type validation before upload
# TODO: logic for created vs updated
# TODO: configure exchange and index watchlists so only storing select ones

class DatabaseUploader(DatabaseConnector): 

    def __init__(self) -> None: 
        super().__init__()
        self.update_date = dt.datetime.now().strftime('%Y-%m-%d')

    def update_db_exchanges(self, exchanges: pd.DataFrame) -> None:

        exchanges = (
            exchanges
            .rename(columns={'OperatingMIC': 'short_name'})
            .drop(columns=['CountryISO2', 'CountryISO3'])
            .assign(
                created_date = self.update_date,
                last_update_date = self.update_date, 
                last_price_update_date = np.nan, # TODO: remove - move col to another db table
                last_fundamental_update_date = np.nan # TODO: remove - as above
            )
        )
        exchanges.columns = exchanges.columns.str.lower()
        exchanges['name'] = exchanges['name'].str.replace('\'', '\'\'')

        exchanges.to_sql('exchange', self.engine, if_exists='append', index=False) 

    
    def update_db_instruments(self, instruments: pd.DataFrame, db_exchange_id: int) -> None: 
        
        if instruments.shape[0] == 0: 
            logger.warning(f'Dataframe is empty.')

        instruments = (
            instruments
            .rename(columns={'Code': 'ticker', 'Type': 'instrument_type'})
            .drop(columns=['Country', 'Exchange', 'Isin']) # TODO: review dropping these, eg. US
            .assign(
                exchange_id = db_exchange_id,
                created_date = self.update_date,
                last_update_date = self.update_date, 

            )
        )
        instruments.columns = instruments.columns.str.lower()
        for col in ['ticker', 'name']:
            instruments[col] = instruments[col].str.replace('\'', '\'\'')

        instruments.to_sql('instrument', self.engine, if_exists='append', index=False) 


    def update_db_indices(self, indices: pd.DataFrame) -> None: 
        
        if indices.shape[0] == 0: 
            logger.warning(f'Dataframe is empty.')

        indices = (
            indices
            .rename(columns={'Code': 'ticker'})
            .drop(columns=['Type', 'Isin', 'Exchange']) 
            .assign(
                created_date = self.update_date,
                last_update_date = self.update_date, 
            )
        )
        indices.columns = indices.columns.str.lower()
        for col in ['ticker', 'name']:
            indices[col] = indices[col].str.replace('\'', '\'\'')

        indices.to_sql('index', self.engine, if_exists='append', index=False) 


    def update_db_index_constituents(
            self, constituents: pd.DataFrame, db_index_id: int) -> None: 
        
        constituents = (
            constituents
            .rename(columns={'Code': 'ticker', 'Exchange': 'exchange_code'})
            .drop(columns=['Name', 'Sector', 'Industry']) # info already in instrument table
            .assign(
                index_id = db_index_id,
                index_date = get_prev_bd(self.update_date),
                created_date = self.update_date,
                last_update_date = self.update_date, 
            )
        )
        constituents.columns = constituents.columns.str.lower()
        
        constituents.to_sql('index_constituents', self.engine, if_exists='append', index=False)


    def update_db_prices(
            self, prices: pd.DataFrame, db_instrument_id: int, 
            data_vendor_id: int = 1) -> None: 
        
        prices = (
            prices
            .rename(columns={'date': 'price_date'})
            .assign(
                instrument_id = db_instrument_id,
                created_date = self.update_date,
                last_update_date = self.update_date, 
            )
        )
        prices.columns = prices.columns.str.lower()
        prices['price_date'] = prices['price_date'].astype(str)

        prices.to_sql('price', self.engine, if_exists='append', index=False)


    def update_earnings_qtr(
            self, earnings_qtr: pd.DataFrame, db_instrument_id: int) -> None: 
        
        earnings_qtr = (
            earnings_qtr
            .rename(columns={'date': 'interval_end_date'})
            .assign(
                instrument_id = db_instrument_id,
                created_date = self.update_date,
                last_update_date = self.update_date,
            )
        )
        earnings_qtr.columns = earnings_qtr.columns.map(get_snake_case_from_camel_case)

        earnings_qtr.to_sql('earnings_qtr', self.engine, if_exists='append', index=False)


    def update_earnings_yr(
            self, earnings_yr: pd.DataFrame, db_instrument_id: int) -> None: 
        
        earnings_yr = (
            earnings_yr
            .rename(columns={'date': 'interval_end_date'})
            .assign(
                instrument_id = db_instrument_id,
                created_date = self.update_date,
                last_update_date = self.update_date,
            )
        )
        earnings_yr.columns = earnings_yr.columns.map(get_snake_case_from_camel_case)

        earnings_yr.to_sql('earnings_yr', self.engine, if_exists='append', index=False)


    def update_earnings_trend(
            self, earnings_trend: pd.DataFrame, db_instrument_id: int) -> None: 
        
        earnings_trend = (
            earnings_trend
            .rename(columns={'date': 'interval_end_date'})
            .assign(
                instrument_id = db_instrument_id,
                created_date = self.update_date,
                last_update_date = self.update_date,
            )
        )
        earnings_trend.columns = earnings_trend.columns.map(get_snake_case_from_camel_case)
        
        earnings_trend.to_sql('earnings_trend', self.engine, if_exists='append', index=False)


    def update_balance_sheet_qtr(
            self, balance_sheet_qtr: pd.DataFrame, db_instrument_id: int) -> None: 
        
        balance_sheet_qtr = (
            balance_sheet_qtr
            .rename(columns={'date': 'interval_end_date'})
            .assign(
                instrument_id = db_instrument_id,
                created_date = self.update_date,
                last_update_date = self.update_date,
            )
        )
        balance_sheet_qtr.columns = (
            balance_sheet_qtr
            .columns
            .map(get_snake_case_from_camel_case)
        )
        
        balance_sheet_qtr.to_sql(
            'balance_sheet_qtr', self.engine, if_exists='append', index=False
            )


    def update_balance_sheet_yr(
            self, balance_sheet_yr: pd.DataFrame, db_instrument_id: int) -> None: 
        
        balance_sheet_yr = (
            balance_sheet_yr
            .rename(columns={'date': 'interval_end_date'})
            .assign(
                instrument_id = db_instrument_id,
                created_date = self.update_date,
                last_update_date = self.update_date,
            )
        )
        balance_sheet_yr.columns = (
            balance_sheet_yr
            .columns
            .map(get_snake_case_from_camel_case)
        )
        
        balance_sheet_yr.to_sql(
            'balance_sheet_yr', self.engine, if_exists='append', index=False
            )

    def update_income_statement_qtr( 
            self, income_statement_qtr: pd.DataFrame, db_instrument_id: int) -> None: 
        
        income_statement_qtr = (
            income_statement_qtr
            .rename(columns={'date': 'interval_end_date'})
            .assign(
                instrument_id = db_instrument_id,
                created_date = self.update_date,
                last_update_date = self.update_date,
            )
        )
        income_statement_qtr.columns = (
            income_statement_qtr
            .columns
            .map(get_snake_case_from_camel_case)
        )
        
        income_statement_qtr.to_sql(
            'income_statement_qtr', self.engine, if_exists='append', index=False
            )

    def update_income_statement_yr( 
            self, income_statement_yr: pd.DataFrame, db_instrument_id: int) -> None: 
        
        income_statement_yr = (
            income_statement_yr
            .rename(columns={'date': 'interval_end_date'})
            .assign(
                instrument_id = db_instrument_id,
                created_date = self.update_date,
                last_update_date = self.update_date,
            )
        )
        income_statement_yr.columns = (
            income_statement_yr
            .columns
            .map(get_snake_case_from_camel_case)
        )
        
        income_statement_yr.to_sql(
            'income_statement_yr', self.engine, if_exists='append', index=False
            )
        

    def update_cash_flow_qtr(
            self, cash_flow_qtr: pd.DataFrame, db_instrument_id: int) -> None: 
        
        cash_flow_qtr = (
            cash_flow_qtr
            .rename(columns={'date': 'interval_end_date'})
            .assign(
                instrument_id = db_instrument_id,
                created_date = self.update_date,
                last_update_date = self.update_date,
            )
        )
        cash_flow_qtr.columns = (
            cash_flow_qtr
            .columns
            .map(get_snake_case_from_camel_case)
        )
        
        cash_flow_qtr.to_sql(
            'cash_flow_qtr', self.engine, if_exists='append', index=False
            )
        

    def update_cash_flow_yr(
            self, cash_flow_yr: pd.DataFrame, db_instrument_id: int) -> None: 
        
        cash_flow_yr = (
            cash_flow_yr
            .rename(columns={'date': 'interval_end_date'})
            .assign(
                instrument_id = db_instrument_id,
                created_date = self.update_date,
                last_update_date = self.update_date,
            )
        )
        cash_flow_yr.columns = (
            cash_flow_yr
            .columns
            .map(get_snake_case_from_camel_case)
        )
        
        cash_flow_yr.to_sql(
            'cash_flow_yr', self.engine, if_exists='append', index=False
            )
        
    def update_fundamentals_snapshot(
            self, fundamentals_snapshot: pd.DataFrame, db_instrument_id: int) -> None: 
        
        fundamentals_snapshot = (
            fundamentals_snapshot
            .rename(columns={
                'date': 'interval_end_date', 
                'Code': 'ticker',
                'Type': 'instrument_type'
                })
            .drop(columns=['Description', 'Address', 'Phone'])
            .assign(
                instrument_id = db_instrument_id,
                created_date = self.update_date,
                last_update_date = self.update_date,
            )
        )
        fundamentals_snapshot.columns = (
            fundamentals_snapshot
            .columns
            .map(get_snake_case_from_camel_case)
        )
        
        fundamentals_snapshot.to_sql(
            'fundamentals_snapshot', self.engine, if_exists='append', index=False
            )


    # TODO: map tickers to instument_ids before this step
    # def update_db_bulk_prices(
    #         self, bulk_prices: pd.DataFrame, db_exchange_id: int) -> None: 
        
    #     bulk_prices = (
    #         bulk_prices
    #         .rename(columns={'date': 'price_date'})
    #         .assign(
    #             instrument_id = db_instrument_id,
    #             created_date = self.update_date,
    #             last_update_date = self.update_date, 
    #         )
    #     )
    #     bulk_prices.columns = bulk_prices.columns.str.lower()
    #     bulk_prices['price_date'] = bulk_prices['price_date'].astype(str)

    #     bulk_prices.to_sql('price', self.engine, if_exists='append', index=False)






    

    


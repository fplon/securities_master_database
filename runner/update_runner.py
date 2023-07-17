import logging
import pandas as pd
# from typing import Tuple

from eod_data.eod_downloader import EODDownloader
from db_connection.db_uploader import DatabaseUploader
from db_connection.db_downloader import DatabaseDownloader
from config.watchlist_config import INDEX_CONSTITUENTS

LOGGER = logging.getLogger(__name__)


class DatabaseUpdaterTemplate: 

    def __init__(self): 
        self.eod_downloader = EODDownloader()
        self.database_uploader = DatabaseUploader
        self.database_downloader = DatabaseDownloader

    def _get_api_data(self): 
        # raise NotImplementedError
        pass

    def _get_db_data(self): 
        pass

    def _filter_new_data(self):
        pass

    def _log_unavailable_data(self): 
        pass

    def _upload_new_data_to_db(self): 
        pass


class DatabaseUpdaterExchangeData(DatabaseUpdaterTemplate):

    def _get_api_data(self) -> None: 
        self.api_exchanges = self.eod_downloader.get_eod_exchanges()
    
    def _get_db_data(self) -> None: 
        with self.database_downloader() as db_downloader: 
            self.db_exchanges = db_downloader.get_db_exchanges()
    
    def _filter_new_data(self) -> None:
        
        db_exchange_codes = set(self.db_exchanges['code'].values)
        api_exchange_codes = set(self.api_exchanges['Code'].values)

        self.new_exchange_codes = list(api_exchange_codes - db_exchange_codes)
        self.retired_exchange_codes = list(db_exchange_codes - api_exchange_codes)
        self.new_exchanges = (
            self.api_exchanges
            .loc[self.api_exchanges['Code'].isin(self.new_exchange_codes), :]
        )
        
    def _log_unavailable_data(self) -> None:
        
        LOGGER.error(
            'The following exchange codes are no longer available '
            f'via the API: {self.retired_exchange_codes}')
        LOGGER.error(
            'The following exchange codes are new and will be added '
            f'to the database: {self.new_exchange_codes}')
        
    def _upload_new_data_to_db(self):

        with self.database_uploader() as db_uploader: 
            db_uploader.update_db_exchanges(self.new_exchanges)
        

class DatabaseUpdaterIndexData(DatabaseUpdaterTemplate):

    def _get_api_data(self) -> None: 
        self.api_indices = self.eod_downloader.get_eod_indices()
    

    def _get_db_data(self) -> None: 
        with self.database_downloader() as db_downloader: 
            self.db_indices = db_downloader.get_db_indices()
    

    def _filter_new_data(self) -> None:
        
        db_index_codes = set(self.db_indices['ticker'].values)
        api_index_codes = set(self.api_indices['Code'].values)

        self.new_index_codes = list(api_index_codes - db_index_codes)
        self.retired_index_codes = list(db_index_codes - api_index_codes)
        self.new_indices = (
            self.api_indices
            .loc[self.api_indices['Code'].isin(self.new_index_codes), :]
        )
        
    def _log_unavailable_data(self) -> None:
        
        LOGGER.error(
            'The following index codes are no longer available '
            f'via the API: {self.retired_index_codes}')
        LOGGER.error(
            'The following index codes are new and will be added '
            f'to the database: {self.new_index_codes}')
        

    def _upload_new_data_to_db(self):

        with self.database_uploader() as db_uploader: 
            db_uploader.update_db_indices(self.new_indices)


class DatabaseUpdaterIndexConstituentsData(DatabaseUpdaterTemplate):

    # TODO: factor get_eod_constituents with method for multiple tickers
    def _get_api_data(self) -> None: 
        self.api_index_constituents = pd.DataFrame()
        for index_ticker in INDEX_CONSTITUENTS:
            constituents = self.eod_downloader.get_eod_constituents(
                index=index_ticker
            )
            self.api_index_constituents = pd.concat(
                [self.api_index_constituents ,constituents],
                ignore_index=True
            )

    def _get_db_data(self) -> None: 
        with self.database_downloader() as db_downloader: 
            index_id = db_downloader.map_db_index_id_from_index_code(INDEX_CONSTITUENTS)
            self.db_index_constituents = db_downloader.get_db_constituents(index_id)


    def _filter_new_data(self) -> None:
        
        db_constituents_codes = set(self.db_index_constituents[['index_date', 'ticker']].values)
        api_constituents_codes = set(self.api_index_constituents['Code'].values)

        self.new_index_codes = list(api_index_codes - db_index_codes)
        self.retired_index_codes = list(db_index_codes - api_index_codes)
        self.new_indices = (
            self.api_indices
            .loc[self.api_indices['Code'].isin(self.new_index_codes), :]
        )

    ### second level abstraction
    ## update exchanges
    # get eod exchanges
    # get db exchanges
    # filter new exchanges
    # log dropped exchanges
    # upload new exchanges
    #
    ## update indices and constituents (DOING SEPARATELY)
    # (list specified in config)
    # get eod indices 
    # get db indices 
    # get selection (list of indices required) from config
    # filter new indices?
    # log dropped indices
    # upload new indices (if any) 
    # get eod index constiuents for each index
    # upload new index constiuents
    #
    ## update instruments
    # (per exchange - list speciifed in config )
    # get eod instruments 
    # get db instruments
    # filter new instruments
    # log dropped instruments
    # upload new instruments
    #
    ## update prices
    # (per instrument)
    # get eod prices for each instrument
    # data quality checks 
    # get db prices 
    # log unavailable prices
    # log prices that don't match (check prev day prices as well)
    # upload new prices 
    # 
    ## update corp actions 
    # (per instrument)
    # get eod corp actions
    # get db corp actions
    # filter new corp actions 
    # log unavailable corp actions per instruments
    # upload new corp actions to db
    # 
    ## update fundamentals
    # (per instrument)
    # get eod fundamentals 
    # get db fundamentals 
    # filter new fundamentals 
    # log unavailable fundamentals per instruments
    # upload new fundamentals 

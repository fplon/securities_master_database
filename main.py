from logging import config, getLogger
import pandas as pd

from eod_data.eod_downloader import EODDownloader
from db_connection.db_connector import DatabaseConnector
from db_connection.db_uploader import DatabaseUploader
from db_connection.db_downloader import DatabaseDownloader
from config.log_config import LOG_CONFIG, disable_third_party_loggers

disable_third_party_loggers()
config.dictConfig(LOG_CONFIG)
logger = getLogger(__name__)

if __name__ == '__main__': 
    eod = EODDownloader()
    with DatabaseUploader() as db_uploader:
        pass

    ## think about where / what logs are needed 




    ### second level abstraction
    ## update exchanges
    # get eod exchanges
    # get db exchanges
    # filter new exchanges
    # log dropped exchanges
    # upload new exchanges
    #
    ## update indices and constituents 
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


    ### top level abstraction 
    # update exchanges
    # update indices and constituents (selection)
    # update instruments (select exchanges)
    # update prices (select instruments)
    # update corp actions (select instruments)
    # update fundamentals (select instruments)



    # exchanges
    eod_exchanges = eod.get_eod_exchanges()
    
    db_uploader.update_db_exchanges(eod_exchanges)



    # for exch_code in 
    # for exch_code 
    # eod_instruments = eod.get_eod_instruments


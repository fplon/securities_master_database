from logging import config, getLogger

from eod_data.eod_downloader import EODDownloader
from db_connection.db_connector import DatabaseConnector
from db_connection.db_uploader import DatabaseUploader
from db_connection.db_downloader import DatabaseDownloader
from config.log_config import LOG_CONFIG

config.dictConfig(LOG_CONFIG)
logger = getLogger(__name__)

if __name__ == '__main__':
    
    # db_update_instruments()
    # db_update_index_constituents() 
    # db_update_bulk_prices(exchange_list = [1, 2, 8, 68])
    # db_update_fund_watchlist_data()
    # db_update_company_fundamentals(exchange_list = [2])

    try: 
        eod = EODDownloader()
        # eod_exchanges = eod.get_eod_exchanges(format='df')
        # eod_instruments = eod.get_eod_instruments(exchange='US', format='df')
        # eod_indices = eod.get_eod_indices(format='df')
        # eod_constituents, eod_index_info = eod.get_eod_constituents('GSPC', format='df')
        # eod_price = eod.get_eod_price('MSFT', 'US', format = 'df')
        # eod_bulk_price = eod.get_eod_bulk_price('US', format='json')
        # (
        #     eod_earnings_qtr,
        #     eod_earnings_trend,
        #     eod_earnings_yr,
        #     eod_balance_sheet_qtr, 
        #     eod_income_statement_qtr,
        #     eod_cash_flow_qtr,
        #     eod_balance_sheet_yr,
        #     eod_income_statement_yr,
        #     eod_cash_flow_yr,
        #     eod_fundamentals_snapshot
        # ) = eod.get_eod_fundamentals('MSFT', 'US', format='df')
        # eod_div = eod.get_eod_corp_act('BP', 'LSE', corp_act_type='div', format='df')
        # eod_splits = eod.get_eod_corp_act('BP', 'LSE', corp_act_type='splits', format='df')
        # eod_etf = eod.get_eod_etf('VTI', 'US', format='df')

        # with DatabaseDownloader() as db_conn: 
            # db_exchanges = db_conn.get_db_table('exchange')
            # db_exchanges = db_conn.get_db_table('exchang')
            # db_table = db_conn.get_db_fund_watchlist()
            # db_table = db_conn.get_db_instruments()
            # db_table = db_conn.get_db_indices()

            # query_params = {'instrument_id': 12490}
            # db_price = db_conn.get_db_price(query_params)

            # query_params = {'price_date': '2021-04-29'}
            # db_price = db_conn.get_db_price(query_params)

            # query_params = {'instrument_id': 12490, 'price_date': '2021-04-29'}
            # db_price = db_conn.get_db_price(query_params)

            # query_params = {
            #     'instrument_id': 12490, 
            #     'price_date': '2021-04-29', 
            #     'include_ticker': True
            #     }
            # db_price = db_conn.get_db_price(query_params)
            
        # with DatabaseUploader() as db_up: 
            # logger.info(db_up.database)
            # logger.info(db_up.host)
            # logger.info(db_up.user)
            # logger.info(db_up.password)
            # logger.info(type(db_up.engine))
            # db_up.update_db_exchanges(exchanges=eod_exchanges)
            # db_up.update_db_instruments(instruments=eod_instruments, db_exchange_id=1)
            # db_up.update_db_indices(indices=eod_indices)
            # db_up.update_db_index_constituents(constituents=eod_constituents, db_index_id=428)
            # db_up.update_db_prices(eod_price, db_instrument_id=30252) # MSFT
            # db_up.update_earnings_qtr(eod_earnings_qtr, 30252)
            # db_up.update_earnings_yr(eod_earnings_yr, 30252)
            # db_up.update_earnings_trend(eod_earnings_trend, 30252)
            # db_up.update_balance_sheet_qtr(eod_balance_sheet_qtr, 30252)
            # db_up.update_balance_sheet_yr(eod_balance_sheet_yr, 30252)
            # db_up.update_income_statement_qtr(eod_income_statement_qtr, 30252)
            # db_up.update_income_statement_yr(eod_income_statement_yr, 30252)
            # db_up.update_cash_flow_qtr(eod_cash_flow_qtr, 30252)
            # db_up.update_cash_flow_yr(eod_cash_flow_yr, 30252)
            # db_up.update_fundamentals_snapshot(eod_fundamentals_snapshot, 30252)


    
    except:      
        logger.exception('Failed to download data.')

    logger.info('___')


# class DataSourceComparator():

#     def __init__(self) -> None:
        
#         self.db_downloader = DatabaseDownloader()
#         self.eod_downloader = EODDownloader()
#         self.data_differences = pd.DataFrame()


#     def compare_data_sources(self, data_group: str) -> None:
        
#         db_exchanges = self.db_downloader.get_db_exchanges()
#         eod_exchanges = self.eod_downloader.get_eod_exchanges()
#         x = 1

#         # TODO: I need a reformatted version of the eod data - make that a method in either EOD or DB downloader
        

# comparator = DataSourceComparator()
# comparator.compare_data_sources('exchange')





# def update_latest_exchanges_to_db():
    
#     # get_exchanges_not_in_db()
#     ...
    
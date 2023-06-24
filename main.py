from logging import config, getLogger

from eod_data.eod_downloader import eod_downloader
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
        eod = eod_downloader()
        # eod_exchanges = eod.get_eod_exchanges(format='df')
        # eod_instruments = eod.get_eod_instruments(exchange='LSE', format='df')
        # eod_bulk_price = eod.get_eod_bulk_price('LSE', format='df')
        # eod_constituents, eod_index_info = eod.get_eod_constituents('GSPC', format='json')
        # eod_price = eod.get_eod_price('MSFT', 'US', format = 'json')
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
        eod_etf = eod.get_eod_etf('VTI', 'US', format='df')
    
    except: 
        logger.exception('Failed to download data.')

    logger.info('___')



import requests
import json
import pandas as pd
import datetime as dt
import logging

from config.eod_config import API_KEY

logging.getLogger("urllib3").setLevel(logging.CRITICAL+1) # disable urllib logs
logger = logging.getLogger(__name__)

# TODO: data types checker decorator
# TODO: response code handler for 4xx and 5xx codes - response.status_code
# TODO: timer function decorator 
# TODO: date format checker
# TODO: parallel or multi-thread
# TODO: change "format" to another word
# TODO: decide whether to return empty object or throw error

class EODDownloader: 

    def __init__(self) -> None: 
        self.api_key = API_KEY
        self.root_url = f'http://eodhistoricaldata.com/api'
        self.start_date = '1990-01-01'
        self.end_date = (dt.datetime.now() - dt.timedelta(1)).strftime('%Y-%m-%d')
        

    def get_eod_bulk_price(
            self, exchange: str, format: str = 'df', 
            price_date: str | None = None) -> pd.DataFrame | list:
        """Gets prices for ONE date for all assets for a given exchange.

        Args:
            exchange (str): Exchange code eg. 'LSE'
            format (str, optional): Output format. Defaults to 'df'.
            price_date (str | None, optional): Date of prices. Defaults to None.

        Raises:
            ValueError: Output type must be either 'df' or 'json'

        Returns:
            pd.DataFrame | list: Bulk prices.
        """
        
        if price_date is None: 
            price_date = self.end_date

        valid_types = ('json', 'df')
        if format in valid_types: 

            url = (
                f'{self.root_url}/eod-bulk-last-day/{exchange}'
                f'?api_token={self.api_key}&fmt=json&date={price_date}'
            )
            response = requests.get(url)
            data = response.text

            if format == 'df': 
                eod_bulk_price = pd.read_json(data)
            elif format == 'json': 
                eod_bulk_price = json.loads(data)
            
            return eod_bulk_price

        else: 
            raise ValueError('Not a valid output format.')


    def get_eod_constituents(
            self, index: str, format: str = 'df'
            ) -> tuple[pd.DataFrame | dict, pd.DataFrame | dict]:
        """Gets the LATEST index contituents for a given index.

        Args:
            index (str): Index Code eg. 'GSPC'
            format (str, optional): Output format. Defaults to 'df'.

        Raises:
            ValueError: Output type must be either 'df' or 'json'

        Returns:
            tuple[pd.DataFrame | dict, pd.DataFrame | dict]: Index constituents and 
                general index information.
        """

        valid_types = ('json', 'df')
        if format in valid_types: 
            url = (
                f'{self.root_url}/fundamentals/{index}.INDX?api_token={self.api_key}'
                f'&historical=1&from={self.start_date}&to={self.end_date}'
                )
            
            response = requests.get(url)
            init_data = json.loads(response.text)

            eod_index_info = init_data.get('General', {})
            eod_constituents = init_data.get('Components', {})
            # eod_historical_constituents = init_data.get('HistoricalComponents', {})
            # eod_historical_tickers = init_data.get('HistoricalTickerComponents', {})
            
            if format == 'df':
                eod_index_info = pd.Series(eod_index_info, name='index_info').to_frame().T
                eod_constituents = pd.DataFrame.from_dict(eod_constituents).T
                # eod_historical_constituents = pd.DataFrame(eod_historical_constituents)
                # eod_historical_tickers = pd.DataFrame(eod_historical_tickers)

            # TODO: historical constituents research and solution
            return eod_constituents, eod_index_info
            
        
        else: 
            raise ValueError('Not a valid output format.')


    def get_eod_exchanges(self, format: str = 'df') -> pd.DataFrame | list:
        """Gets information on exchanges currently supported by EOD HD. 

        Args:
            format (str, optional): Output style. Defaults to 'df'.

        Raises:
            ValueError: Output type must be either 'df' or 'json'

        Returns:
            pd.DataFrame | list: Exchanges. 
        """
        
        valid_formats = ('json', 'df')
        if format in valid_formats: 
            
            url = f'{self.root_url}/exchanges-list/?api_token={self.api_key}&fmt=json'
            response = requests.get(url)
            data = response.text
            
            if format == 'json': 
                exchanges = json.loads(data)
            elif format == 'df': 
                exchanges = pd.read_json(data)

            return exchanges
        else: 
            raise ValueError('Not a valid output format')


    def get_eod_fundamentals(
            self, ticker: str, exchange: str, format='df') -> tuple[
                pd.DataFrame | dict, pd.DataFrame | dict, 
                pd.DataFrame | dict, pd.DataFrame | dict,
                pd.DataFrame | dict, pd.DataFrame | dict,
                pd.DataFrame | dict, pd.DataFrame | dict,
                pd.DataFrame | dict, pd.DataFrame | dict
                ]:
        """Gets company fundamentals data (including general info).

        Args:
            ticker (str): Asset ticker code
            exchange (str): Exchange code
            format (str, optional): Output style. Defaults to 'df'.

        Raises:
            ValueError: Output type must be either 'df' or 'json'

        Returns:
            tuple[ pd.DataFrame | dict, pd.DataFrame | dict, 
                pd.DataFrame | dict, pd.DataFrame | dict, pd.DataFrame | dict, 
                pd.DataFrame | dict, pd.DataFrame | dict, pd.DataFrame | dict, 
                pd.DataFrame | dict, pd.DataFrame | dict ]: _description_
        """

        if format not in ('json', 'df'):
            raise ValueError('Not a valid output format')

        url = (
            f'{self.root_url}/fundamentals/{ticker}.{exchange}?from={self.start_date}'
            f'&to={self.end_date}&api_token={self.api_key}&period=d&fmt=json'
            )
        response = requests.get(url)
        init_data = json.loads(response.text)

        eod_fundamentals_snapshot = {
            **init_data.get('General', {}),
            **init_data.get('Highlights', {}),
            **init_data.get('Valuation', {}),
            **init_data.get('SharesStats', {}),
            **init_data.get('SplitsDividends', {}),
            **init_data.get('AnalystRatings', {}),
            # **init_data.get('Holders', {}), # leave for now
            # **init_data.get('InsiderTransactions', {}), # leave for now
            # **init_data.get('ESGScores', {}), # leave for now
            # **init_data.get('outstandingShares', {}) # leave for now
        }

        eod_earnings_qtr = init_data.get('Earnings', {}).get('History', {})
        eod_earnings_trend = init_data.get('Earnings', {}).get('Trend', {}) 
        eod_earnings_yr = init_data.get('Earnings', {}).get('Annual', {})
        eod_balance_sheet_qtr = (
            init_data
            .get('Financials', {})
            .get('Balance_Sheet', {})
            .get('quarterly', {})
        )
        eod_income_statement_qtr = (
            init_data
            .get('Financials', {})
            .get('Income_Statement', {})
            .get('quarterly', {})
        )
        eod_cash_flow_qtr = (
            init_data
            .get('Financials', {})
            .get('Cash_Flow', {})
            .get('quarterly', {})
        )
        eod_balance_sheet_yr = (
            init_data
            .get('Financials', {})
            .get('Balance_Sheet', {})
            .get('yearly', {})
        )
        eod_income_statement_yr = (
            init_data
            .get('Financials', {})
            .get('Income_Statement', {})
            .get('yearly', {})
        )
        eod_cash_flow_yr = (
            init_data
            .get('Financials', {})
            .get('Cash_Flow', {})
            .get('yearly', {})
        )

        if format == 'df': 

            def _dict_to_df(eod_dict: dict) -> pd.DataFrame:
                if len(eod_dict.keys()) == 0:
                    logger.warning(
                        f'{ticker}.{exchange} has no fundamental data. Returned empty df.'
                        )
                    return pd.DataFrame()
                return pd.DataFrame.from_dict(eod_dict).T.set_index('date')

            eod_earnings_qtr = _dict_to_df(eod_earnings_qtr)
            eod_earnings_trend = _dict_to_df(eod_earnings_trend)
            eod_earnings_yr = _dict_to_df(eod_earnings_yr)
            eod_balance_sheet_qtr = _dict_to_df(eod_balance_sheet_qtr)
            eod_income_statement_qtr = _dict_to_df(eod_income_statement_qtr)
            eod_cash_flow_qtr = _dict_to_df(eod_cash_flow_qtr)
            eod_balance_sheet_yr = _dict_to_df(eod_balance_sheet_yr)
            eod_income_statement_yr = _dict_to_df(eod_income_statement_yr)
            eod_cash_flow_yr = _dict_to_df(eod_cash_flow_yr)
            
            snapshot_drop_cols = [
                'AddressData', 
                'Listings', 
                'Officers', 
                'NumberDividendsByYear'
                ]
            eod_fundamentals_snapshot = (
                pd.Series(eod_fundamentals_snapshot, name=self.end_date)
                .drop(snapshot_drop_cols) # drop nested fields for now
                .to_frame()
                .T
            )
        
        # TODO: handler for ETFs
        return (
            eod_earnings_qtr,
            eod_earnings_trend,
            eod_earnings_yr,
            eod_balance_sheet_qtr, 
            eod_income_statement_qtr,
            eod_cash_flow_qtr,
            eod_balance_sheet_yr,
            eod_income_statement_yr,
            eod_cash_flow_yr,
            eod_fundamentals_snapshot
        )


    def get_eod_instruments(
            self, exchange: str = 'INDX', format: str = 'df') -> pd.DataFrame | list: 
        """Gets the instruments (assets) for a given exchange. 

        Args:
            exchange (str, optional): Code for the exchange. Defaults to 'INDX'.
            format (str, optional): Output style. Defaults to 'df'.

        Raises:
            ValueError: Output type must be either 'df' or 'json'

        Returns:
            pd.DataFrame | list: Instruments. 
        """
        
        valid_formats = ('json', 'df')
        if format in valid_formats: 

            url = (
                f'{self.root_url}/exchange-symbol-list/{exchange}'
                f'?api_token={self.api_key}&fmt=json'
                )
            response = requests.get(url)
            data = response.text
            
            if format == 'json': 
                eod_instruments = json.loads(data)
            elif format == 'df': 
                eod_instruments = pd.read_json(data)
            
            return eod_instruments
        
        else: 
            raise ValueError('Not a valid output format')


    def get_eod_price(
            self, ticker: str, exchange: str, start_date: str | None = None, 
            end_date: str | None = None, format: str = 'df') -> pd.DataFrame:
        """Gets OHLCV prices for a given ticker and exchange, also over the 
            desired start and end dates.

        Args:
            ticker (str): Asset ticker code
            exchange (str): Exchange code
            start_date (str | None, optional): Starting date of series. Defaults to None.
            end_date (str | None, optional): End date of series. Defaults to None.
            format (str, optional): Output style. Defaults to 'df'.

        Raises:
            ValueError: Output type must be either 'df' or 'json'

        Returns:
            pd.DataFrame: OHLCV price series. 
        """
        
        valid_formats = ('json', 'df')
        if format in valid_formats:

            if start_date is None: 
                start_date = self.start_date
            if end_date is None: 
                end_date = self.end_date

            url = (
                f'{self.root_url}/eod/{ticker}.{exchange}?from={start_date}&'
                f'to={end_date}&api_token={self.api_key}&period=d&fmt=json'
                )

            response = requests.get(url)
            data = response.text
            
            if format == 'json': 
                eod_price = json.loads(data)
            elif format == 'df': 
                eod_price = pd.read_json(data)
                
                if eod_price.shape[0] > 0: 
                    eod_price.set_index('date', inplace=True)
            
            # TODO: handler for ticker, exchange mismatch
            return eod_price
        
        else: 
            raise ValueError('Not a valid output format')
            

    def get_eod_corp_act(
            self, ticker: str, exchange: str, corp_act_type: str, 
            format: str = 'df') -> pd.DataFrame | list:
        """Gets corporate action history for a given company and exchange. 

        Args:
            ticker (str): Asset ticker code
            exchange (str): Exchange code
            corp_act_type (str): Either dividends paid or stock splits.
            format (str, optional): Output style. Defaults to 'df'.

        Raises:
            ValueError: Output type must be either 'df' or 'json'
            ValueError: Corp action type must be either 'div' or 'splits'

        Returns:
            pd.DataFrame | list: Corporate actions series.
        """

        valid_types = ('json', 'df')
        if format not in (valid_types): 
            raise ValueError('Not a valid output format.')
        
        valid_corp_acts = ('div', 'splits')
        if corp_act_type not in valid_corp_acts:
            raise ValueError('Not a valid corporate action type.')

        url = (
            f'{self.root_url}/{corp_act_type}/{ticker}.{exchange}?'
            f'api_token={self.api_key}&from={self.start_date}&fmt=json'
            )
        response = requests.get(url)
        data = response.text
        if format == 'json': 
            eod_corp_act = json.loads(data)
        elif format == 'df': 
            eod_corp_act = pd.read_json(data)
            if eod_corp_act.shape[0] > 0: 
                eod_corp_act.set_index('date', inplace = True)
    
        return eod_corp_act      
        

    def get_eod_etf(self, ticker: str, exchange: str, format: str = 'df') -> pd.DataFrame:

        valid_types = ('json', 'df')
        if format not in (valid_types): 
            raise ValueError('Not a valid output format.')
        
        url = (
            f'{self.root_url}/fundamentals/{ticker}.{exchange}?'
            f'api_token={self.api_key}&historical=1'
            )
        
        response = requests.get(url)
        init_data = json.loads(response.text)

        eod_etf = {
            **init_data.get('General', {}), 
            **init_data.get('Technicals', {}), 
            **init_data.get('ETF_Data', {})
        }

        if format == 'df': 
            # TODO: find solution instead of dropping these if df
            snapshot_drop_cols = [
                'Market_Capitalisation', 
                'Asset_Allocation',
                'World_Regions', 
                'Sector_Weights', 
                'Fixed_Income', 
                'Holdings_Count',
                'Top_10_Holdings', 
                'Holdings', 
                'Valuations_Growth', 
                'MorningStar',
                'Performance'
                ]
            eod_etf = (
                pd.Series(eod_etf, name=self.end_date)
                .drop(snapshot_drop_cols) # drop nested fields for now
                .to_frame()
                .T
            )

        return eod_etf
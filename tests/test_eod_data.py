import pytest
from pandas import DataFrame as pDataFrame

from eod_data.eod_downloader import eod_downloader
from config.eod_config import API_KEY



def test_eod_downloader() -> None: 
    eod = eod_downloader()
    assert eod.api_key == API_KEY
    assert eod.root_url == 'http://eodhistoricaldata.com/api'

def test_get_eod_exchanges() -> None: 
    expected_cols = [
        'Name', 
        'Code', 
        'OperatingMIC', 
        'Country', 
        'Currency', 
        'CountryISO2',
        'CountryISO3'
        ]
    eod = eod_downloader()
    
    eod_exchanges = eod.get_eod_exchanges(format='json')
    assert type(eod_exchanges) == list
    assert type(eod_exchanges[0]) == dict
    assert all(act == exp for act, exp in zip(list(eod_exchanges[0].keys()), expected_cols))
    
    eod_exchanges = eod.get_eod_exchanges(format='df')
    assert type(eod_exchanges) == pDataFrame
    assert all(act == exp for act, exp in zip(eod_exchanges.columns, expected_cols))

    with pytest.raises(ValueError):
        eod.get_eod_exchanges(format='str')
    

def test_get_eod_instruments() -> None: 
    expected_cols = [
        'Code', 
        'Name', 
        'Country', 
        'Exchange', 
        'Currency', 
        'Type', 
        'Isin'
        ]
    eod = eod_downloader()
    
    eod_instruments = eod.get_eod_instruments(format='json')
    assert type(eod_instruments) == list
    assert type(eod_instruments[0]) == dict
    assert all(act == exp for act, exp in zip(list(eod_instruments[0].keys()), expected_cols))
    
    eod_instruments = eod.get_eod_instruments(format='df')
    assert type(eod_instruments) == pDataFrame
    assert all(act == exp for act, exp in zip(eod_instruments.columns, expected_cols))

    with pytest.raises(ValueError):
        eod.get_eod_instruments(format='str')


def test_get_eod_bulk_price() -> None: 
    expected_cols = [
        'code', 
        'exchange_short_name', 
        'date', 
        'open', 
        'high', 
        'low', 
        'close',
        'adjusted_close', 
        'volume'   
        ]
    eod = eod_downloader()

    test_date = '2023-06-22'
    test_exchange = 'LSE'
    
    eod_bulk_price = eod.get_eod_bulk_price(exchange=test_exchange, format='json')
    assert type(eod_bulk_price) == list
    assert type(eod_bulk_price[0]) == dict
    assert all(act == exp for act, exp in zip(list(eod_bulk_price[0].keys()), expected_cols))
    unique_date = eod_bulk_price[0].get('date', None)
    assert unique_date is not None
    assert unique_date == test_date
    
    
    eod_bulk_price = eod.get_eod_bulk_price(
        exchange=test_exchange, format='df', price_date=test_date)
    assert type(eod_bulk_price) == pDataFrame
    assert all(act == exp for act, exp in zip(eod_bulk_price.columns, expected_cols))

    with pytest.raises(ValueError):
        eod.get_eod_bulk_price(exchange=test_exchange, format='str')

    
def test_get_eod_constituents() -> None: 
    expected_cols = {
        'eod_constituents': [
            'Code', 
            'Exchange', 
            'Name', 
            'Sector', 
            'Industry'
            ],   
        'eod_index_info': [
            'Code', 
            'Type', 
            'Name', 
            'Exchange', 
            'CurrencyCode', 
            'CurrencyName',
            'CurrencySymbol', 
            'CountryName', 
            'CountryISO', 
            'OpenFigi'
            ]
        }
    eod = eod_downloader()

    test_index = 'GSPC'
    
    eod_constituents, eod_index_info = eod.get_eod_constituents(
        index=test_index, format='json')
    assert type(eod_constituents) == dict
    assert type(eod_constituents.get('0')) == dict
    assert all(
        act == exp for act, exp 
        in zip(list(eod_constituents.get('0').keys()), expected_cols.get('eod_constituents'))
        )     
    assert all(
        act == exp for act, exp 
        in zip(eod_index_info.keys(), expected_cols.get('eod_index_info'))
        )
    
    eod_constituents, eod_index_info = eod.get_eod_constituents(
        index=test_index, format='df')
    assert type(eod_constituents) == pDataFrame
    assert type(eod_index_info) == pDataFrame
    assert all(
        act == exp for act, exp 
        in zip(eod_constituents.columns, expected_cols.get('eod_constituents'))
        )
    assert all(
        act == exp for act, exp 
        in zip(eod_index_info.columns, expected_cols.get('eod_index_info'))
        )

    with pytest.raises(ValueError):
        eod.get_eod_constituents(index=test_index, format='str')


def test_get_eod_price() -> None: 
    
    eod = eod_downloader()
    
    expected_cols = ['date', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume']
    test_ticker = 'MSFT'
    test_exchange = 'US'
    test_start_date = '2023-06-01'
    test_end_date = '2023-06-22'
    
    eod_price = eod.get_eod_price(
        ticker=test_ticker, exchange=test_exchange, start_date=test_start_date, 
        end_date=test_end_date, format='json')
    assert type(eod_price) == list
    assert type(eod_price[0]) == dict
    assert all(act == exp for act, exp in zip(list(eod_price[0].keys()), expected_cols))
    act_start_date = eod_price[0].get('date', None)
    assert act_start_date is not None
    assert act_start_date == test_start_date
    act_end_date = eod_price[-1].get('date', None)
    assert act_end_date is not None
    assert act_end_date == test_end_date

    
    eod_price = eod.get_eod_price(ticker=test_ticker, exchange=test_exchange, format='df')
    assert type(eod_price) == pDataFrame
    assert all(act == exp for act, exp in zip(eod_price.columns, expected_cols[1:]))

    with pytest.raises(ValueError):
        eod.get_eod_price(ticker=test_ticker, exchange=test_exchange, format='str')


def test_get_eod_fundamentals() -> None: 
    # for what is the most complex method, this is a bit light
    
    eod = eod_downloader()

    # expected_cols = {} 
    test_ticker = 'MSFT'
    test_exchange = 'US'

    eod_fundamentals_dicts = eod.get_eod_fundamentals(
        ticker=test_ticker, exchange=test_exchange, format='json'
        )
    for d in eod_fundamentals_dicts: 
        assert type(d) == dict

    eod_fundamentals_dfs = eod.get_eod_fundamentals(
        ticker=test_ticker, exchange=test_exchange, format='df'
        )
    assert len(eod_fundamentals_dfs) == 10
    for d in eod_fundamentals_dfs: 
        assert type(d) == pDataFrame
        if not d.empty:
            assert type(d.index[0]) == str

    with pytest.raises(ValueError):
        eod.get_eod_fundamentals(ticker=test_ticker, exchange=test_exchange, format='str')


def test_get_eod_corp_act() -> None: 
    
    eod = eod_downloader()
    
    expected_cols = {
        'div': [
            'date', 
            'declarationDate', 
            'recordDate', 
            'paymentDate', 
            'period', 
            'value',
            'unadjustedValue', 
            'currency'
        ], 
        'splits': ['date', 'split']
        }
    test_ticker = 'BP'
    test_exchange = 'LSE'
    
    eod_div = eod.get_eod_corp_act(
        ticker=test_ticker, exchange=test_exchange, corp_act_type='div', format='json'
        )
    assert type(eod_div) == list
    assert type(eod_div[0]) == dict
    assert all(
        act == exp for act, exp 
        in zip(list(eod_div[0].keys()), expected_cols.get('div'))
        )
       
    eod_div = eod.get_eod_corp_act(
        ticker=test_ticker, exchange=test_exchange, corp_act_type='div', format='df'
        )
    assert type(eod_div) == pDataFrame
    assert all(
        act == exp for act, exp 
        in zip(eod_div.columns, expected_cols.get('div')[1:])
        )

    with pytest.raises(ValueError):
        eod.get_eod_corp_act(
            ticker=test_ticker, exchange=test_exchange, corp_act_type='div', format='str'
            )

    eod_splits = eod.get_eod_corp_act(
        ticker=test_ticker, exchange=test_exchange, corp_act_type='splits', format='json')
    assert type(eod_splits) == list
    assert type(eod_splits[0]) == dict
    assert all(
        act == exp for act, exp 
        in zip(list(eod_splits[0].keys()), expected_cols.get('splits'))
        )
       
    eod_splits = eod.get_eod_corp_act(
        ticker=test_ticker, exchange=test_exchange, corp_act_type='splits', format='df'
        )
    assert type(eod_splits) == pDataFrame
    assert all(
        act == exp for act, exp 
        in zip(eod_splits.columns, expected_cols.get('splits')[1:])
        )

    with pytest.raises(ValueError):
        eod.get_eod_corp_act(
            ticker=test_ticker, exchange=test_exchange, corp_act_type='splits', format='str'
            )


def test_get_eod_etf() -> None: 

    eod = eod_downloader()

    expected_cols = [
        'Code', 'Type', 'Name', 'Exchange', 'CurrencyCode', 'CurrencyName',
        'CurrencySymbol', 'CountryName', 'CountryISO', 'OpenFigi',
        'Description', 'Category', 'UpdatedAt', 'Beta', '52WeekHigh',
        '52WeekLow', '50DayMA', '200DayMA', 'ISIN', 'Company_Name',
        'Company_URL', 'ETF_URL', 'Domicile', 'Index_Name', 'Yield',
        'Dividend_Paying_Frequency', 'Inception_Date', 'Max_Annual_Mgmt_Charge',
        'Ongoing_Charge', 'Date_Ongoing_Charge', 'NetExpenseRatio',
        'AnnualHoldingsTurnover', 'TotalAssets', 'Average_Mkt_Cap_Mil'
        ]
    test_ticker = 'VTI'
    test_exchange = 'US'
    
    eod_etf = eod.get_eod_etf(
        ticker=test_ticker, exchange=test_exchange, format='json'
        )
    assert type(eod_etf) == dict
    assert all(act == exp for act, exp in zip(list(eod_etf.keys()), expected_cols))
    
    eod_etf = eod.get_eod_etf(
        ticker=test_ticker, exchange=test_exchange, format='df'
        )
    assert type(eod_etf) == pDataFrame
    assert all(act == exp for act, exp in zip(eod_etf.columns, expected_cols))

    with pytest.raises(ValueError):
        eod.get_eod_etf(
            ticker=test_ticker, exchange=test_exchange, format='str'
            )
import pytest
from pandas import DataFrame as pDataFrame
from sqlalchemy.engine.base import Engine as saEngine
# from psycopg2.errors import UndefinedTable

from db_downloader.db_downloader import DatabaseConnector
from config.db_config import CONN_CONF

# TODO: tests for new fundamentals tables

def test_db_conn() -> None: 
    with DatabaseConnector() as db_conn: 
        assert db_conn.database == CONN_CONF.get('database')
        assert db_conn.user == CONN_CONF.get('user')
        assert db_conn.password == CONN_CONF.get('password')
        assert db_conn.host == CONN_CONF.get('host')
        assert type(db_conn.engine) == saEngine


def test_get_db_table() -> None: 
    expected_cols = [
        'id', 'code', 'name', 'short_name', 'country', 'currency',
        'created_date', 'last_update_date', 'last_price_update_date',
        'last_fundamental_update_date'
    ]

    with DatabaseConnector() as db_conn: 
        db_exchanges = db_conn.get_db_table(table_name='exchange')
        assert type(db_exchanges) == pDataFrame
        assert all(
            act == exp for act, exp 
            in zip(list(db_exchanges.columns), expected_cols)
            )
        assert db_exchanges.shape[0] == len(db_exchanges['id'].unique())


def test_get_db_table_from_query() -> None: 
    expected_cols = [
        'id', 'code', 'name', 'short_name', 'country', 'currency',
        'created_date', 'last_update_date', 'last_price_update_date',
        'last_fundamental_update_date'
    ]
    query = 'SELECT * FROM exchange;'
    query_fail = 'SELECT * FROM faulty_table;'

    with DatabaseConnector() as db_conn: 
        db_exchanges = db_conn.get_db_table_from_query(query=query)
        assert type(db_exchanges) == pDataFrame
        assert all(
            act == exp for act, exp 
            in zip(list(db_exchanges.columns), expected_cols)
            )
        assert db_exchanges.shape[0] == len(db_exchanges['id'].unique())

        # with DatabaseConnector() as db_conn: 
        #     with pytest.raises(UndefinedTable):
        #         db_conn.get_db_table_from_query(query=query_fail)

def test_get_db_exchange() -> None: 
    expected_cols = [
        'id', 'code', 'name', 'short_name', 'country', 'currency',
        'created_date', 'last_update_date', 'last_price_update_date',
        'last_fundamental_update_date'
    ]

    with DatabaseConnector() as db_conn: 
        db_exchanges = db_conn.get_db_exchanges()
        assert type(db_exchanges) == pDataFrame
        assert all(
            act == exp for act, exp 
            in zip(list(db_exchanges.columns), expected_cols)
            )
        assert db_exchanges.shape[0] == len(db_exchanges['id'].unique())


def test_get_db_fund_watchlist() -> None: 
    expected_cols = ['id', 'instrument_id', 'created_date', 'last_update_date']

    with DatabaseConnector() as db_conn: 
        db_table = db_conn.get_db_fund_watchlist()
        assert type(db_table) == pDataFrame
        assert all(
            act == exp for act, exp 
            in zip(list(db_table.columns), expected_cols)
            )
        assert db_table.shape[0] == len(db_table['id'].unique())


def test_get_db_instrument() -> None: 
    expected_cols = [
        'id', 'exchange_id', 'ticker', 'instrument_type', 'name', 'currency',
        'created_date', 'last_update_date'
    ]

    with DatabaseConnector() as db_conn: 
        db_table = db_conn.get_db_instruments()
        assert type(db_table) == pDataFrame
        assert all(
            act == exp for act, exp 
            in zip(list(db_table.columns), expected_cols)
            )
        assert db_table.shape[0] == len(db_table['id'].unique())

    with DatabaseConnector() as db_conn: 
        db_table = db_conn.get_db_instruments(exchange_id=2)
        assert type(db_table) == pDataFrame
        assert all(
            act == exp for act, exp 
            in zip(list(db_table.columns), expected_cols)
            )
        assert db_table.shape[0] == len(db_table['id'].unique())


def test_get_db_indices() -> None: 
    expected_cols = [
        'id', 'short_name', 'name', 'city', 'country', 'timezone_offset',
        'created_date', 'last_updated_date'
    ]

    with DatabaseConnector() as db_conn: 
        db_table = db_conn.get_db_indices()
        assert type(db_table) == pDataFrame
        assert all(
            act == exp for act, exp 
            in zip(list(db_table.columns), expected_cols)
            )
        assert db_table.shape[0] == len(db_table['id'].unique())


def test_get_db_price() -> None: 
    expected_cols = [
        'id', 'data_vendor_id', 'instrument_id', 'price_date', 'created_date',
        'last_updated_date', 'open_price', 'high_price', 'low_price',
        'close_price', 'adj_close_price', 'volume'
    ]
    expected_cols_w_join = expected_cols + ['ticker']

    with DatabaseConnector() as db_conn: 
        db_table = db_conn.get_db_price(price_date='2021-04-29')
        assert type(db_table) == pDataFrame
        assert all(
            act == exp for act, exp 
            in zip(list(db_table.columns), expected_cols)
            )
        assert db_table.shape[0] == len(db_table['id'].unique())

    with DatabaseConnector() as db_conn: 
        db_table = db_conn.get_db_price(instrument_id=12490)
        assert type(db_table) == pDataFrame
        assert all(
            act == exp for act, exp 
            in zip(list(db_table.columns), expected_cols)
            )
        assert db_table.shape[0] == len(db_table['id'].unique())
        
    with DatabaseConnector() as db_conn: 
        db_table = db_conn.get_db_price(
            instrument_id=12490, price_date='2021-04-29'
            )
        assert type(db_table) == pDataFrame
        assert all(
            act == exp for act, exp 
            in zip(list(db_table.columns), expected_cols)
            )
        assert db_table.shape[0] == len(db_table['id'].unique())

    with DatabaseConnector() as db_conn: 
        db_table = db_conn.get_db_price(
            instrument_id=12490, price_date='2021-04-29',
            include_ticker=True
            )
        assert type(db_table) == pDataFrame
        assert all(
            act == exp for act, exp 
            in zip(list(db_table.columns), expected_cols_w_join)
            )
        assert db_table.shape[0] == len(db_table['id'].unique())


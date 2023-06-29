import pytest
import pathlib

from utils.strings import get_snake_case_from_camel_case, get_sql_string_from_file
from utils.dates import get_prev_bd


def test_get_snake_case_from_camel_case() -> None: 
    test_strings = {
        'ThisString': 'this_string', 
        'ASAP': 'asap', 
        'EPSGrowth': 'eps_growth', 
        'AssetsYOY': 'assets_yoy', 
        'OneTWOThree': 'one_two_three'
    }

    for input_str, output_str in test_strings.items(): 
        assert get_snake_case_from_camel_case(input_str) == output_str


def test_get_prev_bd() -> None: 
    test_dates = {
        'thu': '2023-06-29',
        'fri': '2023-06-30',
        'sat': '2023-07-01',
        'sun': '2023-07-02',
        'mon': '2023-07-03',
        'tue': '2023-07-04',
        'wed': '2023-07-05'
    }

    assert get_prev_bd(test_dates.get('fri')) == test_dates.get('thu')
    assert get_prev_bd(test_dates.get('sat')) == test_dates.get('fri')
    assert get_prev_bd(test_dates.get('sun')) == test_dates.get('fri')
    assert get_prev_bd(test_dates.get('mon')) == test_dates.get('fri')
    assert get_prev_bd(test_dates.get('tue')) == test_dates.get('mon')
    assert get_prev_bd(test_dates.get('wed')) == test_dates.get('tue')


def test_get_sql_string_from_file() -> None: 

    root_path = str(pathlib.Path(__file__).parent)
    path = f'{root_path}/sql_files/select_from_table.sql'
    expected_sql = 'SELECT * FROM sample_table;'

    sql = get_sql_string_from_file(path=path)

    assert sql == expected_sql

    
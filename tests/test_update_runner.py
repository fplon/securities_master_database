import pytest

from runner.update_runner import (
    DatabaseUpdaterTemplate, 
    DatabaseUpdaterExchangeData, 
    DatabaseUpdaterIndexData
)

# should mock these instead of calling to db

def test_template_get_eod_data() -> None: 
    updater = DatabaseUpdaterTemplate()
    data = updater._get_eod_data()
    assert data is None

def test_template_get_db_data() -> None: 
    updater = DatabaseUpdaterTemplate()
    data = updater._get_db_data()
    assert data is None

def test_template_filter_new_data() -> None: 
    updater = DatabaseUpdaterTemplate()
    data = updater._filter_new_data()
    assert data is None

def test_template_log_unavailable_data() -> None: 
    updater = DatabaseUpdaterTemplate()
    data = updater._log_unavailable_data()
    assert data is None

def test_template_upload_new_data_to_db() -> None: 
    updater = DatabaseUpdaterTemplate()
    data = updater._upload_new_data_to_db()
    assert data is None

def test_exchange_update_db() -> None: 
    updater = DatabaseUpdaterExchangeData()
    updater._get_api_data()
    updater._get_db_data()
    updater._filter_new_data()
    updater._log_unavailable_data()
    updater._upload_new_data_to_db()

def test_index_update_db() -> None: 
    updater = DatabaseUpdaterIndexData()
    updater._get_api_data()
    updater._get_db_data()
    updater._filter_new_data()
    updater._log_unavailable_data()
    updater._upload_new_data_to_db()


    
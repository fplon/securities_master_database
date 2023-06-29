from sqlalchemy.engine.base import Engine as saEngine

from db_connection.db_connector import DatabaseConnector
from config.db_config import CONN_CONF

def test_db_conn() -> None: 
    with DatabaseConnector() as db_conn: 
        assert db_conn.database == CONN_CONF.get('database')
        assert db_conn.user == CONN_CONF.get('user')
        assert db_conn.password == CONN_CONF.get('password')
        assert db_conn.host == CONN_CONF.get('host')
        assert type(db_conn.engine) == saEngine
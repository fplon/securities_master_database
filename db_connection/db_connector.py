# import psycopg2 as pg
import pandas as pd
import logging
from sqlalchemy import create_engine

from config.db_config import CONN_CONF

logger = logging.getLogger(__name__)


class DatabaseConnector:

    def __init__(self, conn_config: dict = CONN_CONF) -> None: 
        self.database, self.user, self.password, self.host = conn_config.values()
        self.engine = self._init_engine()

    def __enter__(self): 
        return self

    def __exit__(self, type, value, traceback) -> None: 
        self._dispose_engine()
        self.engine = None

    def _init_engine(self) : 
        return create_engine(
            f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}/{self.database}'
            )
    
    def _dispose_engine(self):
        self.engine.dispose()
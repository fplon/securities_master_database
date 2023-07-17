# import pytest
# import pathlib
# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker

# from db_connection.db_uploader import DatabaseUploader
# from config.db_config import CONN_CONF_TEST
# from utils.strings import get_sql_string_from_file

# ROOT_PATH = str(pathlib.Path(__file__).parent.parent)

# TODO - more background on this required (pytest, mocks) ...

# @pytest.mark.database
def test_database_write():

    assert False

    # with DatabaseUploader(conn_config=CONN_CONF_TEST) as db_conn: 
        
    #     # Session = sessionmaker(bind=db_conn.engine)
    #     # session = Session()
    #     con = db_conn.engine.connect()

    #     con.execute(f'CREATE DATABASE securities_master_test')

    #     con.execute(get_sql_string_from_file(f'{ROOT_PATH}/sql_files/exchange.sql'))
        # Perform the database write operation
        # ...
        # Your code to test the database write operation goes here

        # Clean up the database after the test
        # ...

# @pytest.fixture(autouse=True)
# def setup_teardown():

#     with DatabaseUploader(conn_config=CONN_CONF_TEST) as db_conn: 

#         # Create a SQLAlchemy engine to connect to the default 'postgres' database
#         db_url = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/postgres'
#         engine = create_engine(db_url)

#         # Create a connection
#         connection = engine.connect()

#         # Create the test database
#         connection.execute(f"CREATE DATABASE {DB_NAME}")

#         # Close the connection
#         connection.close()

#         # Create a SQLAlchemy engine to connect to the test database
#         db_url = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
#         engine = create_engine(db_url)

#         # Create the database tables or perform any other setup
#         # ...
#         # Your code to set up the database for testing goes here

#         # Yield to run the test function
#         yield

#         # Clean up the test database
#         # Drop the database tables or perform any other cleanup
#         # ...
#         # Your code to clean up the database after testing goes here

#         # Create a connection
#         connection = engine.connect()

#         # Drop the test database
#         connection.execute(f"DROP DATABASE {DB_NAME}")

#         # Close the connection
#         connection.close()
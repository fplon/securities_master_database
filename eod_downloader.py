import requests
import json
import pandas as pd
import numpy as np
import datetime as dt
import psycopg2 as pg
import pymongo as pm
import os

import eod_api

api = eod_api.api
e_date = (dt.datetime.now() - dt.timedelta(1)).strftime('%Y-%m-%d')
error_log = []


def get_db_exchanges():
    
    con = pg.connect(database = 'securities_master', user = 'postgres')
    cur = con.cursor()
    
    command = 'SELECT * FROM exchange'
    cur.execute(command)
    data = cur.fetchall()
    
    db_exchanges_df = pd.DataFrame(
        data, 
        columns = [
            'id', 
            'code', 
            'name', 
            'short_name', 
            'country', 
            'currency', 
            'created_date', 
            'last_updated_date',
            'last_price_update_date',
            'last_fundamental_update_date'
        ]
    )
    db_exchanges_df.set_index('id', inplace = True)
    
    cur.close()
    con.close()
    
    return db_exchanges_df


def get_db_fundamentals(instrument_id): 
    pass


def get_db_fund_watchlist():
    
    con = pg.connect(database = 'securities_master', user = 'postgres')
    cur = con.cursor()
    
    command = 'SELECT * FROM fund_watchlist'
    cur.execute(command)
    data = cur.fetchall()
    
    db_fund_watchlist_df = pd.DataFrame(
        data, 
        columns = ['id', 'instrument_id', 'created_date', 'last_updated_date']
    )
    db_fund_watchlist_df.set_index('id', inplace = True)
    
    cur.close()
    con.close()
    
    return db_fund_watchlist_df


def get_db_indices(): 
    
    con = pg.connect(database = 'securities_master', user = 'postgres')
    cur = con.cursor()
    
    command = 'SELECT * FROM benchmark_index'
    cur.execute(command)
    data = cur.fetchall()
    
    db_indices_df = pd.DataFrame(
        data, 
        columns = [
            'id', 
            'short_name', 
            'name', 
            'city', 
            'country', 
            'timezone_offset', 
            'created_date', 
            'last_updated_date'
        ]
    )
    db_indices_df.set_index('id', inplace = True)
    
    cur.close()
    con.close()
    
    return db_indices_df


def get_db_instruments(exchange_id = None):
    
    con = pg.connect(database = 'securities_master', user = 'postgres')
    cur = con.cursor()
    
    if exchange_id == None: 
        command = 'SELECT * FROM instrument'
    else:
        command = f'SELECT * FROM instrument WHERE exchange_id = {exchange_id}'
    # command = ('SELECT sym.id, sym.index_id, sym.ticker, bm.id, bm.short_name FROM symbol AS sym'
    #            'JOIN benchmark_index AS bm ON (sym.index_id = bm.id')
    
    cur.execute(command)
    data = cur.fetchall()
    cols = [
        'id', 
        'exchange_id', 
        'ticker', 
        'instrument_type', 
        'name', 
        'currency', 
        'created_date', 
        'last_updated_date'
    ]
    
    db_instruments_df = pd.DataFrame(
        data, 
        columns = cols
    )
    db_instruments_df.set_index('id', inplace = True)
    
    cur.close()
    con.close()
    
    return db_instruments_df



def get_db_price(instrument_id = None, price_date = None, include_ticker = False):
    
    con = pg.connect(database = 'securities_master', user = 'postgres')
    cur = con.cursor()
    
    cols = [
        'id', 
        'data_vendor_id', 
        'instrument_id', 
        'price_date', 
        'created_date', 
        'last_updated_date', 
        'open_price', 
        'high_price', 
        'low_price', 
        'close_price', 
        'adj_close_price', 
        'volume'
    ]
    
    if include_ticker: 
        ticker_join = ' JOIN instrument ON (daily_price.instrument_id = instrument.id)'
        instr_cols = [
            'inst_id', 
            'exchange_id', 
            'ticker',
            'instrument_type', 
            'name',
            'currency', 
            'inst_created_date', 
            'inst_last_update_date'
            
        ]
        cols.extend(instr_cols)
    else: 
        ticker_join = ''
        
    
    if (instrument_id == None) & (price_date == None): 
        command = 'SELECT * FROM daily_price{ticker_join}'
    elif (instrument_id != None) & (price_date == None):
        command = f'SELECT * FROM daily_price{ticker_join} WHERE instrument_id = {instrument_id}'
    elif (instrument_id == None) & (price_date != None):
        command = f'SELECT * FROM daily_price{ticker_join} WHERE price_date = \'{price_date}\''
    else:
        command = (f'SELECT * FROM daily_price{ticker_join} '
                   f'WHERE instrument_id = {instrument_id} AND price_date = \'{price_date}\'')
    
    cur.execute(command)
    data = cur.fetchall()
    
    db_prices_df = pd.DataFrame(
        data, 
        columns = cols
    )
    db_prices_df.set_index('id', inplace = True)
    if include_ticker:
        drop_cols = [
            'inst_id', 
            'exchange_id', 
            # 'ticker',
            'instrument_type', 
            'name',
            'currency', 
            'inst_created_date', 
            'inst_last_update_date'
        ]
        db_prices_df.drop(drop_cols, axis = 1, inplace = True)
    
    cur.close()
    con.close()
    
    return db_prices_df



def get_eod_bulk_price(ex, e_date = e_date):
    '''
    Parameters
    ----------
    ex : string : exchange (eg. US)

    Returns
    -------
    df : pandas dataframe 
    '''
    
    url = (
        f'http://eodhistoricaldata.com/api/eod-bulk-last-day/{ex}'
        f'?api_token={api}&fmt=json&date={e_date}'
    )

    response = requests.get(url)
    data = response.text
    bulk_data = pd.read_json(data)
    # bulk_data = json.loads(data)
    
    return bulk_data


def get_eod_constituents(index, s_date = '1990-01-01'):
    
    url = (f'https://eodhistoricaldata.com/api/fundamentals/{index}.INDX?'
           f'api_token={api}&historical=1&from={s_date}&to={e_date}')
        
    
    response = requests.get(url)
    data = response.text
    df = pd.read_json(data)
    
    general_info = df['General'].dropna()
    constituents = df['Components'].dropna()
    
    if constituents.shape[0] > 0:
        constituent_keys = list(constituents[0].keys())
        constituent_values = [list(i.values()) for i in constituents]
        constituents = pd.DataFrame.from_records(constituent_values, columns = constituent_keys)
        
    return constituents, general_info


def get_eod_corp_act(sec, ex, corp_act_type, s_date = '1900-01-01'):
    '''
    Parameters
    ----------
    sec : string : security (eg. AAPL)
    ex : string : exchange (eg. US)
    corp_act_type: type of corporate action ('div', 'splits', 'shorts')
    s_date : string : 'yyyy-mm-dd' format

    Returns
    -------
    df : pandas dataframe 
    '''
    valid_types = ['div', 'splits', 'shorts']
    
    if corp_act_type in valid_types:

        url = (f'https://eodhistoricaldata.com/api/{corp_act_type}/'
               f'{sec}.{ex}?api_token={api}&from={s_date}&fmt=json')

        response = requests.get(url)
        data = response.text
        df = pd.read_json(data).T
        df.set_index('date', inplace = True)
    
        return df

    else:
        print('Not a valid corporate action type.')
        

def get_eod_etf(sec, ex, s_date = '1900-01-01'):
    '''
    Parameters
    ----------
    sec : string : security (eg. AAPL)
    ex : string : exchange (eg. US)
    s_date : string : 'yyyy-mm-dd' format

    Returns
    -------
    df : pandas dataframe 
    '''
    
    url = (f'https://eodhistoricaldata.com/api/fundamentals/{sec}.{ex}?'
           f'api_token={api}&historical=1&from={s_date}&to={e_date}')
    
    response = requests.get(url)
    data = response.text
    df = pd.read_json(data)

    return df


def get_eod_exchanges(format = 'df'):
    
    valid_formats = ['json', 'df']
    if format in valid_formats: 
        
        url = f'https://eodhistoricaldata.com/api/exchanges-list/?api_token={api}&fmt=json'
        response = requests.get(url)
        data = response.text
        
        if format == 'json': 
            exchanges = json.loads(data)
        
        elif format == 'df': 
            exchanges = pd.read_json(data)

        return exchanges


def get_eod_fundamentals(sec, ex, s_date = '1900-01-01'):
    '''
    Parameters
    ----------
    sec : string : security (eg. AAPL)
    ex : string : exchange (eg. US)
    s_date : string : 'yyyy-mm-dd' format

    Returns
    -------
    fundamentals : dictionary object 
    '''

    url = (f'https://eodhistoricaldata.com/api/fundamentals/{sec}.{ex}?from={s_date}&to={e_date}'
           f'&api_token={api}&period=d&fmt=json')
    

    response = requests.get(url)
    data = response.text
    fundamentals = json.loads(data)
    
    return fundamentals


def get_eod_instruments(exchange = 'INDX', format = 'df'): 
    
    valid_formats = ['json', 'df']
    if format in valid_formats: 

        url = (
            f'https://eodhistoricaldata.com/api/exchange-symbol-list/{exchange}'
            f'?api_token={api}&fmt=json'
       )
        response = requests.get(url)
        data = response.text
        
        if format == 'json': 
            instruments = json.loads(data)
            
        elif format == 'df': 
            instruments = pd.read_json(data)
        
        return instruments


def get_eod_price(sec, ex, s_date = '1900-01-01'):
    '''
    Parameters
    ----------
    sec : string : security (eg. AAPL)
    ex : string : exchange (eg. US)
    s_date : string : 'yyyy-mm-dd' format

    Returns
    -------
    df : pandas dataframe 
    '''

    url = (f'https://eodhistoricaldata.com/api/eod/{sec}.{ex}?from={s_date}&to={e_date}'
           f'&api_token={api}&period=d&fmt=json')
    
    try:
        response = requests.get(url)
        data = response.text
        
        df = pd.read_json(data)
        if df.shape[0] > 0: 
            df.set_index('date', inplace = True)
        
        return df
    
    except: 
        error_log.append(['Error: get_eod_price', url])
        
        return pd.DataFrame()
    
    
def eod_bulk_prices_to_db(eod_bulk_prices_df, exch_id, data_vendor_id):
    
    con = pg.connect(database = 'securities_master', user = 'postgres')
    cur = con.cursor()
    
    command = (f'select instrument.id, instrument.ticker, exchange.code '
               f'from instrument join exchange on (instrument.exchange_id = exchange.id) '
               f'where exchange.id = {exch_id}')
    
    cur.execute(command)
    data = cur.fetchall()
    map_df = pd.DataFrame(
        data, 
        columns = ['id', 'ticker', 'exchange_code']
    )
    map_df.set_index('id', inplace = True)
    
    now = dt.datetime.now().strftime('%Y-%m-%d')
    cols = ('data_vendor_id, instrument_id, price_date, created_date, last_updated_date, '
            'open_price, high_price, low_price, close_price, adj_close_price, volume')
    
    for ind, row in eod_bulk_prices_df.iterrows():
        try:
            ticker = str(row['code'])
            # exchange = str(row['exchange_short_name'])
            instrument_id = map_df[map_df['ticker'] == ticker].index[0]
            price_date = row['date'].strftime('%Y-%m-%d')
            open_price = row['open']
            high_price = row['high']
            low_price = row['low']
            close_price = row['close']
            adj_close_price = row['adjusted_close']
            volume = row['volume']
            
            vals = (
                f"'{data_vendor_id}', '{instrument_id}', '{price_date}', "
                f"'{now}', '{now}', '{open_price}', "
                f"'{high_price}', '{low_price}', '{close_price}', "
                f"'{adj_close_price}', '{volume}'"
            )
            command = f'INSERT INTO daily_price ({cols}) VALUES ({vals})' 
            cur.execute(command)
        except:
            error_log.append(row)
    
    
    con.commit()
    cur.close()
    con.close()


def eod_constituents_to_db(eod_constituents_df, index_id):
    
    con = pg.connect(database = 'securities_master', user = 'postgres')
    cur = con.cursor()
    
    now = dt.datetime.now().strftime('%Y-%m-%d')
    command = (
        f'SELECT date FROM benchmark_index_member WHERE index_id = {index_id} '
        f'ORDER BY date DESC LIMIT 1'
    )
    cur.execute(command)
    data = cur.fetchall()
    
    if len(data) > 0: 
        last_date = data[0][0]
    else: 
        last_date = dt.date(1990, 1, 1)

    if last_date < dt.date.today():
        cols = 'index_id, ticker, exchange_code, date, last_update_date' 
    
        for ind, row in eod_constituents_df.iterrows(): 
           ticker = str(row['Code'])
           exchange_code = row['Exchange']
           vals = f"'{index_id}', '{ticker}', '{exchange_code}', '{now}', '{now}'"
           command = f'INSERT INTO benchmark_index_member ({cols}) VALUES ({vals})'
           cur.execute(command)     
    
    
    con.commit()
    cur.close()
    con.close()
    
    
def eod_exchanges_to_db(eod_exchanges_df, db_exchanges_df):
    
    con = pg.connect(database = 'securities_master', user = 'postgres')
    cur = con.cursor()
    
    now = dt.datetime.now().strftime('%Y-%m-%d')
    cols = 'code, name, short_name, country, currency, created_date, last_update_date' 

    new_exchanges = [ex for ex in eod_exchanges_df['Code'] if ex not in db_exchanges_df['code'].values]
    exchanges_df = eod_exchanges_df[eod_exchanges_df['Code'].isin(new_exchanges)]
    
    # upload new exchanges to 'exchange' table in SMDB
    for ind, row in exchanges_df.iterrows():
        code = str(row['Code'])
        name = str(row['Name']).replace("'", "")
        short_name = row['OperatingMIC']
        country = str(row['Country'])
        currency = str(row['Currency'])
        
        vals = f"'{code}', '{name}', '{short_name}', '{country}', '{currency}', '{now}', '{now}'"
        command = f'INSERT INTO exchange ({cols}) VALUES ({vals})' 
        cur.execute(command)
    
    con.commit()
    cur.close()
    con.close()
    
    
def eod_fundamentals_to_db(eod_fundamentals):
    
    # client = pm.MongoClient()
    # db = client['test']
    # result = db.fundamentals.insert_one(eod_fundamentals)
    # print('One post: {0}'.format(result.inserted_id))  ## confirmed in shell that this has worked
    
    print('Done')
    
    
def eod_index_to_db(info_df):
    
    con = pg.connect(database = 'securities_master', user = 'postgres')
    cur = con.cursor()
    
    code, name, country = str(info_df['Code']), str(info_df['Name']), str(info_df['CountryName'])
    now = dt.datetime.now().strftime('%Y-%m-%d')
    cols = 'short_name, name, country, created_date, last_updated_date' 
    vals = f"'{code}', '{name}', '{country}', '{now}', '{now}'"
    
    command = f'INSERT INTO benchmark_index ({cols}) VALUES ({vals})' 

    cur.execute(command)
    con.commit()
        
    cur.close()
    con.close()
    

def eod_instruments_to_db(eod_instruments_df, db_instruments_df, db_exchange_id):
    
    if eod_instruments_df.shape[0] > 0: 
        con = pg.connect(database = 'securities_master', user = 'postgres')
        cur = con.cursor()
        
        now = dt.datetime.now().strftime('%Y-%m-%d')
        cols = ('exchange_id, ticker, instrument_type, name, currency, created_date, last_update_date') 
    
        missing_tickers = [
            t for t in eod_instruments_df['Code'] if t not in db_instruments_df['ticker'].values
        ]
        instruments_df = eod_instruments_df[eod_instruments_df['Code'].isin(missing_tickers)]
        
        # upload new instruments to 'symbols' table in SMDB
       
        for ind, row in instruments_df.iterrows():
            ticker = str(row['Code']).replace("'", "\'\'")
            name = str(row['Name']).replace("'", "\'\'")
            currency = str(row['Currency'])
            instrument_type = str(row['Type'])
            vals = (
                f"'{db_exchange_id}', '{ticker}', '{instrument_type}', "
                f"'{name}', '{currency}', '{now}', '{now}'"
            )
            command = f'INSERT INTO instrument ({cols}) VALUES ({vals})' 
            cur.execute(command)
    
        
        con.commit()
        cur.close()
        con.close()


def eod_prices_to_db(eod_prices_df, db_prices_df, instrument_id, data_vendor_id):
    
    con = pg.connect(database = 'securities_master', user = 'postgres')
    cur = con.cursor()
    
    now = dt.datetime.now().strftime('%Y-%m-%d')
    cols = ('data_vendor_id, instrument_id, price_date, created_date, last_updated_date, '
            'open_price, high_price, low_price, close_price, adj_close_price, volume')

    # new_prices = [
    #     p.strftime('%Y-%m-%d') for p in eod_prices_df.index 
    #     if p not in db_prices_df['price_date'].values
    # ] 
    prices_df = eod_prices_df#[eod_prices_df.index.isin(new_prices)]
    
    # upload new exchanges to 'exchange' table in SMDB
    for ind, row in prices_df.iterrows():
        try:
            price_date = ind.strftime('%Y-%m-%d')
            open_price = row['open']
            high_price = row['high']
            low_price = row['low']
            close_price = row['close']
            adj_close_price = row['adjusted_close']
            volume = row['volume']
            
            vals = (
                f"'{data_vendor_id}', '{instrument_id}', '{price_date}', "
                f"'{now}', '{now}', '{open_price}', "
                f"'{high_price}', '{low_price}', '{close_price}', "
                f"'{adj_close_price}', '{volume}'"
            )
            command = f'INSERT INTO daily_price ({cols}) VALUES ({vals})' 
            cur.execute(command)
        except: 
            error_log.append(['Error: eod_prices_to_db', row])
    
    
    con.commit()
    cur.close()
    con.close()
    
    
def db_update_instruments():
    
    # adds new exchanges if missing from SMDB
    eod_exchanges_df = get_eod_exchanges()
    db_exchanges_df = get_db_exchanges()
    eod_exchanges_to_db(eod_exchanges_df, db_exchanges_df)
    
    
    # loop through every exchange in SMDB
    db_exchanges_df = get_db_exchanges() # get updated list
    
    x = 0
    for exch_id, exch_data in db_exchanges_df.iterrows(): 
        
        percent_done = round((x / db_exchanges_df.shape[0]) * 100, 2)
        print(f'Part 1: {percent_done}% complete. Working on Exchange: {exch_data["code"]}.')
        
        eod_instruments_df = get_eod_instruments(exch_data['code'])
        db_instruments_df = get_db_instruments(exch_id)
        eod_instruments_to_db(eod_instruments_df, db_instruments_df, exch_id)
        x += 1

        
    print('Part 1: 100% complete.')


    
def db_update_index_constituents():
    
    eod_indices_df = get_eod_instruments('INDX')
    db_indices_df = get_db_indices()
    
    for x, ind in enumerate(eod_indices_df['Code']):
        
        percent_done = round((x / len(eod_indices_df['Code'])) * 100, 2)
        print(f'Part 2: {percent_done}% complete. Working on Index: {ind}.')
        
        eod_constituents_df, info = get_eod_constituents(ind)
        
        # adds new indices if missing from SMDB
        if ind not in db_indices_df['short_name'].values:
            eod_index_to_db(info)
            db_indices_df = get_db_indices() # gets updated list
            
        # adds new symbols if missing from SMDB and records constituents per index
        if eod_constituents_df.shape[0] > 0:
            db_index_id = db_indices_df[db_indices_df['short_name'] == ind].index[0]
            eod_constituents_to_db(eod_constituents_df, db_index_id)
        
    print('Part 1: 100% complete.')
    
    
def db_update_prices(): 
    '''
    This is the function for importing price data that is more than just one day. For single
    date prices, see the `db_update_bulk_prices` function    
    

    The first loop iterates through the exchanges in the `exchange` tables.
    The second loop iterates through the instruments in `instrument` table for that exchange.
    
    There is no initial reference to the SMDB with this fucntion to check if price data 
    already exists.    

    '''
    db_exchanges_df = get_db_exchanges()
    
    # TEMP SOLUTION
    # db_exchanges_df.drop(1, inplace = True)
    # db_exchanges_df = db_exchanges_df.loc[8, :].to_frame().T # Testing data for XETRA Exchange
    # db_exchanges_df = db_exchanges_df.loc[2, :].to_frame().T # Same again for LSE listed companies
    # db_exchanges_df = db_exchanges_df.loc[68, :].to_frame().T # Same again for Indices
    # db_exchanges_df = db_exchanges_df.loc[75, :].to_frame().T # Same again for TSE listed companies
    # db_exchanges_df = db_exchanges_df.loc[64, :].to_frame().T # Same again for EUFUND
    db_exchanges_df = db_exchanges_df.loc[1, :].to_frame().T # Same again for US listed companies
    
    y = 0
    for exch_id, exch_data in db_exchanges_df.iterrows(): 
        db_instruments_df = get_db_instruments(exch_id)
        
        if db_exchanges_df.index == 64: # EUFUND
            db_fund_watchlist_df = get_db_fund_watchlist()
            db_instruments_df = db_instruments_df.loc[db_fund_watchlist_df['instrument_id'].values, :]
        
        
        percent_done = round(((y)/ db_exchanges_df.shape[0]) * 100, 2)
        print(f'Part ?: {percent_done}% complete. Working on prices for Exchange: {exch_data["code"]}.')
        
        last_price_list = []
        x = 0
        for instrument_id, instrument_data in db_instruments_df.iterrows():

            # db_prices_df = get_db_price(instrument_id)
            
            
            sub_percent_done = round((x / db_instruments_df.shape[0]) * 100, 2)
            print(f'\t{sub_percent_done}% complete. {instrument_data["ticker"]} uploading.')
            
            ## This is where using the SMDB index will be useful
            
            # if db_prices_df.shape[0] > 0: 
            #     last_db_date = db_prices_df['price_date'].sort_values(ascending = False).values[0]
            #     start_date = (last_db_date + dt.timedelta(1))
            # else: 
            #     start_date = dt.date(1900, 1, 1)
            
            start_date = dt.date(1900, 1, 1) ## TEMP SOLUTION
            
            if start_date < (dt.date.today() -  dt.timedelta(1)):
                ticker = instrument_data['ticker']
                exch_code = exch_data['code']
                eod_prices_df = get_eod_price(ticker, exch_code, start_date.strftime('%Y-%m-%d'))
                if eod_prices_df.shape[0] > 0: 
                    eod_prices_to_db(eod_prices_df, pd.DataFrame(),instrument_id, 1) ## TEMP SOLUTION
                    # eod_prices_to_db(eod_prices_df, db_prices_df, instrument_id, 1)
                    last_price_list.append(eod_prices_df.index[-1])
            x += 1
            
        y += 1
        db_update_last_updated_date('daily_price', min(last_price_list), exch_id)
        print('\t100% complete.')
    print('Part 3: 100% complete')
        
    
def db_update_last_updated_date(updated_table_name, last_date, exchange_id):
    
    valid_table_names = ['daily_price', 'fundamental']
    
    if updated_table_name not in valid_table_names:
        print('Table does not exist. No update was completed.')
        
    else:
        con = pg.connect(database = 'securities_master', user = 'postgres')
        cur = con.cursor()
        
        if updated_table_name == valid_table_names[0]:
            cols = 'last_price_update_date'
        elif updated_table_name == valid_table_names[1]:
            cols = 'last_fundamental_update_date'
        
        
        command = f'UPDATE exchange SET {cols} = \'{last_date}\' WHERE id = {exchange_id}'
        cur.execute(command)
        
        con.commit()
        cur.close()
        con.close()
        
        
def db_update_bulk_prices(exchange_list = None): 

    db_exchanges_df = get_db_exchanges()
    if exchange_list is not None: 
        db_exchanges_df = db_exchanges_df.loc[exchange_list, :]
    
    y = 0
    for exch_id, exch_data in db_exchanges_df.iterrows():
        
        # get `last_price_update_date` from `exchange`
        exch_code = exch_data['code']
        print(f'Part 3: Working on Exchange: {exch_code}')
        
        last_price_date = exch_data['last_price_update_date']
        if (last_price_date == '') | (last_price_date == None):
            print(
                f'No price data for {exch_code} exists in SMDB. '
                'Use different function to import prices.'
            )
            continue
        
        # use this date to get bulk prices
        end_date = dt.datetime.now().date() - dt.timedelta(1)
        exch_update_date = end_date
        db_prices_df = get_db_price(price_date = last_price_date, include_ticker = True)
        
        while end_date >= last_price_date: 
            str_date = end_date.strftime('%Y-%m-%d')
            print(f'\tWorking on date: {str_date}')
            
            # compare with existing prices to identify `adj_close_price` differences
            eod_bulk_prices_df = get_eod_bulk_price(exch_code, e_date = end_date)
            if (eod_bulk_prices_df.shape[0] > 0) & (end_date > last_price_date):
                eod_bulk_prices_to_db(eod_bulk_prices_df, exch_id, 1)
            
            if end_date == last_price_date:
                x = 0
                for _, instrument in eod_bulk_prices_df.iterrows():
                    
                    
                    ticker = instrument['code']
                    pct = round((x / eod_bulk_prices_df.shape[0] * 100), 2)
                    print(f'\t\t{pct}% through {exch_code}. Working on {ticker}.')
                    
                    eod_adj_close = instrument['adjusted_close']
                    db_adj_close = db_prices_df.loc[
                        db_prices_df['ticker'] == ticker, 'adj_close_price'
                    ]
                    db_id = db_prices_df.loc[db_prices_df['ticker'] == ticker, 'instrument_id']
                    if (len(db_adj_close.values) < 1) | (len(db_id.values) < 1):
                        error_log.append(
                            f'EOD price for {ticker} on {end_date} exists but not in DB. '
                            'db_update_bulk_prices'
                        )
                        continue
                    db_adj_close = float(db_adj_close.values[0])
                    db_id = db_id.values[0]
                    
                    # update adj_close_price where necessary
                    if (eod_adj_close - db_adj_close) > 0.00001:
                        # price_diff = round(eod_adj_close - db_adj_close, 4)
                        # db_update_adjusted_close(db_id, last_price_date, price_diff)
                        print('\t\tPrice issue. Reloading price history.')
                        error_log.append(
                            f'Issue with {ticker}.{exch_code}. '
                            'Reloaded full price history. db_update_bulk_prices'
                        )
                        remove_db_prices(db_id)
                        eod_prices_df = get_eod_price(ticker, exch_code)
                        eod_prices_to_db(eod_prices_df, pd.DataFrame(), db_id, 1)
                    x += 1
                print(f'\t\t100% complete with exchange {exch_code}')
                
            end_date = end_date - dt.timedelta(1)
        y += 1
        db_update_last_updated_date('daily_price', exch_update_date, exch_id)
    print('Part 3: 100% complete.')

def remove_db_prices(instrument_id):
    
    con = pg.connect(database = 'securities_master', user = 'postgres')
    cur = con.cursor()
    
    command = f'DELETE FROM daily_price WHERE instrument_id = {instrument_id}'

    cur.execute(command)
    
    con.commit()
    cur.close()
    con.close()
    

def db_update_adjusted_close(instrument_id, price_date, price_diff):
    
    con = pg.connect(database = 'securities_master', user = 'postgres')
    cur = con.cursor()
    
    command = (
        f'UPDATE daily_price SET adj_close_price = adj_close_price + {price_diff} '
        f'WHERE instrument_id = {instrument_id} AND price_date BETWEEN '
        f'\'1900-01-01\' AND \'{price_date}\''
    )
    
    cur.execute(command)
    
    con.commit()
    cur.close()
    con.close()
    
def db_update_fund_watchlist_data(exchange_id = 64): 
    '''
    The current watchlist length is less than 100 funds. The EOD bulk prices request uses 100
    of the 100,000 available API calls. Therefore, it makes more sense just to make individual 
    API calls for each fund, as long as the list is less than 100. 
    
    The process currently deletes the existing price history and updates the DB with a full
    price time series for each fund. As the list grows, it may be more efficient to 
    identify missing dates rather than completely removing and reloading each time series. 
    This depends on the speed the missing dates can be identified from the DB. 

    '''
    
    
    db_fund_watchlist_df = get_db_fund_watchlist()
    db_instruments_df = get_db_instruments(exchange_id)
    db_instruments_df = db_instruments_df.loc[db_fund_watchlist_df['instrument_id'], :]
    db_exchange_df = get_db_exchanges()
    
    x = 1
    for instrument_id in db_fund_watchlist_df['instrument_id']:
        print(f'Working on fund {x} of {db_fund_watchlist_df.shape[0]} funds in watchlist.')
        try:
            remove_db_prices(instrument_id)
            ticker = db_instruments_df.loc[instrument_id, 'ticker']
            exchange = db_exchange_df.loc[exchange_id, 'code']
            eod_prices_df = get_eod_price(ticker, exchange)
            eod_prices_to_db(eod_prices_df, pd.DataFrame(), instrument_id, 1)
        except:
            print('Could not process fund {ticker}')
        x += 1
    
    db_update_last_updated_date('daily_price', eod_prices_df.index[-1], exchange_id)
    print('100% complete.')
                    
    
def db_update_fundamentals(): 
    
    db_exchanges_df = get_db_exchanges()
    
    for exch_id, exch_data in db_exchanges_df.iterrows(): 
        db_instruments_df = get_db_instruments(exch_id)
        
        percent_done = round(((exch_id - 1)/ db_exchanges_df.shape[0]) * 100, 2)
        print(
            f'Part 4: {percent_done}% complete. '
            f'Working on fundamentals for Exchange: {exch_data["code"]}.'
        )
        
        for instrument_id, instrument_data in db_instruments_df.iterrows():
            # db_fundamentals_df = get_db_fundamentals(instrument_id)
            start_date = dt.date(1900, 1, 1)
            eod_fundamentals = get_eod_fundamentals(
                instrument_data['ticker'], 
                exch_data['code'], 
                start_date.strftime('%Y-%m-%d')
            )
            
            eod_fundamentals_to_db(eod_fundamentals)
            
            
def db_update_company_fundamentals(exchange_list = None): 
    
    db_exchange_df = get_db_exchanges()
    if exchange_list is not None: 
        db_exchange_df = db_exchange_df.loc[exchange_list, :]
    
    for exch_id, exch_data in db_exchange_df.iterrows(): 
        
        # filter instruments to only `Common Stock`
        db_instruments_df = get_db_instruments(exch_id)
        db_instruments_df = db_instruments_df[
            db_instruments_df['instrument_type'] == 'Common Stock'
        ]
        exch_code = exch_data['code']
        
        # call function to load info to SMDB
        # the check for existing data is performed within the below
        eod_company_fundamentals_to_db(db_instruments_df, exch_id, exch_code)
        
        db_update_last_updated_date('fundamental', e_date, exch_id)
        
            
    
def eod_company_fundamentals_to_db(db_instruments_df, exch_id, exch_code):
    
    con = pg.connect(database = 'securities_master', user = 'postgres')
    cur = con.cursor()
    now = dt.datetime.now().strftime('%Y-%m-%d')
    
    db_company_general_info_df = get_db_company_general_info()#exch_id)
    db_company_fundamentals_df = get_db_company_fundamentals()
    
    x = 0
    for inst_id, inst_data in db_instruments_df.iterrows():
        
        ticker = inst_data['ticker']
        pct = round(x / db_instruments_df.shape[0] * 100, 2)
        print(f'\t{pct}% complete. Uploading data for {ticker}.')
        
        try:
            eod_fundamentals_dict = get_eod_fundamentals(ticker, exch_code)
            
            eod_general_fields = [
                'ISIN', 
                'GicSector', 
                'GicGroup', 
                'GicIndustry',
                'GicSubIndustry',
                'City', 
                'Country', 
                'ZIP'
            ]
            
            eod_highlight_fields = [
                'MarketCapitalization',
                # 'MarketCapitalizationMln',
                'EBITDA',
                'PERatio',
                'PEGRatio',
                'WallStreetTargetPrice',
                'BookValue',
                'DividendShare',
                'DividendYield',
                'EarningsShare',
                'EPSEstimateCurrentYear',
                'EPSEstimateNextYear',
                'EPSEstimateCurrentQuarter',
                'EPSEstimateNextQuarter',
                'MostRecentQuarter',
                'ProfitMargin',
                'OperatingMarginTTM',
                'ReturnOnAssetsTTM',
                'ReturnOnEquityTTM',
                'RevenueTTM',
                'RevenuePerShareTTM',
                'QuarterlyRevenueGrowthYOY',
                'QuarterlyEarningsGrowthYOY',
                'GrossProfitTTM',
                'DilutedEpsTTM',
            ]
            
            eod_valuation_fields = [
                'TrailingPE', 
                'ForwardPE', 
                'PriceSalesTTM', 
                'PriceBookMRQ', 
                'EnterpriseValueRevenue', 
                'EnterpriseValueEbitda'
            ]
            
            eod_general_data = [inst_id]
            eod_fundamentals_data = [inst_id]
            
            for field in eod_general_fields:
                try:
                    value = eod_fundamentals_dict['General'][field]
                    if value == None:
                        eod_general_data.append('None')
                    else:
                        eod_general_data.append(value)
                except: 
                    eod_general_data.append('Unknown')
                    
            # then through the `highlight` and `valuation` data
            for field in eod_highlight_fields:
                try:
                    value = eod_fundamentals_dict['Highlights'][field]
                    if value == None:
                        eod_fundamentals_data.append(np.nan)
                    else:
                        eod_fundamentals_data.append(value)
                except: 
                    eod_fundamentals_data.append(np.nan)
                    
            for field in eod_valuation_fields:
                try:
                    value = eod_fundamentals_dict['Valuation'][field]
                    if value == None:
                        eod_fundamentals_data.append(np.nan)
                    else:
                        eod_fundamentals_data.append(value)
                except: 
                    eod_fundamentals_data.append(np.nan)
            
            # check the SMDB for existing general data
            db_general_data = db_company_general_info_df[
                db_company_general_info_df['instrument_id'] == inst_id
            ]
            
            if db_general_data.shape[0] > 0:
                db_general_data_last = db_general_data.iloc[-1, :].values[:-2].tolist()
            else:
                db_general_data_last = []
            
            if db_general_data_last != eod_general_data:
            
                cols = (
                    'instrument_id, isin, gic_sector, gic_group, gic_industry, '
                    'gic_subindustry, city, country, zip, created_date, last_update_date' 
                )
                vals = (
                    f"'{eod_general_data[0]}', '{eod_general_data[1]}', "
                    f"'{eod_general_data[2]}', '{eod_general_data[3]}', "
                    f"'{eod_general_data[4]}', '{eod_general_data[5]}', "
                    f"'{eod_general_data[6]}', '{eod_general_data[7]}', "
                    f"'{eod_general_data[8]}', '{now}', '{now}'"
                )
                command = f'INSERT INTO company_general_info ({cols}) VALUES ({vals})'
                cur.execute(command)
                
            # check the SMDB for existing highlight and valuation data
            db_fundamentals_data = db_company_fundamentals_df[
                db_company_fundamentals_df['instrument_id'] == inst_id
            ]
            
            if db_fundamentals_data.shape[0] > 0:
                db_fundamentals_data_last = db_fundamentals_data.iloc[-1, :].values[:-2].tolist()
            else:
                db_fundamentals_data_last = []
            
            if db_fundamentals_data_last != eod_fundamentals_data:
            
                cols = (
                    'instrument_id, market_cap, ebitda, pe_ratio, peg_ratio ,target_price, '
        	        'book_value, dps, div_yield, eps, eps_est_fy0, eps_est_fy1, eps_est_q0, '
        	        'eps_est_q1, mrq, prof_margin, op_margin, roa, roe, sales, sps, '
        	        'qtrly_sales_growth_yoy, qtrly_earnings_growth_yoy, gross_profit, '
        	        'dil_eps, trailing_pe_ratio, forward_pe_ratio, ps_ratio, pb_ratio, '
        	        'ev_sales, ev_ebitba, created_date, last_update_date'
                )
                vals = (
                    f"'{eod_fundamentals_data[0]}', '{eod_fundamentals_data[1]}', "
                    f"'{eod_fundamentals_data[2]}', '{eod_fundamentals_data[3]}', "
                    f"'{eod_fundamentals_data[4]}', '{eod_fundamentals_data[5]}', "
                    f"'{eod_fundamentals_data[6]}', '{eod_fundamentals_data[7]}', "
                    f"'{eod_fundamentals_data[8]}', '{eod_fundamentals_data[9]}', "
                    f"'{eod_fundamentals_data[10]}', '{eod_fundamentals_data[11]}', "
                    f"'{eod_fundamentals_data[12]}', '{eod_fundamentals_data[13]}', "
                    f"'{eod_fundamentals_data[14]}', '{eod_fundamentals_data[15]}', "
                    f"'{eod_fundamentals_data[16]}', '{eod_fundamentals_data[17]}', "
                    f"'{eod_fundamentals_data[18]}', '{eod_fundamentals_data[19]}', "
                    f"'{eod_fundamentals_data[20]}', '{eod_fundamentals_data[21]}', "
                    f"'{eod_fundamentals_data[22]}', '{eod_fundamentals_data[23]}', "
                    f"'{eod_fundamentals_data[24]}', '{eod_fundamentals_data[25]}', "
                    f"'{eod_fundamentals_data[26]}', '{eod_fundamentals_data[27]}', "
                    f"'{eod_fundamentals_data[28]}', '{eod_fundamentals_data[29]}', "
                    f"'{eod_fundamentals_data[30]}', '{now}', '{now}'"
                )
                vals = vals.replace('\'nan\'', 'NULL').replace('\'0000-00-00\'', 'NULL')
                command = f'INSERT INTO company_fundamentals ({cols}) VALUES ({vals})'
                cur.execute(command)
                x += 1
            
        except: 
            print(f'\tError getting fundamentals data for {ticker}.')
            error_log.append(
                f'Error getting fundamentals data for {ticker}. '
                f'Function: eod_company_general_info_to_db'
            )
        
    print('\t100% complete.')          
    con.commit()
        
    cur.close()
    con.close()    
    

def get_db_company_general_info(exch_id = None):
    
    con = pg.connect(database = 'securities_master', user = 'postgres')
    cur = con.cursor()
    
    if exch_id is not None: 
        exchange_sql = f' WHERE exchange_id = {exch_id}'
    else:
        exchange_sql = ''
    
    
    command = f'SELECT * FROM company_general_info{exchange_sql}'
    cur.execute(command)
    data = cur.fetchall()
    
    db_company_general_info_df = pd.DataFrame(
        data, 
        columns = [
            'id', 
            'instrument_id', 
            'isin', 
            'gic_sector', 
            'gic_group', 
            'gic_industry', 
            'gic_subindustry', 
            'city', 
            'country', 
            'zip', 
            'created_date', 
            'last_updated_date'
        ]
    )
    db_company_general_info_df.set_index('id', inplace = True)
    
    cur.close()
    con.close()
    
    return db_company_general_info_df


def get_db_company_fundamentals(exch_id = None):
    
    con = pg.connect(database = 'securities_master', user = 'postgres')
    cur = con.cursor()
    
    if exch_id is not None: 
        exchange_sql = f' WHERE exchange_id = {exch_id}'
    else:
        exchange_sql = ''
    
    
    command = f'SELECT * FROM company_fundamentals{exchange_sql}'
    cur.execute(command)
    data = cur.fetchall()
    
    db_company_fundamentals_df = pd.DataFrame(
        data, 
        columns = [
            'id', 
            'instrument_id', 
        	'market_cap', 
        	'ebitda', 
        	'pe_ratio',
        	'peg_ratio',
        	'target_price',
        	'book_value', 
        	'dps',
        	'div_yield', 
        	'eps', 
        	'eps_est_fy0',
        	'eps_est_fy1', 
        	'eps_est_q0', 
        	'eps_est_q1', 
        	'mrq', 
        	'prof_margin', 
        	'op_margin', 
        	'roa', 
        	'roe', 
        	'sales', 
        	'sps', 
        	'qtrly_sales_growth_yoy', 
        	'qtrly_earnings_growth_yoy',
        	'gross_profit', 
        	'dil_eps', 
        	'trailing_pe_ratio', 
        	'forward_pe_ratio', 
        	'ps_ratio', 
        	'pb_ratio', 
        	'ev_sales', 
        	'ev_ebitba',
        	'created_date', 
        	'last_update_date' 
        ]
    )
    db_company_fundamentals_df.set_index('id', inplace = True)
    
    cur.close()
    con.close()
    
    return db_company_fundamentals_df
    
            
def error_log_file_creation():
    
    if len(error_log) > 0:
        
        path = 'error_logs'
        now = dt.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        
        if not os.path.exists(path):
            os.makedirs(path)
        
        filename = now + '.txt'
        with open(os.path.join(path, filename), 'w') as f:
            f.write('\n'.join(str(error_log)))
    

if __name__ == '__main__':
    
    db_update_instruments()
    db_update_index_constituents()
    db_update_bulk_prices(exchange_list = [1, 2, 8, 68])
    db_update_fund_watchlist_data()
    # db_update_company_fundamentals(exchange_list = [1, 2, 8])


    
    # db_update_prices()
            
    
    
    error_log_file_creation()
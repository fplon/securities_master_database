import pandas as pd
import psycopg2 as pg
import datetime as dt
import os

def get_ds_constituents(file):
    
    filepath = os.getcwd() + '/Historic_Constituents/' + file
    raw_excel = pd.read_excel(filepath)
    
    master_df = pd.DataFrame()
    
    for x in range(0, raw_excel.shape[1] + 1, 5):
        # look at constituents one date at a time 
        current_df = raw_excel.iloc[:, x:x+4]
        date = current_df.columns[0]
        current_df.columns = current_df.loc[0].values
        current_df.drop(0, inplace = True)
        current_df.index = [date] * current_df.shape[0]
        if current_df['NAME'].isna().sum() != current_df.shape[0]:
            master_df = pd.concat([master_df, current_df.astype(str)])
    
    master_df.dropna(axis = 0, how = 'all', inplace = True)
    master_df.rename(columns = {'Type': 'DS CODE'}, inplace = True)
    
    return master_df

def ds_constituents_to_db(ds_constituents_df, index_id):
    
    con = pg.connect(database = 'securities_master', user = 'postgres')
    cur = con.cursor()
    
    now = dt.datetime.now().strftime('%Y-%m-%d')
    cols = (
        'index_id, ds_code, name, isin_code, sedol_code, '
        'index_date, created_date, last_update_date' 
    )
    
    # upload DS constituents to 'datastream_benchmark_index_members' table in SMDB
    for date, const_data in ds_constituents_df.iterrows():
        ds_code = str(const_data['DS CODE'])
        name = str(const_data['NAME']).replace('\'', '\'\'')
        isin = str(const_data['ISIN CODE'])
        sedol = str(const_data['SEDOL CODE'])
        
        vals = (
            f"'{index_id}', '{ds_code}', '{name}', '{isin}', '{sedol}', "
            f"'{date}', '{now}', '{now}'"
        )
        command = f'INSERT INTO datastream_benchmark_index_member ({cols}) VALUES ({vals})' 
        try:
            cur.execute(command)
        except: 
            print(f'\t{name} failed to upload.')
    
    con.commit()
    cur.close()
    con.close()
    
    
def db_update_ds_constituents():
    
    for file in os.listdir('Historic_Constituents'): 
        index_id = file.split('_')[0]
        if (file[-5:] == '.xlsx') & (file[0] != '~'):
            try: 
                ds_constituents_df = get_ds_constituents(file)
                ds_constituents_to_db(ds_constituents_df, index_id)
                print(f'{file} uploaded to DB')
            except:
                print(f'{file} upload failed. ')
    

if __name__ == '__main__':
    
    db_update_ds_constituents()
import pandas as pd
import numpy as np
import os
from glob import glob

base_path = '../Data/'
other_csvs_path = '../Data/Other_csvs/'
stocks_data_path = base_path + 'Stock_Data/Csvs/'
os.listdir(base_path)

gl_file = other_csvs_path + [x for x in os.listdir(other_csvs_path) if x.lower().startswith('gl')][0]
hl_file = other_csvs_path + [x for x in os.listdir(other_csvs_path) if x.lower().startswith('hl')][0]
mcap_file = other_csvs_path + [x for x in os.listdir(other_csvs_path) if x.lower().startswith('mcap')][0]
pe_file = other_csvs_path + [x for x in os.listdir(other_csvs_path) if x.lower().startswith('pe')][0]

indices_to_include = ['India VIX', 'Nifty 50', 'NIFTY MIDSML 400', 'Nifty 500', 'Nifty IT',
                      'Nifty Bank', 'Nifty Realty', 'Nifty Infra', 'Nifty Energy', 'Nifty FMCG',
                      'Nifty Pharma', 'Nifty PSE', 'Nifty PSU Bank',
                      'Nifty Auto', 'Nifty Metal', 'Nifty Media']


def loading_clean():
    global gl_file, hl_file, mcap_file,  pe_file

    # --- 1. PD FILES — stack all 245 days ---------------------------
    pd_files = glob(stocks_data_path + 'Pd*.csv')

    dfs = []
    for file in pd_files:
        df = pd.read_csv(file)
        fname = os.path.basename(file)
        date_str = fname[2:8]
        df['date'] = pd.to_datetime(date_str, format='%d%m%y')
        dfs.append(df)

    pd_all = pd.concat(dfs, ignore_index=True)
    pd_all.columns = pd_all.columns.str.strip().str.lower().str.replace(' ', '_')

    stocks_df = pd_all[pd_all['series'].str.strip() == 'EQ'].copy()
    index_df  = pd_all[(pd_all['ind_sec'] == 'Y') & (pd_all['series'] == ' ')].copy()
    index_df  = index_df[index_df['security'].isin(indices_to_include)]

    num_cols = ['prev_cl_pr','open_price','high_price','low_price',
                'close_price','net_trdval','net_trdqty','hi_52_wk','lo_52_wk']
    for col in num_cols:
        stocks_df[col] = pd.to_numeric(stocks_df[col], errors='coerce')
        index_df[col]  = pd.to_numeric(index_df[col],  errors='coerce')

    print(f'Stocks: {stocks_df.shape}')
    print(f'Indices: {index_df.shape}')

    # --- 2. GL FILE ---------------------------
    gl_df = pd.read_csv(gl_file)
    gl_df.columns = gl_df.columns.str.strip().str.lower()
    gl_df = gl_df[gl_df['gain_loss'].isin(['G', 'L'])].copy()
    gl_df = gl_df.drop_duplicates()
    print(f'GL: {gl_df.shape}')

    # --- 3. HL FILE ---------------------------
    hl_df = pd.read_csv(hl_file)
    hl_df.columns = hl_df.columns.str.strip().str.lower()
    hl_df = hl_df.drop_duplicates()
    print(f'HL: {hl_df.shape}')

    # --- 4. MCAP FILE ---------------------------
    mcap_df = pd.read_csv(mcap_file)
    mcap_df.columns = mcap_df.columns.str.strip().str.lower().str.replace(' ','_')
    mcap_df = mcap_df[mcap_df['series'].str.strip() == 'EQ'].copy()
    mcap_df = mcap_df.rename(columns={'market_cap(rs.)': 'mcap'})
    print(f'MCAP: {mcap_df.shape}')

    # --- 5. PE FILE ---------------------------
    pe_df = pd.read_csv(pe_file)
    pe_df.columns = pe_df.columns.str.strip().str.lower().str.replace(' ', '_')
    print(f'PE: {pe_df.shape}')

    return stocks_df, index_df, gl_df, hl_df, mcap_df, pe_df

stocks_df, index_df, gl_df, hl_df, mcap_df, pe_df = loading_clean()


def merge_and_clean_stocks():
    global stocks_df, mcap_df, pe_df, index_df, gl_df, hl_df

    stocks_df = pd.merge(stocks_df, pe_df[['symbol','adjusted_p/e']], on='symbol', how='left')
    stocks_df = pd.merge(stocks_df, mcap_df[['symbol', 'mcap']], on='symbol', how='left')

    top_1000_stocks = mcap_df.sort_values('mcap', ascending=False).reset_index(drop=True).head(1000)
    stocks_df = stocks_df[stocks_df['symbol'].isin(top_1000_stocks['symbol'])]
    stocks_df = stocks_df[['date','symbol','security','prev_cl_pr','close_price','open_price',
                            'high_price','low_price','hi_52_wk','lo_52_wk','adjusted_p/e','mcap','net_trdval']]
    stocks_df = stocks_df.sort_values('mcap', ascending=False).reset_index(drop=True)
    stocks_df['ma_20']  = stocks_df.groupby('symbol')['close_price'].transform(lambda x: x.rolling(20).mean())
    stocks_df['ma_50']  = stocks_df.groupby('symbol')['close_price'].transform(lambda x: x.rolling(50).mean())
    stocks_df['ma_100'] = stocks_df.groupby('symbol')['close_price'].transform(lambda x: x.rolling(100).mean())
    stocks_df['ma_200'] = stocks_df.groupby('symbol')['close_price'].transform(lambda x: x.rolling(200).mean())

    index_df = index_df[['date','security','prev_cl_pr','close_price','open_price',
                          'high_price','low_price','hi_52_wk','lo_52_wk']].reset_index(drop=True)
    index_df['ma_20']  = index_df.groupby('security')['close_price'].transform(lambda x: x.rolling(20).mean())
    index_df['ma_50']  = index_df.groupby('security')['close_price'].transform(lambda x: x.rolling(50).mean())
    index_df['ma_100'] = index_df.groupby('security')['close_price'].transform(lambda x: x.rolling(100).mean())
    index_df['ma_200'] = index_df.groupby('security')['close_price'].transform(lambda x: x.rolling(200).mean())
    
    #....filter GL and HL to top 1000 stocks of stocks_df only...#
    
    valid_securities = stocks_df['security'].str.strip().unique().tolist()
    gl_df['security'] = gl_df['security'].str.strip()
    gl_df = gl_df[gl_df['security'].isin(valid_securities)]
    print(f'GL filtered: {gl_df.shape}')
    
    hl_df['security'] = hl_df['security'].str.strip()
    hl_df = hl_df[hl_df['security'].isin(valid_securities)]
    print(f'NH filtered: {hl_df.shape}')
    
    
        # --- most and least volatile index today ---------------------------
    exclude = ['India VIX', 'Nifty 50', 'NIFTY MIDSML 400', 'Nifty 500']
    latest_idx = index_df[index_df['date'] == index_df['date'].max()].copy()
    latest_idx = latest_idx[~latest_idx['security'].isin(exclude)]
    latest_idx['adr'] = ((latest_idx['high_price'] - latest_idx['low_price']) / latest_idx['close_price'])
    most_volatile  = latest_idx.nlargest(1, 'adr')[['security', 'adr']].reset_index(drop=True)
    least_volatile = latest_idx.nsmallest(1, 'adr')[['security', 'adr']].reset_index(drop=True)
    volatility_df = pd.DataFrame({
        'most_volatile_name': [most_volatile['security'].iloc[0]],
        'most_volatile_adr':  [round(most_volatile['adr'].iloc[0], 5)],
        'least_volatile_name': [least_volatile['security'].iloc[0]],
        'least_volatile_adr':  [round(least_volatile['adr'].iloc[0], 5)]
    })
    volatility_df.to_csv('../DATA/volatility.csv', index=False)
    print('volatility.csv saved')
    

merge_and_clean_stocks()


def create_index_pct():
    global index_df
    temp = index_df.copy()
    temp['pct_change'] = ((temp['close_price'] - temp['prev_cl_pr']) / temp['prev_cl_pr'])
    index_pct = temp.pivot_table(index='date', columns='security', values='pct_change')
    index_pct.columns.name = None
    index_pct = index_pct.reset_index()

    # --- latest row ---------------------------
    index_latest = index_pct[index_pct['date'] == index_pct['date'].max()].copy()

    # --- for each index col, add _val numeric and keep original as string ---------------------------
    index_cols = [c for c in index_latest.columns if c != 'date']
    for col in index_cols:
        index_latest[f'{col}_val'] = index_latest[col]
        index_latest[col] = index_latest[col].apply(
            lambda x: f'+{round(x*100, 2)} % ▲' if x > 0 else f'{round(x*100, 2)} % ▼'
        )

    index_latest.to_csv('../DATA/index_latest.csv', index=False)
    return (index_pct, index_latest)


index_pct, index_latest = create_index_pct()

stocks_df.to_csv('../DATA/stocks_data.csv', index=False)
index_df.to_csv('../DATA/indices.csv', index=False)
index_pct.to_csv('../DATA/index_pct.csv', index=False)
index_latest.to_csv('../DATA/index_latest.csv', index=False)
gl_df.to_csv('../DATA/gainers_losers.csv', index=False)
hl_df.to_csv('../DATA/new_highs.csv', index=False)

print('All files saved.')
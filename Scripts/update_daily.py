import pandas as pd
import numpy as np
import requests
import zipfile
import os
from datetime import date, timedelta

# --- PATHS ---
other_csvs_path  = '../Data/Other_csvs/'
stocks_csvs_path = '../Data/Stock_Data/Csvs/'
output_path      = '../DATA/'

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.nseindia.com/",
    "Accept": "*/*",
}

indices_to_include = ['India VIX', 'Nifty 50', 'NIFTY MIDSML 400', 'Nifty 500', 'Nifty IT',
                      'Nifty Bank', 'Nifty Realty', 'Nifty Infra', 'Nifty Energy', 'Nifty FMCG',
                      'Nifty Pharma', 'Nifty PSE', 'Nifty PSU Bank',
                      'Nifty Auto', 'Nifty Metal', 'Nifty Media']


def download_today():
    target_date  = date.today()
    max_attempts = 10

    for _ in range(max_attempts):
        day   = str(target_date.day).zfill(2)
        month = str(target_date.month).zfill(2)
        year2 = str(target_date.year)[-2:]
        year4 = str(target_date.year)

        if target_date.weekday() >= 5:
            print(f'Skipping — weekend')
            target_date -= timedelta(days=1)
            continue

        attempts = [
            (f'{day}{month}{year2}', f"https://nsearchives.nseindia.com/archives/equities/bhavcopy/pr/PR{day}{month}{year2}.zip"),
            (f'{day}{month}{year4}', f"https://nsearchives.nseindia.com/archives/equities/bhavcopy/pr/PR{day}{month}{year4}.zip"),
        ]

        found = False
        for date_str, url in attempts:
            print(f'Trying {url}')
            response = requests.get(url=url, headers=headers)

            if response.status_code == 200:
                
                # --- delete old GL, HL, PE files ---------------------------
                for f in os.listdir(other_csvs_path):
                    if f.lower().startswith(('gl', 'hl', 'pe', 'mcap')):
                        os.remove(os.path.join(other_csvs_path, f))
                        print(f'Deleted old file: {f}')
                
                
                print(f'Found: PR{date_str}.zip')

                zip_path = f'../Data/Stock_Data/Zips/PR{date_str}.zip'
                os.makedirs(os.path.dirname(zip_path), exist_ok=True)
                with open(zip_path, 'wb') as f:
                    f.write(response.content)

                with zipfile.ZipFile(zip_path, 'r') as z:
                    for file in z.namelist():
                        if file.lower().startswith('pd'):
                            new_name = file.capitalize()
                            if len(file) > 12:
                                new_name = new_name[:6] + new_name[8:]
                            with z.open(file) as source:
                                with open(os.path.join(stocks_csvs_path, new_name), 'wb') as target:
                                    target.write(source.read())
                            print(f'Extracted as: {new_name}')
                        elif file.lower()[:2] in ('gl', 'hl', 'mc'):
                            z.extract(file, other_csvs_path)
                            print(f'Extracted: {file}')

                # --- download PE file ---
                pe_url  = f'https://nsearchives.nseindia.com/content/equities/peDetail/PE_{date_str}.csv'
                pe_path = other_csvs_path + f'PE_{date_str}.csv'
                pe_response = requests.get(pe_url, headers=headers)
                if pe_response.status_code == 200:
                    with open(pe_path, 'wb') as f:
                        f.write(pe_response.content)
                    print(f'Saved: PE_{date_str}.csv')
                    

                else:
                    print(f'PE not available for {date_str} — status {pe_response.status_code}')

                found = True
                return date_str

        if not found:
            print(f'Skipped {day}{month} — holiday/unavailable')
            target_date -= timedelta(days=1)

    print('No trading day found in last 10 days.')
    return None


def load_mcap():
    mcap_file = other_csvs_path + [x for x in os.listdir(other_csvs_path) if x.lower().startswith('mcap')][0]
    mcap_df = pd.read_csv(mcap_file)
    mcap_df.columns = mcap_df.columns.str.strip().str.lower().str.replace(' ', '_')
    mcap_df = mcap_df[mcap_df['series'].str.strip() == 'EQ'].copy()
    mcap_df = mcap_df.rename(columns={'market_cap(rs.)': 'mcap'})
    mcap_df['symbol'] = mcap_df['symbol'].str.strip()
    return mcap_df[['symbol', 'mcap']]


def load_pe():
    pe_file = other_csvs_path + [x for x in os.listdir(other_csvs_path) if x.lower().startswith('pe')][0]
    pe_df = pd.read_csv(pe_file)
    pe_df.columns = pe_df.columns.str.strip().str.lower().str.replace(' ', '_')
    pe_df['symbol'] = pe_df['symbol'].str.strip()
    return pe_df[['symbol', 'adjusted_p/e']]


def read_pd_file(date_str):
    date_str_long = date_str[:4] + '20' + date_str[4:]

    if os.path.exists(stocks_csvs_path + f'Pd{date_str_long}.csv'):
        pd_file = stocks_csvs_path + f'Pd{date_str_long}.csv'
    else:
        pd_file = stocks_csvs_path + f'Pd{date_str}.csv'

    print(f'Reading: {pd_file}')
    df = pd.read_csv(pd_file)
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    df['date'] = pd.to_datetime(date_str[:6], format='%d%m%y').strftime('%d-%m-%Y')

    num_cols = ['prev_cl_pr', 'open_price', 'high_price', 'low_price',
                'close_price', 'net_trdval', 'net_trdqty', 'hi_52_wk', 'lo_52_wk']
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


def calc_mas(df, group_col):
    df = df.sort_values([group_col, 'date']).reset_index(drop=True)
    for window in [20, 50, 100, 200]:
        df[f'ma_{window}'] = df.groupby(group_col)['close_price'].transform(
            lambda x: x.rolling(window).mean()
        )
    return df


def update_stocks(date_str):
    df = read_pd_file(date_str)

    df = df[df['ind_sec'] == 'N'].copy()
    df = df[df['series'].str.strip() == 'EQ'].copy()
    df['symbol'] = df['symbol'].str.strip()

    pe_df   = load_pe()
    mcap_df = load_mcap()
    df = pd.merge(df, pe_df,   on='symbol', how='left')
    df = pd.merge(df, mcap_df, on='symbol', how='left')

    existing = pd.read_csv(output_path + 'stocks_data.csv')
    top_1000_symbols = existing['symbol'].unique().tolist()
    df = df[df['symbol'].isin(top_1000_symbols)]

    df = df[['date', 'symbol', 'security', 'prev_cl_pr', 'close_price', 'open_price',
             'high_price', 'low_price', 'hi_52_wk', 'lo_52_wk', 'adjusted_p/e', 'mcap', 'net_trdval']]

    updated = pd.concat([existing, df], ignore_index=True)
    updated.drop_duplicates(subset=['date', 'symbol'], keep='last', inplace=True)
    updated = calc_mas(updated, 'symbol')
    updated = updated.sort_values('mcap', ascending=False).reset_index(drop=True)
    updated.to_csv(output_path + 'stocks_data.csv', index=False)
    print(f'stocks_data.csv updated — {len(df)} new rows added')


def update_indices(date_str):
    df = read_pd_file(date_str)

    df = df[(df['ind_sec'] == 'Y') & (df['series'] == ' ')].copy()
    df = df[df['security'].isin(indices_to_include)]

    df = df[['date', 'security', 'prev_cl_pr', 'close_price', 'open_price',
             'high_price', 'low_price', 'hi_52_wk', 'lo_52_wk']].reset_index(drop=True)

    existing = pd.read_csv(output_path + 'indices.csv')
    updated = pd.concat([existing, df], ignore_index=True)
    updated.drop_duplicates(subset=['date', 'security'], keep='last', inplace=True)
    updated = calc_mas(updated, 'security')
    updated.to_csv(output_path + 'indices.csv', index=False)
    print(f'indices.csv updated — {len(df)} new rows added')
    
    # --- update volatility ---------------------------
    
    exclude = ['India VIX', 'Nifty 50', 'NIFTY MIDSML 400', 'Nifty 500']
    existing_idx = pd.read_csv(output_path + 'indices.csv')
    latest_idx = existing_idx[existing_idx['date'] == existing_idx['date'].max()].copy()
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
    volatility_df.to_csv(output_path + 'volatility.csv', index=False)
    print(f'volatility.csv updated')


def update_index_pct(date_str):
    df = read_pd_file(date_str)
    df = df[(df['ind_sec'] == 'Y') & (df['series'] == ' ')].copy()
    df = df[df['security'].isin(indices_to_include)]
    df['pct_change'] = ((df['close_price'] - df['prev_cl_pr']) / df['prev_cl_pr'])

    today_pivot = df.pivot_table(index='date', columns='security', values='pct_change')
    today_pivot.columns.name = None
    today_pivot = today_pivot.reset_index()

    existing = pd.read_csv(output_path + 'index_pct.csv')
    updated = pd.concat([existing, today_pivot], ignore_index=True)
    updated.drop_duplicates(subset=['date'], keep='last', inplace=True)
    updated.to_csv(output_path + 'index_pct.csv', index=False)
    print(f'index_pct.csv updated')

    # --- latest row with string + _val cols ---------------------------
    index_latest = updated[updated['date'] == updated['date'].max()].copy()
    index_cols = [c for c in index_latest.columns if c != 'date']
    for col in index_cols:
        index_latest[f'{col}_val'] = index_latest[col]
        index_latest[col] = index_latest[col].apply(
            lambda x: f'+{round(x*100, 2)} % ▲' if x > 0 else f'{round(x*100, 2)} % ▼'
        )


    
    index_latest.to_csv(output_path + 'index_latest.csv', index=False)
    print(f'index_latest.csv updated')


def update_gl_hl():
    gl_file = other_csvs_path + [x for x in os.listdir(other_csvs_path) if x.lower().startswith('gl')][0]
    hl_file = other_csvs_path + [x for x in os.listdir(other_csvs_path) if x.lower().startswith('hl')][0]

    # --- load valid securities from stocks_data ---------------------------
    existing_stocks = pd.read_csv(output_path + 'stocks_data.csv')
    valid_securities = existing_stocks['security'].str.strip().unique().tolist()

    gl_df = pd.read_csv(gl_file)
    gl_df.columns = gl_df.columns.str.strip().str.lower()
    gl_df['security'] = gl_df['security'].str.strip()
    gl_df = gl_df[gl_df['gain_loss'].isin(['G', 'L'])].copy()
    gl_df = gl_df[gl_df['security'].isin(valid_securities)]
    gl_df.to_csv(output_path + 'gainers_losers.csv', index=False)
    gl_df = gl_df.drop_duplicates()
    
    print(f'gainers_losers.csv updated — {len(gl_df)} rows')

    hl_df = pd.read_csv(hl_file)
    hl_df.columns = hl_df.columns.str.strip().str.lower()
    hl_df['security'] = hl_df['security'].str.strip()
    hl_df = hl_df[hl_df['security'].isin(valid_securities)]
    hl_df.to_csv(output_path + 'new_highs.csv', index=False)
    hl_df = hl_df.drop_duplicates()
    print(f'new_highs.csv updated — {len(hl_df)} rows')

# --- RUN ---
date_str = download_today()
update_stocks(date_str)
update_indices(date_str)
update_index_pct(date_str)
update_gl_hl()
print('Daily update complete.')
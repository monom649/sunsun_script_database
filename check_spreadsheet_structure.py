#!/usr/bin/env python3
"""
失敗したスプレッドシートの実際の構造を確認
"""

import requests
import pandas as pd
import io
import re

def get_csv_url_from_gid(spreadsheet_id, gid):
    """spreadsheet_idとgidからCSV URLを生成"""
    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"

def extract_gid_from_url(url):
    """URLからgidを抽出"""
    match = re.search(r'gid=(\d+)', url)
    return match.group(1) if match else None

def check_spreadsheet_structure(name, url, max_rows=15, max_cols=10):
    """スプレッドシートの構造を確認"""
    print(f"\n{'='*50}")
    print(f"スクリプト: {name}")
    print(f"URL: {url}")
    print(f"{'='*50}")
    
    try:
        # spreadsheet_id抽出
        spreadsheet_match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', url)
        if not spreadsheet_match:
            print("❌ URL解析失敗")
            return
        
        spreadsheet_id = spreadsheet_match.group(1)
        gid = extract_gid_from_url(url)
        
        if not gid:
            print("❌ gid抽出失敗")
            return
        
        csv_url = get_csv_url_from_gid(spreadsheet_id, gid)
        
        # CSV取得
        response = requests.get(csv_url, timeout=30)
        if response.status_code != 200:
            print(f"❌ HTTP {response.status_code}")
            return
        
        # DataFrame作成
        csv_data = io.StringIO(response.text)
        df = pd.read_csv(csv_data, header=None)
        
        if df.empty:
            print("❌ 空データ")
            return
        
        print(f"データフレームサイズ: {len(df)}行 x {len(df.columns)}列")
        print(f"\n最初の{max_rows}行, {max_cols}列:")
        
        # 最初の数行・数列を表示
        display_df = df.iloc[:max_rows, :max_cols]
        
        for row_idx in range(len(display_df)):
            row_data = []
            for col_idx in range(len(display_df.columns)):
                if col_idx < len(display_df.columns):
                    cell_value = display_df.iloc[row_idx, col_idx]
                    if pd.notna(cell_value):
                        cell_str = str(cell_value).strip()
                        if len(cell_str) > 20:
                            cell_str = cell_str[:20] + "..."
                        row_data.append(f'"{cell_str}"')
                    else:
                        row_data.append('""')
                else:
                    row_data.append('""')
            print(f"行{row_idx}: [{', '.join(row_data)}]")
        
        # キャラクター・セリフ列の検索
        print(f"\n「キャラクター」「セリフ」を含むセルの検索:")
        for row_idx in range(min(20, len(df))):
            for col_idx in range(min(15, len(df.columns))):
                cell_value = df.iloc[row_idx, col_idx]
                if pd.notna(cell_value):
                    cell_str = str(cell_value).strip().lower()
                    if 'キャラクター' in cell_str or 'キャラ' == cell_str or 'セリフ' in cell_str or 'せりふ' in cell_str:
                        print(f"  行{row_idx}, 列{col_idx}: \"{str(df.iloc[row_idx, col_idx]).strip()}\"")
        
    except Exception as e:
        print(f"❌ エラー: {e}")

# チェック対象のスプレッドシート（最初の数個）
test_scripts = [
    ("B1029", "https://docs.google.com/spreadsheets/d/120ypuflQ2s1fggX7BcDIUyEjCqAQf0s1yuR54skt5ws/edit?gid=1384097767#gid=1384097767"),
    ("B1042", "https://docs.google.com/spreadsheets/d/1Qd4eTnFgaSLNxAoowBuZoMnj00WdrZcn3Mc9UjDA_k4/edit?gid=1384097767#gid=1384097767"),
    ("B687", "https://docs.google.com/spreadsheets/d/1LGcCPd9R5j8E4nRQnY5XdOujHbg5b_rlvWzTYU9hyPQ/edit?gid=1384097767#gid=1384097767"),
    ("B2147", "https://docs.google.com/spreadsheets/d/1d3l0tPmIOx5rPPJPyuX8DDmeEHI-E0M-LkCuyfKaV3w/edit?gid=1115519680#gid=1115519680")
]

for name, url in test_scripts:
    check_spreadsheet_structure(name, url)
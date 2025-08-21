#!/usr/bin/env python3
"""
B692スプレッドシートの「とんかつ」検索
"""

import requests
import pandas as pd
import io
import re

def check_b692():
    # B692のスプレッドシートURL
    spreadsheet_id = "1Da9LPkx1jCc89OO6zSy6pb2SnCnA4hYN2HDk7zPDdfs"
    gid = "1384097767"  # 台本タブ
    
    csv_url = f"https://docs.google.com/spreadsheets/d/1Da9LPkx1jCc89OO6zSy6pb2SnCnA4hYN2HDk7zPDdfs/export?format=csv&gid=1384097767"
    
    try:
        print("B692スプレッドシート内容確認...")
        response = requests.get(csv_url, timeout=30)
        
        if response.status_code != 200:
            print(f"❌ HTTP {response.status_code}")
            return
        
        csv_data = io.StringIO(response.text)
        df = pd.read_csv(csv_data, header=None)
        
        print(f"データサイズ: {len(df)}行 x {len(df.columns)}列")
        
        # 「とんかつ」「エビフライ」を含むセルを検索
        print("\n「とんかつ」「エビフライ」検索結果:")
        found_count = 0
        
        for row_idx in range(len(df)):
            for col_idx in range(len(df.columns)):
                cell_value = df.iloc[row_idx, col_idx]
                if pd.notna(cell_value):
                    cell_str = str(cell_value)
                    if 'とんかつ' in cell_str or 'エビフライ' in cell_str:
                        print(f"  行{row_idx}, 列{col_idx}: \"{cell_str}\"")
                        found_count += 1
        
        if found_count == 0:
            print("  該当なし")
        
        # 最初の20行を表示
        print(f"\n最初の20行:")
        for row_idx in range(min(20, len(df))):
            row_data = []
            for col_idx in range(min(10, len(df.columns))):
                cell_value = df.iloc[row_idx, col_idx]
                if pd.notna(cell_value):
                    cell_str = str(cell_value).strip()
                    if len(cell_str) > 30:
                        cell_str = cell_str[:30] + "..."
                    row_data.append(f'"{cell_str}"')
                else:
                    row_data.append('""')
            print(f"行{row_idx}: [{', '.join(row_data)}]")
        
    except Exception as e:
        print(f"❌ エラー: {e}")

if __name__ == "__main__":
    check_b692()
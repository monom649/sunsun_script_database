#!/usr/bin/env python3
"""
B692の60行目周辺を詳細確認
"""

import requests
import pandas as pd
import io

def check_b692_detail():
    csv_url = "https://docs.google.com/spreadsheets/d/1Da9LPkx1jCc89OO6zSy6pb2SnCnA4hYN2HDk7zPDdfs/export?format=csv&gid=1384097767"
    
    try:
        response = requests.get(csv_url, timeout=30)
        csv_data = io.StringIO(response.text)
        df = pd.read_csv(csv_data, header=None)
        
        print(f"B692スプレッドシート: {len(df)}行 x {len(df.columns)}列")
        
        # 50-70行目を詳細表示
        print("\n50-70行目:")
        for row_idx in range(50, min(71, len(df))):
            if row_idx < len(df):
                row_data = []
                for col_idx in range(min(8, len(df.columns))):
                    cell_value = df.iloc[row_idx, col_idx]
                    if pd.notna(cell_value):
                        cell_str = str(cell_value).strip()
                        row_data.append(f'"{cell_str}"')
                    else:
                        row_data.append('""')
                print(f"行{row_idx}: [{', '.join(row_data)}]")
        
        # 「とんかつ」「エビフライ」「くもりん」を検索
        print("\n全体での「とんかつ」「エビフライ」「くもりん」検索:")
        found_items = []
        
        for row_idx in range(len(df)):
            for col_idx in range(len(df.columns)):
                cell_value = df.iloc[row_idx, col_idx]
                if pd.notna(cell_value):
                    cell_str = str(cell_value)
                    if 'とんかつ' in cell_str or 'エビフライ' in cell_str or 'くもりん' in cell_str:
                        found_items.append({
                            'row': row_idx,
                            'col': col_idx,
                            'content': cell_str[:100] + ('...' if len(cell_str) > 100 else '')
                        })
        
        for item in found_items:
            print(f"  行{item['row']}, 列{item['col']}: \"{item['content']}\"")
        
        if not found_items:
            print("  該当なし")
        
    except Exception as e:
        print(f"エラー: {e}")

if __name__ == "__main__":
    check_b692_detail()
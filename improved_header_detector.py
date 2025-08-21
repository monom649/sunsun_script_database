#!/usr/bin/env python3
"""
改良版ヘッダー検出器
様々なスプレッドシート構造に対応
"""

import pandas as pd

def find_header_structure(df):
    """
    より柔軟なヘッダー検出
    """
    for row_idx in range(min(25, len(df))):
        row = df.iloc[row_idx]
        
        # キャラクター列を検索
        character_col = None
        for col_idx, value in enumerate(row):
            if pd.notna(value):
                value_str = str(value).strip().lower()
                if 'キャラクター' in value_str or 'キャラ' == value_str:
                    character_col = col_idx
                    break
        
        if character_col is None:
            continue
        
        # キャラクター列が見つかった行で、セリフ列を探す
        dialogue_col = None
        
        # まず隣接する列をチェック（最も一般的）
        if character_col + 1 < len(row):
            next_val = row.iloc[character_col + 1]
            if pd.notna(next_val):
                next_str = str(next_val).strip().lower()
                if not next_str or 'セリフ' in next_str or 'せりふ' in next_str:
                    dialogue_col = character_col + 1
        
        # 隣接列がセリフでない場合、同じ行でセリフ列を探す
        if dialogue_col is None:
            for col_idx, value in enumerate(row):
                if col_idx != character_col and pd.notna(value):
                    value_str = str(value).strip().lower()
                    if 'セリフ' in value_str or 'せりふ' in value_str:
                        dialogue_col = col_idx
                        break
        
        # セリフ列が見つからない場合でも、キャラクター列+1を仮定
        if dialogue_col is None and character_col + 1 < len(df.columns):
            dialogue_col = character_col + 1
        
        if dialogue_col is not None:
            return row_idx, character_col, dialogue_col
    
    return None, None, None

# B1049でテスト
def test_b1049():
    import requests
    import io
    import re
    
    sheet_url = "https://docs.google.com/spreadsheets/d/1FHAecA9zxNco0spYiSfirxp_FLqPsGDWar5iA8XxU40/edit#gid=1384097767"
    
    sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
    gid_pattern = r'[#&]gid=([0-9]+)'
    
    sheet_match = re.search(sheet_pattern, sheet_url)
    gid_match = re.search(gid_pattern, sheet_url)
    
    spreadsheet_id = sheet_match.group(1)
    gid = gid_match.group(1) if gid_match else '0'
    
    csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
    response = requests.get(csv_url, timeout=15)
    
    csv_data = response.content.decode('utf-8', errors='ignore')
    df = pd.read_csv(io.StringIO(csv_data))
    
    header_row, character_col, dialogue_col = find_header_structure(df)
    
    print(f"B1049テスト結果:")
    print(f"ヘッダー行: {header_row+1 if header_row is not None else None}")
    print(f"キャラクター列: {character_col+1 if character_col is not None else None}")
    print(f"セリフ列: {dialogue_col+1 if dialogue_col is not None else None}")
    
    if header_row is not None:
        print(f"\n実際のデータ確認:")
        start_row = header_row + 1
        for i in range(5):
            if start_row + i < len(df):
                row = df.iloc[start_row + i]
                char_val = row.iloc[character_col] if character_col < len(row) else ""
                dialogue_val = row.iloc[dialogue_col] if dialogue_col < len(row) else ""
                print(f"  行{start_row + i + 1}: '{char_val}' → '{dialogue_val}'")

if __name__ == "__main__":
    test_b1049()
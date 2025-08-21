#!/usr/bin/env python3
"""
スプレッドシートのヘッダー行確認ツール
B1780の正しい列構造を確認する
"""

import requests
import pandas as pd
import io
import re

def check_spreadsheet_header():
    # B1780のスプレッドシートURL
    sheet_url = 'https://docs.google.com/spreadsheets/d/1hawj2Z7RifxQsLFiTvpFtCQect8TV92sIWGn6PzFO5I/edit#gid=1115519680'
    
    # スプレッドシートID、GID抽出
    sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
    gid_pattern = r'[#&]gid=([0-9]+)'
    
    sheet_match = re.search(sheet_pattern, sheet_url)
    gid_match = re.search(gid_pattern, sheet_url)
    
    if not sheet_match:
        print("❌ スプレッドシートIDを抽出できませんでした")
        return
    
    spreadsheet_id = sheet_match.group(1)
    gid = gid_match.group(1) if gid_match else '0'
    
    # CSV出力URL
    csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
    
    try:
        # CSVデータ取得
        response = requests.get(csv_url, timeout=15)
        if response.status_code != 200:
            print(f"❌ HTTP {response.status_code}")
            return
        
        # CSV解析
        csv_data = response.content.decode('utf-8', errors='ignore')
        df = pd.read_csv(io.StringIO(csv_data))
        
        print(f"📋 B1780スプレッドシート分析")
        print(f"サイズ: {len(df)}行 x {len(df.columns)}列")
        print("="*80)
        
        # 最初の10行を表示してヘッダー構造確認
        print("🔍 最初の10行:")
        for i in range(min(10, len(df))):
            row = df.iloc[i]
            print(f"行{i+1}: {list(row.values)[:5]}...")  # 最初の5列のみ表示
        
        print("\n📝 列名:")
        for i, col in enumerate(df.columns):
            print(f"列{i+1}: '{col}'")
            
        # ヘッダー行を探す
        print("\n🎯 ヘッダー行検索:")
        for row_idx in range(min(15, len(df))):
            row = df.iloc[row_idx]
            for col_idx, value in enumerate(row):
                if pd.notna(value):
                    value_str = str(value).strip().lower()
                    if any(keyword in value_str for keyword in ['キャラクター', 'セリフ', '指示', '撮影', '音声']):
                        print(f"  行{row_idx+1} 列{col_idx+1}: '{value}' ← ヘッダー候補")
        
        # 26行目（問題の行）を詳細確認
        if len(df) >= 26:
            print(f"\n🎭 26行目詳細（データベースで問題になっている行）:")
            row26 = df.iloc[25]  # 0-indexedなので25
            for i, value in enumerate(row26):
                if pd.notna(value) and str(value).strip():
                    print(f"  列{i+1}: '{value}'")
            
    except Exception as e:
        print(f"❌ エラー: {str(e)}")

if __name__ == "__main__":
    check_spreadsheet_header()
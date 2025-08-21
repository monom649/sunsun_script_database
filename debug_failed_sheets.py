#!/usr/bin/env python3
"""
失敗しているスプレッドシートの構造を詳細確認
"""

import requests
import pandas as pd
import io
import re

def debug_sheet(sheet_url, management_id):
    print(f"\n=== {management_id} デバッグ ===")
    
    try:
        # URL解析
        sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
        gid_pattern = r'[#&]gid=([0-9]+)'
        
        sheet_match = re.search(sheet_pattern, sheet_url)
        gid_match = re.search(gid_pattern, sheet_url)
        
        if not sheet_match:
            print("❌ URL解析失敗")
            return
        
        spreadsheet_id = sheet_match.group(1)
        gid = gid_match.group(1) if gid_match else '0'
        
        csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
        print(f"🔗 CSV URL: {csv_url}")
        
        # リクエスト実行
        response = requests.get(csv_url, timeout=15)
        print(f"📡 HTTP Status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"❌ アクセス失敗: {response.status_code}")
            return
        
        # CSV解析
        csv_data = response.content.decode('utf-8', errors='ignore')
        df = pd.read_csv(io.StringIO(csv_data))
        
        print(f"📋 サイズ: {len(df)}行 x {len(df.columns)}列")
        
        # 最初の15行をチェック
        print("🔍 最初の15行:")
        for i in range(min(15, len(df))):
            row = df.iloc[i]
            non_empty = []
            for j, val in enumerate(row):
                if pd.notna(val) and str(val).strip():
                    non_empty.append(f"列{j+1}:'{str(val).strip()[:20]}...'")
            if non_empty:
                print(f"  行{i+1}: {', '.join(non_empty[:3])}")
        
        # キーワード検索
        print("\n🎯 キーワード検索:")
        keywords = ['キャラクター', 'キャラ', 'セリフ', 'せりふ', '指示', '撮影', '音声']
        found_any = False
        
        for row_idx in range(min(20, len(df))):
            row = df.iloc[row_idx]
            for col_idx, value in enumerate(row):
                if pd.notna(value):
                    value_str = str(value).strip().lower()
                    for keyword in keywords:
                        if keyword in value_str:
                            print(f"  行{row_idx+1} 列{col_idx+1}: '{value}' ← {keyword}検出")
                            found_any = True
        
        if not found_any:
            print("  ❌ キーワードが見つかりません")
            
    except Exception as e:
        print(f"❌ エラー: {str(e)}")

def main():
    # 失敗している3つをデバッグ
    failed_sheets = [
        ("B1049", "https://docs.google.com/spreadsheets/d/1FHAecA9zxNco0spYiSfirxp_FLqPsGDWar5iA8XxU40/edit#gid=1384097767"),
        ("B1050", "https://docs.google.com/spreadsheets/d/1s8WFlpPB0DGSyd7-wxPy31WvpM2328YCELDkmF2NhP4/edit#gid=1384097767"),
        ("B1051", "https://docs.google.com/spreadsheets/d/1rlkAgcPIdlirfd74ZzxdJkKIqbeLBpdvoOb6Wv6axiw/edit#gid=382798477")
    ]
    
    for management_id, sheet_url in failed_sheets:
        debug_sheet(sheet_url, management_id)

if __name__ == "__main__":
    main()
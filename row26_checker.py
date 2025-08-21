#!/usr/bin/env python3
"""
B1780の26行目の詳細確認
"""

import requests
import pandas as pd
import io
import re

def check_row_26():
    sheet_url = 'https://docs.google.com/spreadsheets/d/1hawj2Z7RifxQsLFiTvpFtCQect8TV92sIWGn6PzFO5I/edit#gid=1115519680'
    
    sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
    gid_pattern = r'[#&]gid=([0-9]+)'
    
    sheet_match = re.search(sheet_pattern, sheet_url)
    gid_match = re.search(gid_pattern, sheet_url)
    
    spreadsheet_id = sheet_match.group(1)
    gid = gid_match.group(1) if gid_match else '0'
    
    csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
    
    try:
        response = requests.get(csv_url, timeout=15)
        csv_data = response.content.decode('utf-8', errors='ignore')
        df = pd.read_csv(io.StringIO(csv_data))
        
        print("🎯 26行目の全列詳細:")
        row26 = df.iloc[25]  # 0-indexed
        for i, value in enumerate(row26):
            print(f"  列{i+1}: '{value}' {'(キャラクター)' if i==5 else '(セリフ)' if i==6 else '(音声指示)' if i==7 else '(撮影指示)' if i==8 else '(編集指示)' if i==9 else ''}")
        
        # 正しい列の内容確認
        print("\n✅ 正しい列構造での26行目:")
        print(f"キャラクター（列6）: '{row26.iloc[5] if len(row26) > 5 else 'なし'}'")
        print(f"セリフ（列7）: '{row26.iloc[6] if len(row26) > 6 else 'なし'}'")
        print(f"音声指示（列8）: '{row26.iloc[7] if len(row26) > 7 else 'なし'}'")
        print(f"撮影指示（列9）: '{row26.iloc[8] if len(row26) > 8 else 'なし'}'")
        print(f"編集指示（列10）: '{row26.iloc[9] if len(row26) > 9 else 'なし'}'")
            
    except Exception as e:
        print(f"❌ エラー: {str(e)}")

if __name__ == "__main__":
    check_row_26()
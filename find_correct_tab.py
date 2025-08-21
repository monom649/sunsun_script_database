#!/usr/bin/env python3
"""
正しいタブを総当たりで検出
"""

import requests
import pandas as pd
import io
import re

def test_common_gids(spreadsheet_id):
    """
    よくあるgidをテストして正しいタブを見つける
    """
    # よくあるgid値
    common_gids = [
        '0',           # デフォルト
        '1384097767',  # よく見るgid
        '382798477',   # B1051の古いgid
        '1',           # シンプルな値
        '2',
        '1000000000',  # よくある値
        '2000000000'
    ]
    
    results = []
    
    for gid in common_gids:
        csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
        
        try:
            response = requests.get(csv_url, timeout=10)
            
            if response.status_code == 200:
                csv_data = response.content.decode('utf-8', errors='ignore')
                df = pd.read_csv(io.StringIO(csv_data))
                
                # キャラクター列を探す
                found_character = False
                header_info = None
                
                for row_idx in range(min(15, len(df))):
                    row = df.iloc[row_idx]
                    for col_idx, value in enumerate(row):
                        if pd.notna(value) and 'キャラクター' in str(value):
                            found_character = True
                            header_info = f"行{row_idx+1} 列{col_idx+1}"
                            break
                    if found_character:
                        break
                
                results.append({
                    'gid': gid,
                    'status': 'success',
                    'rows': len(df),
                    'cols': len(df.columns),
                    'has_character': found_character,
                    'header_info': header_info
                })
                
            else:
                results.append({
                    'gid': gid,
                    'status': f'HTTP {response.status_code}',
                    'rows': 0,
                    'cols': 0,
                    'has_character': False,
                    'header_info': None
                })
                
        except Exception as e:
            results.append({
                'gid': gid,
                'status': f'エラー: {str(e)}',
                'rows': 0,
                'cols': 0,
                'has_character': False,
                'header_info': None
            })
    
    return results

def main():
    """
    問題のあったスプレッドシートをテスト
    """
    test_sheets = {
        'B1049': '1FHAecA9zxNco0spYiSfirxp_FLqPsGDWar5iA8XxU40',
        'B1050': '1s8WFlpPB0DGSyd7-wxPy31WvpM2328YCELDkmF2NhP4',
        'B1051': '1rlkAgcPIdlirfd74ZzxdJkKIqbeLBpdvoOb6Wv6axiw'
    }
    
    for management_id, spreadsheet_id in test_sheets.items():
        print(f"\n{'='*50}")
        print(f"{management_id} ({spreadsheet_id})")
        print('='*50)
        
        results = test_common_gids(spreadsheet_id)
        
        for result in results:
            status_emoji = "✅" if result['has_character'] else ("⚠️" if result['status'] == 'success' else "❌")
            print(f"{status_emoji} gid:{result['gid']} - {result['status']} - {result['rows']}行×{result['cols']}列")
            if result['has_character']:
                print(f"   🎯 キャラクター列発見: {result['header_info']}")

if __name__ == "__main__":
    main()
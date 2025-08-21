#!/usr/bin/env python3
"""
"台本"タブのgidを自動検出するツール
"""

import requests
import re
from urllib.parse import urlparse, parse_qs

def find_script_tab_gid(spreadsheet_url):
    """
    スプレッドシートの"台本"タブのgidを検出
    """
    try:
        # スプレッドシートIDを抽出
        sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
        sheet_match = re.search(sheet_pattern, spreadsheet_url)
        
        if not sheet_match:
            return None, "URL解析失敗"
        
        spreadsheet_id = sheet_match.group(1)
        
        # スプレッドシートのメタデータを取得（HTML）
        meta_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(meta_url, headers=headers, timeout=15)
        
        if response.status_code != 200:
            return None, f"HTTP {response.status_code}"
        
        html_content = response.text
        
        # "台本"タブのgidを検索
        # パターン1: "台本"という文字列の近くにあるgid
        script_patterns = [
            r'"台本"[^"]*?"gid":"(\d+)"',
            r'"gid":"(\d+)"[^"]*?"台本"',
            r'台本.*?gid[=:](\d+)',
            r'gid[=:](\d+).*?台本'
        ]
        
        for pattern in script_patterns:
            matches = re.findall(pattern, html_content)
            if matches:
                return matches[0], "台本タブ検出"
        
        # パターン2: 全てのgidを抽出して最初のもの以外を試す
        all_gids = re.findall(r'"gid":"(\d+)"', html_content)
        if len(all_gids) > 1:
            # 最初のgid（通常はシート1）以外を返す
            for gid in all_gids[1:]:
                return gid, f"推定台本タブ (gid:{gid})"
        
        return None, "台本タブが見つからない"
        
    except Exception as e:
        return None, f"エラー: {str(e)}"

def test_gid_detection():
    """
    失敗していたスプレッドシートでテスト
    """
    test_urls = [
        ("B1049", "https://docs.google.com/spreadsheets/d/1FHAecA9zxNco0spYiSfirxp_FLqPsGDWar5iA8XxU40/edit#gid=1384097767"),
        ("B1050", "https://docs.google.com/spreadsheets/d/1s8WFlpPB0DGSyd7-wxPy31WvpM2328YCELDkmF2NhP4/edit#gid=1384097767"),
        ("B1051", "https://docs.google.com/spreadsheets/d/1rlkAgcPIdlirfd74ZzxdJkKIqbeLBpdvoOb6Wv6axiw/edit#gid=1384097767")
    ]
    
    for management_id, url in test_urls:
        print(f"\n=== {management_id} ===")
        gid, message = find_script_tab_gid(url)
        print(f"結果: {message}")
        if gid:
            print(f"検出gid: {gid}")
            
            # 検出したgidでCSVアクセステスト
            sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
            sheet_match = re.search(sheet_pattern, url)
            if sheet_match:
                spreadsheet_id = sheet_match.group(1)
                csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
                
                try:
                    csv_response = requests.get(csv_url, timeout=10)
                    print(f"CSV アクセス: HTTP {csv_response.status_code}")
                    if csv_response.status_code == 200:
                        # ヘッダー確認
                        import pandas as pd
                        import io
                        
                        csv_data = csv_response.content.decode('utf-8', errors='ignore')
                        df = pd.read_csv(io.StringIO(csv_data))
                        
                        # キャラクター列検索
                        found_header = False
                        for row_idx in range(min(10, len(df))):
                            row = df.iloc[row_idx]
                            for col_idx, value in enumerate(row):
                                if pd.notna(value) and 'キャラクター' in str(value):
                                    print(f"✅ キャラクター列発見: 行{row_idx+1} 列{col_idx+1}")
                                    found_header = True
                                    break
                            if found_header:
                                break
                        
                        if not found_header:
                            print("❌ キャラクター列が見つからない")
                
                except Exception as e:
                    print(f"CSV テストエラー: {str(e)}")

if __name__ == "__main__":
    test_gid_detection()
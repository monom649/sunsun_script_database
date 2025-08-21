#!/usr/bin/env python3
"""
B692と同様の列構造問題を抱えるスプレッドシートを特定
"""

import sqlite3
import requests
import pandas as pd
import io
import re
from datetime import datetime
import time

def find_scripts_with_similar_issues():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    # データベースから処理済みスクリプトで、抽出件数が異常に少ないものを特定
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 各スクリプトのダイアログ件数を確認
    cursor.execute("""
        SELECT s.management_id, s.broadcast_date, s.script_url, COUNT(cdu.script_id) as dialogue_count
        FROM scripts s
        LEFT JOIN character_dialogue_unified cdu ON s.id = cdu.script_id
        GROUP BY s.id
        HAVING dialogue_count < 50 AND dialogue_count > 0 AND s.script_url IS NOT NULL AND s.script_url != ''
        ORDER BY dialogue_count ASC, s.broadcast_date
        LIMIT 20
    """)
    
    suspicious_scripts = cursor.fetchall()
    conn.close()
    
    print(f"📋 抽出件数が少ない疑わしいスクリプト: {len(suspicious_scripts)}件")
    print("=" * 80)
    
    for script in suspicious_scripts:
        management_id, broadcast_date, script_url, dialogue_count = script
        print(f"🔍 {management_id} ({broadcast_date}): {dialogue_count}件")
        
        if script_url and 'spreadsheets' in script_url:
            # スプレッドシートの実際の内容を確認
            check_result = check_spreadsheet_content(management_id, script_url)
            if check_result:
                actual_rows, potential_dialogues = check_result
                if potential_dialogues > dialogue_count * 2:  # 2倍以上の差がある場合
                    print(f"  ⚠️  問題の可能性: DB={dialogue_count}件 vs 実際={potential_dialogues}件")
                else:
                    print(f"  ✅ 正常範囲: DB={dialogue_count}件 vs 実際={potential_dialogues}件")
            else:
                print(f"  ❌ スプレッドシート確認失敗")
        
        print()
        time.sleep(1)  # レート制限対策

def check_spreadsheet_content(management_id, script_url):
    """スプレッドシートの実際の内容を確認"""
    try:
        # spreadsheet_idとgid抽出
        spreadsheet_match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', script_url)
        gid_match = re.search(r'gid=(\d+)', script_url)
        
        if not spreadsheet_match or not gid_match:
            return None
        
        spreadsheet_id = spreadsheet_match.group(1)
        gid = gid_match.group(1)
        
        csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
        
        response = requests.get(csv_url, timeout=10)
        if response.status_code != 200:
            return None
        
        content = response.content.decode('utf-8')
        csv_data = io.StringIO(content)
        df = pd.read_csv(csv_data, header=None)
        
        # セリフらしき行をカウント（複数の列でパターン検索）
        potential_dialogues = 0
        
        for row_idx in range(5, min(len(df), 200)):  # 5行目以降をチェック
            row = df.iloc[row_idx]
            
            # 複数の列位置でキャラクター・セリフパターンを検索
            for char_col in [1, 2, 3]:
                dialogue_col = char_col + 1
                
                if len(row) > dialogue_col:
                    char_val = row.iloc[char_col]
                    dial_val = row.iloc[dialogue_col]
                    
                    if pd.notna(char_val) and pd.notna(dial_val):
                        char_str = str(char_val).strip()
                        dial_str = str(dial_val).strip()
                        
                        # キャラクター名らしきパターン
                        if (len(char_str) > 0 and len(char_str) < 20 and 
                            len(dial_str) > 0 and len(dial_str) > 5 and
                            ('サンサン' in char_str or 'くもりん' in char_str or 'シーン' in char_str or
                             char_str in ['ナレーション', 'ナレ', '効果音'])):
                            potential_dialogues += 1
                            break
        
        return len(df), potential_dialogues
        
    except Exception as e:
        return None

if __name__ == "__main__":
    find_scripts_with_similar_issues()
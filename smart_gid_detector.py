#!/usr/bin/env python3
"""
各スプレッドシートごとに正しいgidを検出して修正
"""

import sqlite3
import requests
import pandas as pd
import io
import re
from datetime import datetime

class SmartGidDetector:
    def __init__(self):
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/gid_detection.txt"
    
    def log_message(self, message: str):
        """ログメッセージを出力"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())

detector = SmartGidDetector()

def find_correct_gid_for_sheet(spreadsheet_id):
    """
    スプレッドシートIDから正しい台本タブのgidを検出
    """
    # よくあるgid値をテスト
    test_gids = [
        '1384097767',  # 最も一般的
        '0',           # デフォルト
        '382798477',   # よく見る値
        '1115519680',  # B1780で使われている値
        '1',
        '2'
    ]
    
    for gid in test_gids:
        try:
            csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
            response = requests.get(csv_url, timeout=10)
            
            if response.status_code == 200:
                csv_data = response.content.decode('utf-8', errors='ignore')
                df = pd.read_csv(io.StringIO(csv_data))
                
                # キャラクター列を探す
                for row_idx in range(min(15, len(df))):
                    row = df.iloc[row_idx]
                    for col_idx, value in enumerate(row):
                        if pd.notna(value) and 'キャラクター' in str(value):
                            return gid, f"行{row_idx+1} 列{col_idx+1}"
            
        except Exception:
            continue
    
    return None, "台本タブが見つからない"

def fix_database_gids():
    """
    データベースの全スクリプトURLを正しいgidに修正
    """
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 全スクリプト取得
    cursor.execute("SELECT id, management_id, script_url FROM scripts")
    scripts = cursor.fetchall()
    
    detector.log_message("=" * 60)
    detector.log_message("全スクリプトgid検出・修正開始")
    detector.log_message("=" * 60)
    detector.log_message(f"📊 対象スクリプト: {len(scripts)}件")
    
    success_count = 0
    failed_count = 0
    
    for script_id, management_id, script_url in scripts:  # 全件処理
        # スプレッドシートID抽出
        sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
        sheet_match = re.search(sheet_pattern, script_url)
        
        if not sheet_match:
            detector.log_message(f"❌ {management_id}: URL解析失敗")
            failed_count += 1
            continue
        
        spreadsheet_id = sheet_match.group(1)
        
        # 正しいgidを検出
        correct_gid, result_msg = find_correct_gid_for_sheet(spreadsheet_id)
        
        if correct_gid:
            # URLを更新
            new_url = re.sub(r'#gid=\d+', f'#gid={correct_gid}', script_url)
            cursor.execute("UPDATE scripts SET script_url = ? WHERE id = ?", (new_url, script_id))
            
            detector.log_message(f"✅ {management_id}: gid={correct_gid} ({result_msg})")
            success_count += 1
        else:
            detector.log_message(f"❌ {management_id}: {result_msg}")
            failed_count += 1
        
        # 進捗表示（100件ごと）
        if (success_count + failed_count) % 100 == 0:
            detector.log_message(f"🔧 進捗: {success_count + failed_count}/{len(scripts)} (成功:{success_count}, 失敗:{failed_count})")
    
    conn.commit()
    conn.close()
    
    detector.log_message("=" * 60)
    detector.log_message("gid検出・修正完了")
    detector.log_message("=" * 60)
    detector.log_message(f"📈 最終結果: 成功{success_count}件, 失敗{failed_count}件")
    detector.log_message(f"📝 詳細ログ: gid_detection.txt")

if __name__ == "__main__":
    fix_database_gids()
#!/usr/bin/env python3
"""
列構造問題のある17スクリプトを一括修正
"""

import sqlite3
import requests
import pandas as pd
import io
import re
import time
from datetime import datetime

# 問題のあるスクリプトリスト
problematic_scripts = [
    'B695', 'B700', 'B699', 'B705', 'B704', 'B713', 'B719', 'B728', 
    'B730', 'B741', 'B740', 'B755', 'B764', 'B775', 'B774', 'B785', 'B781'
]

def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def fix_single_script(management_id, db_path):
    """単一スクリプトの修正"""
    try:
        # データベースからスクリプト情報取得
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT id, script_url FROM scripts WHERE management_id = ?", (management_id,))
        result = cursor.fetchone()
        
        if not result:
            log_message(f"❌ {management_id}: スクリプトが見つかりません")
            conn.close()
            return False
        
        script_id, script_url = result
        
        # spreadsheet_idとgid抽出
        spreadsheet_match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', script_url)
        gid_match = re.search(r'gid=(\d+)', script_url)
        
        if not spreadsheet_match or not gid_match:
            log_message(f"❌ {management_id}: URL解析失敗")
            conn.close()
            return False
        
        spreadsheet_id = spreadsheet_match.group(1)
        gid = gid_match.group(1)
        
        csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
        
        # CSV取得
        response = requests.get(csv_url, timeout=30)
        if response.status_code != 200:
            log_message(f"❌ {management_id}: HTTP {response.status_code}")
            conn.close()
            return False
        
        # UTF-8でデコード
        content = response.content.decode('utf-8')
        csv_data = io.StringIO(content)
        df = pd.read_csv(csv_data, header=None)
        
        # 列構造検出（B692と同様のパターン）
        header_row = 4
        character_col = 2
        dialogue_col = 3
        
        # データ抽出
        dialogue_data = []
        for idx in range(header_row + 1, len(df)):
            row = df.iloc[idx]
            
            if len(row) > max(character_col, dialogue_col):
                character = row.iloc[character_col]
                dialogue = row.iloc[dialogue_col]
                
                if pd.notna(character) and pd.notna(dialogue):
                    character_str = str(character).strip()
                    dialogue_str = str(dialogue).strip()
                    
                    if character_str and dialogue_str and len(character_str) < 100:
                        dialogue_data.append({
                            'row': idx,
                            'character': character_str,
                            'dialogue': dialogue_str
                        })
        
        if not dialogue_data:
            log_message(f"⚠️ {management_id}: データ抽出結果なし")
            conn.close()
            return False
        
        # 既存データ削除
        cursor.execute("DELETE FROM character_dialogue_unified WHERE script_id = ?", (script_id,))
        
        # 新データ挿入
        for item in dialogue_data:
            cursor.execute("""
                INSERT INTO character_dialogue_unified 
                (script_id, row_number, character_name, dialogue_text) 
                VALUES (?, ?, ?, ?)
            """, (script_id, item['row'], item['character'], item['dialogue']))
        
        conn.commit()
        conn.close()
        
        log_message(f"✅ {management_id}: {len(dialogue_data)}件を修正完了")
        return True
        
    except Exception as e:
        log_message(f"❌ {management_id}: エラー - {str(e)}")
        if 'conn' in locals():
            conn.close()
        return False

def fix_all_problematic_scripts():
    """問題のある全スクリプトを修正"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    log_message("================================================================================")
    log_message("列構造問題スクリプト一括修正開始")
    log_message("================================================================================")
    log_message(f"🎯 対象スクリプト: {len(problematic_scripts)}件")
    
    success_count = 0
    total_added = 0
    
    for i, management_id in enumerate(problematic_scripts, 1):
        log_message(f"🔧 {management_id} 処理開始 ({i}/{len(problematic_scripts)})")
        
        if fix_single_script(management_id, db_path):
            success_count += 1
        
        time.sleep(1)  # レート制限対策
    
    log_message("================================================================================")
    log_message(f"列構造問題修正完了: {success_count}/{len(problematic_scripts)}件成功")
    log_message("================================================================================")
    
    # 最終統計確認
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(DISTINCT script_id) FROM character_dialogue_unified")
    processed_scripts = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified")
    total_dialogues = cursor.fetchone()[0]
    conn.close()
    
    log_message(f"📊 最終統計: 処理済みスクリプト={processed_scripts}件, 総ダイアログ={total_dialogues}件")

if __name__ == "__main__":
    fix_all_problematic_scripts()
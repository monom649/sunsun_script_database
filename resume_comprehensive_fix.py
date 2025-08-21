#!/usr/bin/env python3
"""
包括的データ修正の続行
未処理スクリプトを特定して処理を続行
"""

import sqlite3
import requests
import pandas as pd
import io
import re
from datetime import datetime
import time

class ResumeComprehensiveFixer:
    def __init__(self, db_path):
        self.db_path = db_path
        
    def log_message(self, message: str):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {message}")
    
    def find_header_row_and_columns(self, df):
        """ヘッダー行とキャラクター・セリフ列を特定"""
        for row_idx in range(min(25, len(df))):
            row = df.iloc[row_idx]
            
            character_col = None
            for col_idx, value in enumerate(row):
                if pd.notna(value):
                    value_str = str(value).strip().lower()
                    if 'キャラクター' in value_str or 'キャラ' == value_str:
                        character_col = col_idx
                        break
            
            if character_col is not None:
                dialogue_col = None
                for col_idx in range(character_col + 1, len(row)):
                    value = row.iloc[col_idx] if col_idx < len(row) else None
                    if pd.notna(value):
                        value_str = str(value).strip().lower()
                        if 'セリフ' in value_str or 'せりふ' in value_str:
                            dialogue_col = col_idx
                            break
                
                if dialogue_col is not None:
                    return row_idx, character_col, dialogue_col
        
        return None, None, None
    
    def extract_gid_from_url(self, url):
        """URLからgidを抽出"""
        match = re.search(r'gid=(\d+)', url)
        return match.group(1) if match else None
    
    def get_csv_url_from_gid(self, spreadsheet_id, gid):
        """spreadsheet_idとgidからCSV URLを生成"""
        return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
    
    def process_script(self, script_id, script_name, script_url):
        """単一スクリプトを処理"""
        try:
            # spreadsheet_id抽出
            spreadsheet_match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', script_url)
            if not spreadsheet_match:
                self.log_message(f"❌ {script_name}: URL解析失敗")
                return False
            
            spreadsheet_id = spreadsheet_match.group(1)
            gid = self.extract_gid_from_url(script_url)
            
            if not gid:
                self.log_message(f"❌ {script_name}: gid抽出失敗")
                return False
            
            csv_url = self.get_csv_url_from_gid(spreadsheet_id, gid)
            
            # CSV取得
            response = requests.get(csv_url, timeout=30)
            if response.status_code != 200:
                self.log_message(f"❌ {script_name}: HTTP {response.status_code}")
                return False
            
            # DataFrame作成
            csv_data = io.StringIO(response.text)
            df = pd.read_csv(csv_data, header=None)
            
            if df.empty:
                self.log_message(f"❌ {script_name}: 空データ")
                return False
            
            # ヘッダー検出
            header_row, character_col, dialogue_col = self.find_header_row_and_columns(df)
            
            if header_row is None:
                self.log_message(f"❌ {script_name}: ヘッダー検出失敗")
                return False
            
            # データ抽出
            dialogue_data = []
            for idx in range(header_row + 1, len(df)):
                row = df.iloc[idx]
                
                if character_col < len(row) and dialogue_col < len(row):
                    character = row.iloc[character_col]
                    dialogue = row.iloc[dialogue_col]
                    
                    if pd.notna(character) and pd.notna(dialogue):
                        character_str = str(character).strip()
                        dialogue_str = str(dialogue).strip()
                        
                        if character_str and dialogue_str:
                            dialogue_data.append({
                                'character': character_str,
                                'dialogue': dialogue_str
                            })
            
            if not dialogue_data:
                self.log_message(f"⚠️ {script_name}: データ抽出結果なし")
                return False
            
            # データベース挿入
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for item in dialogue_data:
                cursor.execute("""
                    INSERT INTO character_dialogue_unified 
                    (script_id, character_name, dialogue) 
                    VALUES (?, ?, ?)
                """, (script_id, item['character'], item['dialogue']))
            
            conn.commit()
            conn.close()
            
            self.log_message(f"✅ {script_name}: ヘッダー行{header_row}, キャラ列{character_col}, セリフ列{dialogue_col}, {len(dialogue_data)}件抽出")
            self.log_message(f"✅ {script_name}: {len(dialogue_data)}件挿入完了")
            
            return True
            
        except Exception as e:
            self.log_message(f"❌ {script_name}: エラー - {str(e)}")
            return False
    
    def get_unprocessed_scripts(self):
        """未処理スクリプトを取得"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT s.id, s.management_id, s.script_url
            FROM scripts s
            LEFT JOIN character_dialogue_unified cdu ON s.id = cdu.script_id
            WHERE cdu.script_id IS NULL
            ORDER BY s.id
        """)
        
        unprocessed = cursor.fetchall()
        conn.close()
        
        return unprocessed
    
    def run(self):
        """未処理スクリプトの処理を実行"""
        self.log_message("================================================================================")
        self.log_message("包括修正続行開始")
        self.log_message("================================================================================")
        
        unprocessed_scripts = self.get_unprocessed_scripts()
        total_unprocessed = len(unprocessed_scripts)
        
        self.log_message(f"🎯 未処理スクリプト: {total_unprocessed}件")
        
        if total_unprocessed == 0:
            self.log_message("✅ 全スクリプト処理済み")
            return
        
        success_count = 0
        for i, (script_id, script_name, script_url) in enumerate(unprocessed_scripts, 1):
            self.log_message(f"🔧 {script_name} 処理開始")
            
            if self.process_script(script_id, script_name, script_url):
                success_count += 1
            
            if i % 50 == 0:
                self.log_message(f"🔧 進捗: {i}/{total_unprocessed}")
            
            time.sleep(1)  # レート制限対策
        
        self.log_message("================================================================================")
        self.log_message(f"包括修正続行完了: {success_count}/{total_unprocessed}件成功")
        self.log_message("================================================================================")

if __name__ == "__main__":
    fixer = ResumeComprehensiveFixer("/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db")
    fixer.run()
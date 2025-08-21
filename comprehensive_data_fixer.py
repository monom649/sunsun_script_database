#!/usr/bin/env python3
"""
全スクリプト包括的データ修正ツール

全てのスプレッドシートを正しいヘッダー構造で再抽出し、
データベースを完全に正しい状態にする
"""

import sqlite3
import requests
import pandas as pd
import io
import re
from datetime import datetime
import time

class ComprehensiveDataFixer:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/comprehensive_fix.txt"
        
        # ヘッダー検出キーワード
        self.character_keywords = ['キャラクター', 'キャラ', 'character']
        self.dialogue_keywords = ['セリフ', 'せりふ', 'dialogue', 'セリフ内容']
    
    def log_message(self, message: str):
        """ログメッセージを出力"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())
    
    def find_header_row_and_columns(self, df):
        """
        改良版ヘッダー行とキャラクター・セリフ列を特定
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
    
    def extract_correct_data(self, sheet_url):
        """
        スプレッドシートから正しいデータを抽出
        """
        try:
            # URL解析
            sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
            gid_pattern = r'[#&]gid=([0-9]+)'
            
            sheet_match = re.search(sheet_pattern, sheet_url)
            gid_match = re.search(gid_pattern, sheet_url)
            
            if not sheet_match:
                return None, "URL解析失敗"
            
            spreadsheet_id = sheet_match.group(1)
            gid = gid_match.group(1) if gid_match else '0'
            
            # CSV取得
            csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
            response = requests.get(csv_url, timeout=15)
            
            if response.status_code != 200:
                return None, f"HTTP {response.status_code}"
            
            # DataFrame作成
            csv_data = response.content.decode('utf-8', errors='ignore')
            df = pd.read_csv(io.StringIO(csv_data))
            
            # ヘッダー行と列を特定
            header_row, character_col, dialogue_col = self.find_header_row_and_columns(df)
            
            if header_row is None:
                return None, "ヘッダー検出失敗"
            
            # データ抽出
            extracted_data = []
            start_row = header_row + 1
            
            for row_idx in range(start_row, len(df)):
                row = df.iloc[row_idx]
                
                # キャラクター名取得
                character_name = ""
                if character_col < len(row):
                    char_val = row.iloc[character_col]
                    if pd.notna(char_val):
                        character_name = str(char_val).strip()
                
                # セリフ取得
                dialogue_text = ""
                if dialogue_col < len(row):
                    dialogue_val = row.iloc[dialogue_col]
                    if pd.notna(dialogue_val):
                        dialogue_text = str(dialogue_val).strip()
                
                # 指示（セリフ列以降の全列）
                instructions = []
                for inst_col in range(dialogue_col + 1, len(row)):
                    inst_val = row.iloc[inst_col]
                    if pd.notna(inst_val):
                        inst_text = str(inst_val).strip()
                        if inst_text:
                            instructions.append(inst_text)
                
                filming_instruction = " | ".join(instructions) if instructions else ""
                
                # 有効なデータの場合のみ追加
                if character_name or dialogue_text:
                    extracted_data.append({
                        'row_number': row_idx + 1,
                        'character_name': character_name,
                        'dialogue_text': dialogue_text,
                        'filming_instruction': filming_instruction
                    })
            
            return extracted_data, f"ヘッダー行{header_row+1}, キャラ列{character_col+1}, セリフ列{dialogue_col+1}"
            
        except Exception as e:
            return None, f"エラー: {str(e)}"
    
    def fix_single_script(self, script_info):
        """
        単一スクリプトを修正
        """
        script_id, management_id, script_url = script_info
        
        self.log_message(f"🔧 {management_id} 処理開始")
        
        # 正しいデータを抽出
        extracted_data, result_msg = self.extract_correct_data(script_url)
        
        if extracted_data is None:
            self.log_message(f"❌ {management_id}: {result_msg}")
            return False
        
        self.log_message(f"✅ {management_id}: {result_msg}, {len(extracted_data)}件抽出")
        
        # 既存データを削除
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM character_dialogue_unified WHERE script_id = ?", (script_id,))
        
        # 新しいデータを挿入
        inserted_count = 0
        for data in extracted_data:
            if data['character_name'] or data['dialogue_text']:
                cursor.execute("""
                    INSERT INTO character_dialogue_unified 
                    (script_id, row_number, character_name, dialogue_text, filming_audio_instructions)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    script_id,
                    data['row_number'],
                    data['character_name'] or '[話者不明]',
                    data['dialogue_text'],
                    data['filming_instruction']
                ))
                inserted_count += 1
        
        conn.commit()
        conn.close()
        
        self.log_message(f"✅ {management_id}: {inserted_count}件挿入完了")
        return True
    
    def fix_all_scripts(self):
        """
        全スクリプトを修正
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 全スクリプト取得
            cursor.execute("SELECT id, management_id, script_url FROM scripts ORDER BY management_id")
            scripts = cursor.fetchall()
            conn.close()
            
            self.log_message("=" * 80)
            self.log_message("全スクリプト包括修正開始")
            self.log_message("=" * 80)
            self.log_message(f"🎯 対象スクリプト: {len(scripts)}件")
            
            success_count = 0
            failed_count = 0
            
            for i, script_info in enumerate(scripts):
                # 進捗表示
                if (i + 1) % 50 == 0:
                    self.log_message(f"🔧 進捗: {i+1}/{len(scripts)}")
                
                # スクリプト修正
                if self.fix_single_script(script_info):
                    success_count += 1
                else:
                    failed_count += 1
                
                # レート制限回避
                time.sleep(0.5)
            
            self.log_message("=" * 80)
            self.log_message("全スクリプト修正完了")
            self.log_message("=" * 80)
            self.log_message(f"✅ 成功: {success_count}件")
            self.log_message(f"❌ 失敗: {failed_count}件")
            
            return success_count, failed_count
            
        except Exception as e:
            self.log_message(f"❌ 全体処理エラー: {str(e)}")
            return 0, 0

def main():
    """メイン実行"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    fixer = ComprehensiveDataFixer(db_path)
    
    print("=== 全スクリプト包括データ修正 ===")
    print("⚠️  この処理には時間がかかります")
    
    success, failed = fixer.fix_all_scripts()
    
    print(f"\n📊 最終結果:")
    print(f"✅ 修正成功: {success}件")
    print(f"❌ 修正失敗: {failed}件")
    print(f"📝 詳細ログ: comprehensive_fix.txt")

if __name__ == "__main__":
    main()
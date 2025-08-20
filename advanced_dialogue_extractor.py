#!/usr/bin/env python3
"""
Advanced Dialogue Extractor

This script uses advanced techniques to extract dialogue data from problematic
spreadsheets by analyzing multiple structure patterns and using fallback methods.
"""

import sqlite3
import requests
import pandas as pd
import io
import re
from datetime import datetime

class AdvancedDialogueExtractor:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/advanced_extractor_log.txt"
        
    def log_message(self, message: str):
        """Log messages with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())
    
    def extract_advanced_spreadsheet_data(self, script_url, management_id):
        """Advanced extraction with multiple fallback strategies"""
        if not script_url or 'docs.google.com' not in script_url:
            return None
        
        try:
            # Extract spreadsheet ID and GID
            sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
            gid_pattern = r'[#&]gid=([0-9]+)'
            
            sheet_match = re.search(sheet_pattern, script_url)
            gid_match = re.search(gid_pattern, script_url)
            
            if not sheet_match:
                return None
            
            spreadsheet_id = sheet_match.group(1)
            gid = gid_match.group(1) if gid_match else '0'
            
            # Build CSV export URL
            csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
            
            # Fetch CSV data
            response = requests.get(csv_url, timeout=15)
            if response.status_code != 200:
                self.log_message(f"⚠️  {management_id}: HTTP {response.status_code}")
                return None
            
            # Parse CSV
            csv_data = response.content.decode('utf-8', errors='ignore')
            df = pd.read_csv(io.StringIO(csv_data))
            
            # Strategy 1: Look for standard character names in any column
            character_names = ['サンサン', 'くもりん', 'ツクモ', 'ノイズ', 'ママ', 'パパ', 'BB', 
                             'みんな', 'こども', '子ども', '（SE）', '(SE)', 'SE']
            
            dialogue_entries = []
            
            for index, row in df.iterrows():
                if index < 3:  # Skip potential header rows
                    continue
                
                row_data = row.to_dict()
                
                # Find character names in any column
                for col_name, value in row_data.items():
                    if pd.notna(value) and isinstance(value, str):
                        value_str = str(value).strip()
                        
                        # Check if this looks like a character name
                        for char_name in character_names:
                            if char_name in value_str:
                                character = char_name
                                
                                # Look for dialogue in adjacent columns
                                try:
                                    cols = list(df.columns)
                                    char_col_index = cols.index(col_name)
                                    
                                    # Check multiple patterns for dialogue
                                    for offset in [1, 2, 3, -1]:
                                        if 0 <= char_col_index + offset < len(cols):
                                            dialogue_col_name = cols[char_col_index + offset]
                                            dialogue_value = row_data.get(dialogue_col_name)
                                            
                                            if pd.notna(dialogue_value):
                                                dialogue_text = str(dialogue_value).strip()
                                                
                                                # Skip empty or instruction-like text
                                                if (len(dialogue_text) > 2 and 
                                                    not dialogue_text.startswith(('カット', '映像', 'テロップ', 'アニメ', 'SE', '（', '(')) and
                                                    not dialogue_text.endswith(('カット', '映像'))):
                                                    
                                                    dialogue_entries.append({
                                                        'row_number': index,
                                                        'character_name': character,
                                                        'dialogue_text': dialogue_text,
                                                        'filming_audio_instructions': ''
                                                    })
                                                    break
                                except Exception:
                                    continue
                                break
            
            # Strategy 2: If no standard names found, look for dialogue patterns
            if len(dialogue_entries) == 0:
                self.log_message(f"🔄 {management_id}: 標準パターンで見つからず、セリフパターンで検索中...")
                
                for index, row in df.iterrows():
                    if index < 3:
                        continue
                    
                    row_data = row.to_dict()
                    
                    # Look for dialogue-like patterns
                    for col_name, value in row_data.items():
                        if pd.notna(value) and isinstance(value, str):
                            value_str = str(value).strip()
                            
                            # Check if this looks like dialogue
                            if (len(value_str) > 10 and 
                                (value_str.endswith(('！', '？', '。', '♪', '～')) or
                                 'だよ' in value_str or 'です' in value_str or 'だね' in value_str or
                                 'ちゃん' in value_str or 'くん' in value_str or
                                 'みんな' in value_str)):
                                
                                # Try to find character name in adjacent columns
                                try:
                                    cols = list(df.columns)
                                    dialogue_col_index = cols.index(col_name)
                                    
                                    character = '不明'
                                    for offset in [-1, -2, 1]:
                                        if 0 <= dialogue_col_index + offset < len(cols):
                                            char_col_name = cols[dialogue_col_index + offset]
                                            char_value = row_data.get(char_col_name)
                                            
                                            if pd.notna(char_value):
                                                char_str = str(char_value).strip()
                                                if len(char_str) > 0 and len(char_str) < 10:
                                                    character = char_str
                                                    break
                                    
                                    dialogue_entries.append({
                                        'row_number': index,
                                        'character_name': character,
                                        'dialogue_text': value_str,
                                        'filming_audio_instructions': ''
                                    })
                                    
                                    if len(dialogue_entries) >= 50:  # Limit to avoid too much data
                                        break
                                        
                                except Exception:
                                    continue
            
            # Strategy 3: If still nothing, extract any text that looks like speech
            if len(dialogue_entries) == 0:
                self.log_message(f"🔄 {management_id}: フリーテキスト検索を実行中...")
                
                for index, row in df.iterrows():
                    if index < 5:
                        continue
                    
                    row_data = row.to_dict()
                    
                    for col_name, value in row_data.items():
                        if pd.notna(value) and isinstance(value, str):
                            value_str = str(value).strip()
                            
                            # Very loose criteria for speech-like text
                            if (len(value_str) >= 5 and 
                                not value_str.startswith(('http', 'https', 'www', '=', 'カット', 'SE')) and
                                ('！' in value_str or '？' in value_str or '。' in value_str)):
                                
                                dialogue_entries.append({
                                    'row_number': index,
                                    'character_name': 'キャラクター',
                                    'dialogue_text': value_str,
                                    'filming_audio_instructions': ''
                                })
                                
                                if len(dialogue_entries) >= 30:
                                    break
                    
                    if len(dialogue_entries) >= 30:
                        break
            
            self.log_message(f"📊 {management_id}: {len(dialogue_entries)}件のセリフを抽出")
            return dialogue_entries if len(dialogue_entries) > 0 else None
            
        except Exception as e:
            self.log_message(f"❌ {management_id}: 抽出エラー - {str(e)}")
            return None
    
    def fix_problematic_script(self, script_info):
        """Fix a single problematic script using advanced extraction"""
        try:
            # Extract dialogue using advanced methods
            new_dialogue = self.extract_advanced_spreadsheet_data(
                script_info['script_url'], 
                script_info['management_id']
            )
            
            if not new_dialogue:
                return False
            
            # Update database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get script ID
            cursor.execute("SELECT id FROM scripts WHERE management_id = ?", (script_info['management_id'],))
            script_row = cursor.fetchone()
            if not script_row:
                conn.close()
                return False
            
            script_id = script_row[0]
            
            # Delete existing empty entries for this script
            cursor.execute("""
                DELETE FROM character_dialogue_unified 
                WHERE script_id = ? AND (dialogue_text IS NULL OR dialogue_text = '')
            """, (script_id,))
            deleted_count = cursor.rowcount
            
            # Insert new dialogue data
            inserted = 0
            for entry in new_dialogue:
                try:
                    cursor.execute("""
                        INSERT INTO character_dialogue_unified 
                        (script_id, row_number, character_name, dialogue_text, filming_audio_instructions)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        script_id,
                        entry['row_number'],
                        entry['character_name'],
                        entry['dialogue_text'],
                        entry['filming_audio_instructions']
                    ))
                    inserted += 1
                except Exception as e:
                    self.log_message(f"⚠️  {script_info['management_id']}: 挿入エラー - {str(e)}")
            
            conn.commit()
            conn.close()
            
            self.log_message(f"✅ {script_info['management_id']}: {deleted_count}件削除、{inserted}件追加")
            return True
            
        except Exception as e:
            self.log_message(f"❌ {script_info['management_id']}: 修正エラー - {str(e)}")
            return False
    
    def fix_all_problematic_scripts(self, max_scripts=50):
        """Fix all problematic scripts using advanced extraction"""
        self.log_message("=" * 80)
        self.log_message("高度抽出による全問題スクリプト修正開始")
        self.log_message("=" * 80)
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get all problematic scripts
            cursor.execute("""
                SELECT DISTINCT s.management_id, s.title, s.script_url
                FROM scripts s
                JOIN character_dialogue_unified cdu ON s.id = cdu.script_id
                WHERE LENGTH(cdu.character_name) > 0 AND (cdu.dialogue_text IS NULL OR cdu.dialogue_text = '')
                ORDER BY s.management_id
                LIMIT ?
            """, (max_scripts,))
            
            problematic_scripts = []
            for mgmt_id, title, script_url in cursor.fetchall():
                problematic_scripts.append({
                    'management_id': mgmt_id,
                    'title': title,
                    'script_url': script_url
                })
            
            conn.close()
            
            self.log_message(f"🎯 修正対象: {len(problematic_scripts)}件")
            
            fixed_count = 0
            failed_count = 0
            
            for i, script in enumerate(problematic_scripts):
                self.log_message(f"🔧 修正中 ({i+1}/{len(problematic_scripts)}): {script['management_id']}")
                
                if self.fix_problematic_script(script):
                    fixed_count += 1
                else:
                    failed_count += 1
                
                # Rate limiting
                if i % 3 == 2:
                    import time
                    time.sleep(1)
            
            self.log_message("=" * 80)
            self.log_message(f"高度抽出修正結果:")
            self.log_message(f"  修正成功: {fixed_count}件")
            self.log_message(f"  修正失敗: {failed_count}件")
            self.log_message("=" * 80)
            
            return fixed_count
            
        except Exception as e:
            self.log_message(f"❌ 全体修正エラー: {str(e)}")
            return 0

def main():
    """Main execution function"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    extractor = AdvancedDialogueExtractor(db_path)
    
    print("=== 高度セリフ抽出ツール ===")
    
    # Fix all problematic scripts
    fixed_count = extractor.fix_all_problematic_scripts(max_scripts=100)
    
    if fixed_count > 0:
        print(f"\n✅ {fixed_count}件のスクリプトが修正されました！")
    else:
        print(f"\n❌ 修正できるスクリプトがありませんでした")

if __name__ == "__main__":
    main()
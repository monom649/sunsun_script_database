#!/usr/bin/env python3
"""
Complete Header-Based Extractor

This script properly identifies all header columns including "キャラクター", "セリフ", 
"撮影指示", and "音声指示" and places data in correct columns.
"""

import sqlite3
import requests
import pandas as pd
import io
import re
from datetime import datetime

class CompleteHeaderExtractor:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/complete_header_log.txt"
        
    def log_message(self, message: str):
        """Log messages with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())
    
    def find_all_header_columns(self, df):
        """Find all header columns"""
        character_col = None
        dialogue_col = None
        filming_col = None
        audio_col = None
        header_row = None
        
        # Search first 10 rows for headers
        for row_idx in range(min(10, len(df))):
            row = df.iloc[row_idx]
            
            for col_idx, value in enumerate(row):
                if pd.notna(value):
                    value_str = str(value).strip()
                    
                    # Check for character header
                    if 'キャラクター' in value_str:
                        character_col = col_idx
                        header_row = row_idx
                    
                    # Check for dialogue header
                    if 'セリフ' in value_str:
                        dialogue_col = col_idx
                        if header_row is None:
                            header_row = row_idx
                    
                    # Check for filming instruction header
                    if '撮影指示' in value_str or '撮影' in value_str:
                        filming_col = col_idx
                        if header_row is None:
                            header_row = row_idx
                    
                    # Check for audio instruction header
                    if '音声指示' in value_str or '音声' in value_str:
                        audio_col = col_idx
                        if header_row is None:
                            header_row = row_idx
        
        return character_col, dialogue_col, filming_col, audio_col, header_row
    
    def extract_complete_data(self, script_url, management_id):
        """Extract complete data using all header columns"""
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
            
            self.log_message(f"📊 {management_id}: {len(df)}行 x {len(df.columns)}列のスプレッドシート")
            
            # Find all header columns
            character_col, dialogue_col, filming_col, audio_col, header_row = self.find_all_header_columns(df)
            
            if character_col is None:
                self.log_message(f"❌ {management_id}: キャラクターヘッダーが見つかりません")
                return None
            
            header_info = [f"キャラクター列{character_col}"]
            if dialogue_col is not None:
                header_info.append(f"セリフ列{dialogue_col}")
            if filming_col is not None:
                header_info.append(f"撮影指示列{filming_col}")
            if audio_col is not None:
                header_info.append(f"音声指示列{audio_col}")
            
            self.log_message(f"✅ {management_id}: {', '.join(header_info)}, ヘッダー行{header_row}")
            
            entries = []
            
            # Extract data starting from after header row
            start_row = header_row + 1 if header_row is not None else 3
            
            for index in range(start_row, len(df)):
                row = df.iloc[index]
                
                # Get character name
                character_value = row.iloc[character_col] if character_col < len(row) else None
                if not pd.notna(character_value):
                    continue
                
                character_name = str(character_value).strip()
                if len(character_name) == 0 or len(character_name) > 20:
                    continue
                
                # Skip if character name looks like instruction
                instruction_markers = ['・', '（', '）', '→', 'カット', '映像', 'テロップ', 
                                     'アニメ', '編集', '選手', '登場', 'シーン', '全種類', '紹介', '作り方', '演出']
                
                if any(marker in character_name for marker in instruction_markers):
                    continue
                
                # Get dialogue text
                dialogue_text = ""
                if dialogue_col is not None and dialogue_col < len(row):
                    dialogue_value = row.iloc[dialogue_col]
                    if pd.notna(dialogue_value):
                        dialogue_text = str(dialogue_value).strip()
                        # Clean contaminated dialogue
                        if any(marker in dialogue_text for marker in instruction_markers):
                            dialogue_text = ""
                
                # Get filming instructions
                filming_instructions = ""
                if filming_col is not None and filming_col < len(row):
                    filming_value = row.iloc[filming_col]
                    if pd.notna(filming_value):
                        filming_instructions = str(filming_value).strip()
                
                # Get audio instructions
                audio_instructions = ""
                if audio_col is not None and audio_col < len(row):
                    audio_value = row.iloc[audio_col]
                    if pd.notna(audio_value):
                        audio_instructions = str(audio_value).strip()
                
                # Combine instructions
                combined_instructions = ""
                if filming_instructions and audio_instructions:
                    combined_instructions = f"{filming_instructions} / {audio_instructions}"
                elif filming_instructions:
                    combined_instructions = filming_instructions
                elif audio_instructions:
                    combined_instructions = audio_instructions
                
                # Only include if we have dialogue or instructions
                if len(dialogue_text) > 0 or len(combined_instructions) > 0:
                    entries.append({
                        'row_number': index,
                        'character_name': character_name,
                        'dialogue_text': dialogue_text,
                        'filming_audio_instructions': combined_instructions
                    })
            
            dialogue_count = sum(1 for entry in entries if len(entry['dialogue_text']) > 0)
            instruction_count = sum(1 for entry in entries if len(entry['filming_audio_instructions']) > 0)
            self.log_message(f"📝 {management_id}: セリフ{dialogue_count}件、指示{instruction_count}件を正しい列に分離")
            
            return entries if len(entries) > 0 else None
            
        except Exception as e:
            self.log_message(f"❌ {management_id}: 抽出エラー - {str(e)}")
            return None
    
    def update_script_data(self, script_info):
        """Update script with complete header-based extraction"""
        try:
            # Extract complete data
            new_data = self.extract_complete_data(
                script_info['script_url'], 
                script_info['management_id']
            )
            
            if not new_data:
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
            
            # Delete all existing entries for this script
            cursor.execute("DELETE FROM character_dialogue_unified WHERE script_id = ?", (script_id,))
            deleted_count = cursor.rowcount
            
            # Insert new properly separated data
            inserted = 0
            for entry in new_data:
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
            
            self.log_message(f"✅ {script_info['management_id']}: {deleted_count}件削除、{inserted}件を正しい列に配置")
            return True
            
        except Exception as e:
            self.log_message(f"❌ {script_info['management_id']}: 更新エラー - {str(e)}")
            return False
    
    def process_all_scripts(self, max_scripts=100):
        """Process all scripts with complete header extraction"""
        self.log_message("=" * 80)
        self.log_message("全ヘッダー完全分離処理開始")
        self.log_message("=" * 80)
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get scripts to process
            cursor.execute("""
                SELECT DISTINCT s.management_id, s.title, s.script_url
                FROM scripts s
                WHERE s.script_url IS NOT NULL AND s.script_url != ''
                ORDER BY s.management_id
                LIMIT ?
            """, (max_scripts,))
            
            scripts = []
            for mgmt_id, title, script_url in cursor.fetchall():
                scripts.append({
                    'management_id': mgmt_id,
                    'title': title,
                    'script_url': script_url
                })
            
            conn.close()
            
            if not scripts:
                self.log_message("❌ 処理対象スクリプトが見つかりませんでした")
                return 0
            
            self.log_message(f"🎯 完全分離対象: {len(scripts)}件")
            
            success_count = 0
            fail_count = 0
            header_found_count = 0
            
            for i, script in enumerate(scripts):
                self.log_message(f"🔧 完全分離中 ({i+1}/{len(scripts)}): {script['management_id']}")
                
                # Check if headers are found
                data = self.extract_complete_data(script['script_url'], script['management_id'])
                if data is not None:
                    header_found_count += 1
                    
                    if self.update_script_data(script):
                        success_count += 1
                    else:
                        fail_count += 1
                else:
                    fail_count += 1
                
                # Rate limiting
                if i % 5 == 4:
                    import time
                    time.sleep(1)
            
            self.log_message("=" * 80)
            self.log_message(f"全ヘッダー完全分離結果:")
            self.log_message(f"  ヘッダー発見: {header_found_count}件")
            self.log_message(f"  分離成功: {success_count}件")
            self.log_message(f"  分離失敗: {fail_count}件")
            self.log_message("=" * 80)
            
            return success_count
            
        except Exception as e:
            self.log_message(f"❌ 全体処理エラー: {str(e)}")
            return 0

def main():
    """Main execution function"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    extractor = CompleteHeaderExtractor(db_path)
    
    print("=== 完全ヘッダー分離ツール ===")
    
    # Process scripts with complete header extraction
    success_count = extractor.process_all_scripts(max_scripts=200)
    
    if success_count > 0:
        print(f"\n✅ {success_count}件のスクリプトが完全分離されました！")
    else:
        print(f"\n❌ 分離できるスクリプトがありませんでした")

if __name__ == "__main__":
    main()
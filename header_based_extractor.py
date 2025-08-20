#!/usr/bin/env python3
"""
Header-Based Dialogue Extractor

This script properly identifies "キャラクター" and "セリフ" header rows in spreadsheets
and uses these columns for accurate character-dialogue extraction, eliminating
instruction text contamination.
"""

import sqlite3
import requests
import pandas as pd
import io
import re
from datetime import datetime

class HeaderBasedExtractor:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/header_based_log.txt"
        
    def log_message(self, message: str):
        """Log messages with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())
    
    def find_header_columns(self, df):
        """Find columns containing headers"""
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
                    if 'キャラクター' in value_str or value_str == 'キャラクター':
                        character_col = col_idx
                        header_row = row_idx
                    
                    # Check for dialogue header
                    if 'セリフ' in value_str or value_str == 'セリフ':
                        dialogue_col = col_idx
                        if header_row is None:
                            header_row = row_idx
                    
                    # Check for filming instruction header
                    if '撮影指示' in value_str or value_str == '撮影指示' or '撮影' in value_str:
                        filming_col = col_idx
                        if header_row is None:
                            header_row = row_idx
                    
                    # Check for audio instruction header
                    if '音声指示' in value_str or value_str == '音声指示' or '音声' in value_str:
                        audio_col = col_idx
                        if header_row is None:
                            header_row = row_idx
        
        return character_col, dialogue_col, filming_col, audio_col, header_row
    
    def extract_header_based_data(self, script_url, management_id):
        """Extract data using header-based column identification"""
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
            
            # Find header columns
            character_col, dialogue_col, filming_col, audio_col, header_row = self.find_header_columns(df)
            
            if character_col is None:
                self.log_message(f"❌ {management_id}: キャラクターヘッダー行が見つかりません")
                return None
            
            header_info = [f"キャラクター列{character_col}"]
            if dialogue_col is not None:
                header_info.append(f"セリフ列{dialogue_col}")
            if filming_col is not None:
                header_info.append(f"撮影指示列{filming_col}")
            if audio_col is not None:
                header_info.append(f"音声指示列{audio_col}")
            
            self.log_message(f"✅ {management_id}: {', '.join(header_info)}, ヘッダー行{header_row}")
            
            dialogue_entries = []
            
            # Extract data starting from after header row
            start_row = header_row + 1 if header_row is not None else 3
            
            for index in range(start_row, len(df)):
                row = df.iloc[index]
                
                # Get values from identified columns
                character_value = row.iloc[character_col] if character_col is not None and character_col < len(row) else None
                dialogue_value = row.iloc[dialogue_col] if dialogue_col is not None and dialogue_col < len(row) else None
                filming_value = row.iloc[filming_col] if filming_col is not None and filming_col < len(row) else None
                audio_value = row.iloc[audio_col] if audio_col is not None and audio_col < len(row) else None
                
                # Process character and dialogue
                if pd.notna(character_value) and character_col is not None:
                    character_name = str(character_value).strip()
                    
                    # Get dialogue text
                    dialogue_text = ""
                    if pd.notna(dialogue_value) and dialogue_col is not None:
                        dialogue_text = str(dialogue_value).strip()
                    
                    # Get filming/audio instructions
                    filming_instructions = ""
                    if pd.notna(filming_value) and filming_col is not None:
                        filming_instructions = str(filming_value).strip()
                    
                    audio_instructions = ""
                    if pd.notna(audio_value) and audio_col is not None:
                        audio_instructions = str(audio_value).strip()
                    
                    # Combine filming and audio instructions
                    combined_instructions = ""
                    if filming_instructions and audio_instructions:
                        combined_instructions = f"{filming_instructions} / {audio_instructions}"
                    elif filming_instructions:
                        combined_instructions = filming_instructions
                    elif audio_instructions:
                        combined_instructions = audio_instructions
                    
                    # Skip empty entries
                    if len(character_name) == 0:
                        continue
                    
                    # Skip if character name looks like instruction
                    instruction_markers = ['・', '（', '）', '→', 'カット', '映像', 'テロップ', 
                                         'アニメ', '編集', '選手', '登場', 'シーン', '全種類', '紹介', '作り方', '演出']
                    
                    if any(marker in character_name for marker in instruction_markers):
                        continue
                    
                    # Skip if character name is too long (likely instruction)
                    if len(character_name) > 20:
                        continue
                    
                    # Clean dialogue text - remove instruction markers if they leaked in
                    if dialogue_text:
                        is_instruction = any(marker in dialogue_text for marker in instruction_markers)
                        if is_instruction:
                            dialogue_text = ""  # Clear contaminated dialogue
                    
                    # Skip very short dialogue unless we have instructions
                    if len(dialogue_text) < 3 and len(combined_instructions) == 0:
                        continue
                    
                    dialogue_entries.append({
                        'row_number': index,
                        'character_name': character_name,
                        'dialogue_text': dialogue_text,
                        'filming_audio_instructions': combined_instructions
                    })
            
            dialogue_count = sum(1 for entry in dialogue_entries if len(entry['dialogue_text']) > 0)
            instruction_count = sum(1 for entry in dialogue_entries if len(entry['filming_audio_instructions']) > 0)
            self.log_message(f"📝 {management_id}: セリフ{dialogue_count}件、指示{instruction_count}件を正しい列に抽出")
            return dialogue_entries if len(dialogue_entries) > 0 else None
            
        except Exception as e:
            self.log_message(f"❌ {management_id}: 抽出エラー - {str(e)}")
            return None
    
    def fix_contaminated_script(self, script_info):
        """Fix a script with instruction contamination using header-based extraction"""
        try:
            # Extract clean dialogue using header-based method
            new_dialogue = self.extract_header_based_data(
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
            
            # Delete all existing entries for this script (both empty and contaminated)
            cursor.execute("DELETE FROM character_dialogue_unified WHERE script_id = ?", (script_id,))
            deleted_count = cursor.rowcount
            
            # Insert new clean dialogue data
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
            
            self.log_message(f"✅ {script_info['management_id']}: {deleted_count}件削除、{inserted}件の純粋セリフ追加")
            return True
            
        except Exception as e:
            self.log_message(f"❌ {script_info['management_id']}: 修正エラー - {str(e)}")
            return False
    
    def get_all_scripts(self, limit=1000):
        """Get all scripts for complete header-based reconstruction"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get all scripts
            cursor.execute("""
                SELECT DISTINCT s.management_id, s.title, s.script_url
                FROM scripts s
                WHERE s.script_url IS NOT NULL AND s.script_url != ''
                ORDER BY s.management_id
                LIMIT ?
            """, (limit,))
            
            all_scripts = []
            for mgmt_id, title, script_url in cursor.fetchall():
                all_scripts.append({
                    'management_id': mgmt_id,
                    'title': title,
                    'script_url': script_url
                })
            
            conn.close()
            return all_scripts
            
        except Exception as e:
            self.log_message(f"❌ 全スクリプト取得エラー: {str(e)}")
            return []
    
    def complete_header_based_reconstruction(self, max_scripts=500):
        """Complete reconstruction using header-based extraction for all scripts"""
        self.log_message("=" * 80)
        self.log_message("全スプレッドシートのヘッダーベース完全再構築開始")
        self.log_message("=" * 80)
        
        # Get all scripts
        all_scripts = self.get_all_scripts(max_scripts)
        
        if not all_scripts:
            self.log_message("❌ 処理対象スクリプトが見つかりませんでした")
            return 0
        
        self.log_message(f"🎯 完全再構築対象: {len(all_scripts)}件")
        
        fixed_count = 0
        failed_count = 0
        header_found_count = 0
        
        for i, script in enumerate(all_scripts):
            self.log_message(f"🔧 再構築中 ({i+1}/{len(all_scripts)}): {script['management_id']}")
            
            # Try header-based extraction
            new_dialogue = self.extract_header_based_data(
                script['script_url'], 
                script['management_id']
            )
            
            if new_dialogue is not None:
                header_found_count += 1
                
                # Update database with clean extraction
                if self.fix_contaminated_script(script):
                    fixed_count += 1
                else:
                    failed_count += 1
            else:
                failed_count += 1
            
            # Rate limiting
            if i % 3 == 2:
                import time
                time.sleep(1)
        
        self.log_message("=" * 80)
        self.log_message(f"ヘッダーベース完全再構築結果:")
        self.log_message(f"  ヘッダー発見: {header_found_count}件")
        self.log_message(f"  再構築成功: {fixed_count}件")
        self.log_message(f"  再構築失敗: {failed_count}件")
        self.log_message("=" * 80)
        
        return fixed_count

def main():
    """Main execution function"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    extractor = HeaderBasedExtractor(db_path)
    
    print("=== ヘッダーベース完全再構築ツール ===")
    
    # Complete reconstruction for all scripts
    fixed_count = extractor.complete_header_based_reconstruction(max_scripts=800)
    
    if fixed_count > 0:
        print(f"\n✅ {fixed_count}件のスクリプトが完全再構築されました！")
    else:
        print(f"\n❌ 再構築できるスクリプトがありませんでした")

if __name__ == "__main__":
    main()
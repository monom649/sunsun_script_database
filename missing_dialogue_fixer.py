#!/usr/bin/env python3
"""
Missing Dialogue Fixer

This script identifies and fixes scripts with missing dialogue data by
re-extracting from original spreadsheets.
"""

import sqlite3
import requests
import pandas as pd
import io
import re
from datetime import datetime

class MissingDialogueFixer:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/missing_dialogue_fix_log.txt"
        
    def log_message(self, message: str):
        """Log messages with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())
    
    def identify_problematic_scripts(self):
        """Identify scripts with empty dialogue issues"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get scripts with empty dialogue
            cursor.execute("""
                SELECT DISTINCT s.management_id, s.title, s.script_url,
                       COUNT(cdu.id) as total_entries,
                       COUNT(CASE WHEN cdu.dialogue_text IS NULL OR cdu.dialogue_text = '' THEN 1 END) as empty_entries
                FROM scripts s
                JOIN character_dialogue_unified cdu ON s.id = cdu.script_id
                WHERE LENGTH(cdu.character_name) > 0
                GROUP BY s.id, s.management_id, s.title, s.script_url
                HAVING empty_entries > 0
                ORDER BY empty_entries DESC
            """)
            
            problematic_scripts = []
            for mgmt_id, title, script_url, total, empty in cursor.fetchall():
                empty_ratio = empty / total if total > 0 else 0
                problematic_scripts.append({
                    'management_id': mgmt_id,
                    'title': title,
                    'script_url': script_url,
                    'total_entries': total,
                    'empty_entries': empty,
                    'empty_ratio': empty_ratio
                })
            
            conn.close()
            
            self.log_message(f"🔍 問題のあるスクリプトを特定: {len(problematic_scripts)}件")
            
            # Show top 10 worst cases
            self.log_message("\n📊 問題の深刻なスクリプト (上位10件):")
            for script in problematic_scripts[:10]:
                self.log_message(f"  {script['management_id']}: {script['empty_entries']}/{script['total_entries']} empty ({script['empty_ratio']*100:.1f}%) - {script['title'][:50]}...")
            
            return problematic_scripts
            
        except Exception as e:
            self.log_message(f"❌ スクリプト特定エラー: {str(e)}")
            return []
    
    def extract_spreadsheet_data(self, script_url):
        """Extract data from a specific script URL"""
        if not script_url or 'docs.google.com' not in script_url:
            return None
        
        try:
            # Extract spreadsheet ID and GID from URL
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
            response = requests.get(csv_url, timeout=10)
            if response.status_code != 200:
                return None
            
            # Parse CSV
            csv_data = response.content.decode('utf-8')
            df = pd.read_csv(io.StringIO(csv_data))
            
            # Extract dialogue data based on standard format
            dialogue_entries = []
            
            for index, row in df.iterrows():
                if index < 4:  # Skip header rows
                    continue
                
                # Try different column patterns for dialogue extraction
                possible_patterns = [
                    # Pattern 1: Standard format
                    {'character_col': 4, 'dialogue_col': 5, 'instruction_col': 6},
                    # Pattern 2: Alternative format  
                    {'character_col': 'キャラクター', 'dialogue_col': 'セリフ', 'instruction_col': '音声指示'},
                    # Pattern 3: Positional
                    {'character_col': 'Unnamed: 4', 'dialogue_col': 'Unnamed: 5', 'instruction_col': 'Unnamed: 6'}
                ]
                
                for pattern in possible_patterns:
                    try:
                        character = row.get(pattern['character_col'], '')
                        dialogue = row.get(pattern['dialogue_col'], '')
                        instruction = row.get(pattern['instruction_col'], '')
                        
                        # Clean data
                        if pd.notna(character) and str(character).strip():
                            character = str(character).strip()
                            dialogue = str(dialogue).strip() if pd.notna(dialogue) else ''
                            instruction = str(instruction).strip() if pd.notna(instruction) else ''
                            
                            # Skip if character looks like instruction
                            if not any(char in character.lower() for char in ['テロップ', 'アニメ', 'カメラ', '映像']):
                                dialogue_entries.append({
                                    'row_number': index,
                                    'character_name': character,
                                    'dialogue_text': dialogue,
                                    'filming_audio_instructions': instruction
                                })
                                break
                    except Exception:
                        continue
            
            return dialogue_entries
            
        except Exception as e:
            self.log_message(f"❌ スプレッドシート抽出エラー {script_url}: {str(e)}")
            return None
    
    def fix_script_dialogue(self, script_info):
        """Fix dialogue for a specific script"""
        try:
            # Extract new dialogue data
            new_dialogue = self.extract_spreadsheet_data(script_info['script_url'])
            
            if not new_dialogue:
                self.log_message(f"⚠️  {script_info['management_id']}: データ抽出失敗")
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
                    self.log_message(f"⚠️  挿入エラー {script_info['management_id']}: {str(e)}")
            
            conn.commit()
            conn.close()
            
            self.log_message(f"✅ {script_info['management_id']}: {inserted}件のセリフを修正")
            return True
            
        except Exception as e:
            self.log_message(f"❌ スクリプト修正エラー {script_info['management_id']}: {str(e)}")
            return False
    
    def run_mass_fix(self, max_scripts=200):
        """Run mass fixing for problematic scripts"""
        self.log_message("=" * 80)
        self.log_message("大量セリフデータ修正開始")
        self.log_message("=" * 80)
        
        # Identify problematic scripts
        problematic_scripts = self.identify_problematic_scripts()
        
        if not problematic_scripts:
            self.log_message("✅ 修正が必要なスクリプトは見つかりませんでした")
            return
        
        # Focus on scripts with URLs that can be fixed
        fixable_scripts = [s for s in problematic_scripts if s.get('script_url')]
        
        self.log_message(f"🎯 修正対象: {len(fixable_scripts)}件 (URL有り)")
        
        # Fix scripts (limit to avoid timeout)
        fixed_count = 0
        error_count = 0
        
        for i, script in enumerate(fixable_scripts[:max_scripts]):
            self.log_message(f"🔧 修正中 ({i+1}/{min(max_scripts, len(fixable_scripts))}): {script['management_id']}")
            
            if self.fix_script_dialogue(script):
                fixed_count += 1
            else:
                error_count += 1
            
            # Rate limiting
            if i % 5 == 4:
                import time
                time.sleep(1)
        
        # Report results
        self.log_message("=" * 80)
        self.log_message(f"修正結果:")
        self.log_message(f"  修正成功: {fixed_count}件")
        self.log_message(f"  修正失敗: {error_count}件")
        self.log_message(f"  残り未修正: {len(fixable_scripts) - max_scripts}件")
        self.log_message("=" * 80)
        
        return fixed_count

def main():
    """Main execution function"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    fixer = MissingDialogueFixer(db_path)
    
    print("=== セリフデータ欠損修正ツール ===")
    
    # Run mass fix
    fixed_count = fixer.run_mass_fix(max_scripts=200)
    
    if fixed_count > 0:
        print(f"\n✅ {fixed_count}件のスクリプトのセリフデータを修正しました！")
    else:
        print(f"\n⚠️  修正できるスクリプトがありませんでした。")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Complete Extraction Executor

This script executes complete extraction of all 245 missing script dialogue data
from 作業進捗_new spreadsheet script URLs.
"""

import requests
import sqlite3
import pandas as pd
import io
import re
from datetime import datetime
import os
import time
import random

class CompleteExtractionExecutor:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/complete_extraction_log.txt"
        self.success_count = 0
        self.fail_count = 0
        self.total_dialogue_extracted = 0
        
    def log_progress(self, message):
        """Log extraction progress"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry + "\n")
        
        print(log_entry)
    
    def get_all_missing_scripts(self):
        """Get all scripts that have URLs but no dialogue data"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get all scripts with URLs but no dialogue data
            query = """
                SELECT s.id, s.management_id, s.title, s.script_url, s.broadcast_date
                FROM scripts s
                LEFT JOIN character_dialogue cd ON s.id = cd.script_id
                WHERE s.script_url IS NOT NULL 
                  AND s.script_url != ''
                  AND cd.script_id IS NULL
                ORDER BY s.broadcast_date DESC, s.management_id
            """
            
            cursor.execute(query)
            scripts = []
            
            for row in cursor.fetchall():
                scripts.append({
                    'id': row[0],
                    'management_id': row[1],
                    'title': row[2],
                    'script_url': row[3],
                    'broadcast_date': row[4] or 'Unknown'
                })
            
            conn.close()
            self.log_progress(f"Found {len(scripts)} scripts with missing dialogue data")
            return scripts
            
        except Exception as e:
            self.log_progress(f"❌ Error getting missing scripts: {str(e)}")
            return []
    
    def extract_spreadsheet_data(self, url):
        """Extract data from Google Spreadsheet URL"""
        try:
            # Extract spreadsheet ID and GID
            sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
            gid_pattern = r'[#&]gid=([0-9]+)'
            
            sheet_match = re.search(sheet_pattern, url)
            gid_match = re.search(gid_pattern, url)
            
            if not sheet_match:
                return None, "Invalid URL format"
            
            spreadsheet_id = sheet_match.group(1)
            gid = gid_match.group(1) if gid_match else '0'
            
            csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
            
            response = requests.get(csv_url, timeout=20)
            response.raise_for_status()
            
            csv_data = response.content.decode('utf-8')
            df = pd.read_csv(io.StringIO(csv_data))
            
            return df, "Success"
            
        except requests.exceptions.Timeout:
            return None, "Timeout"
        except requests.exceptions.HTTPError as e:
            return None, f"HTTP {e.response.status_code}"
        except Exception as e:
            return None, str(e)
    
    def extract_dialogue_from_dataframe(self, df, management_id):
        """Extract dialogue data from DataFrame with improved logic"""
        dialogue_entries = []
        
        if df is None or len(df) == 0:
            return dialogue_entries
        
        try:
            # Known character names
            characters = ['サンサン', 'くもりん', 'ツクモ', 'ノイズ', 'ママ', 'パパ', 'BB']
            
            for index, row in df.iterrows():
                try:
                    # Look for dialogue patterns across all columns
                    found_character = ""
                    found_dialogue = ""
                    found_voice = ""
                    
                    row_values = []
                    for col in df.columns:
                        cell_value = row.get(col)
                        if pd.notna(cell_value):
                            row_values.append(str(cell_value).strip())
                    
                    # Find character name
                    for val in row_values:
                        if val in characters and len(val) < 20:
                            found_character = val
                            break
                    
                    # Find dialogue content
                    for val in row_values:
                        if val and len(val) > 3:
                            # Check for dialogue patterns
                            if (any(pattern in val for pattern in ['だよ', 'です', 'ます', '！', '？', 'だね', 'かな']) 
                                or any(char in val for char in characters)
                                or '「' in val or '」' in val):
                                # Exclude obvious non-dialogue
                                if not any(exclude in val for exclude in ['タイトル', 'キャラクター', 'シーン', '使用おもちゃ', '撮影', 'カット', 'SE', 'BGM']):
                                    found_dialogue = val
                                    break
                    
                    # Find voice instruction
                    for val in row_values:
                        if val and len(val) < 30:
                            if any(voice_word in val for voice_word in ['元気', '悲しく', '驚き', '笑い', '怒り', '優しく', '楽しく']):
                                found_voice = val
                                break
                    
                    # If we found dialogue or character, add entry
                    if found_dialogue or (found_character and len(found_character) > 1):
                        dialogue_entries.append({
                            'management_id': management_id,
                            'row_number': index + 1,
                            'character_name': found_character,
                            'dialogue_text': found_dialogue,
                            'voice_instruction': found_voice
                        })
                
                except Exception as row_error:
                    continue
        
        except Exception as e:
            self.log_progress(f"Error extracting dialogue for {management_id}: {str(e)}")
        
        return dialogue_entries
    
    def save_dialogue_to_database(self, dialogue_entries):
        """Save dialogue entries to database"""
        if not dialogue_entries:
            return 0
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            inserted_count = 0
            
            for entry in dialogue_entries:
                try:
                    # Get script_id from management_id
                    cursor.execute("SELECT id FROM scripts WHERE management_id = ?", (entry['management_id'],))
                    script_result = cursor.fetchone()
                    
                    if script_result:
                        script_id = script_result[0]
                        
                        cursor.execute("""
                            INSERT INTO character_dialogue 
                            (script_id, row_number, character_name, dialogue_text, voice_instruction)
                            VALUES (?, ?, ?, ?, ?)
                        """, (
                            script_id,
                            entry['row_number'],
                            entry['character_name'] or '',
                            entry['dialogue_text'] or '',
                            entry['voice_instruction'] or ''
                        ))
                        inserted_count += 1
                
                except sqlite3.Error:
                    continue
            
            conn.commit()
            conn.close()
            
            return inserted_count
            
        except Exception as e:
            self.log_progress(f"Error saving dialogue: {str(e)}")
            return 0
    
    def process_script(self, script):
        """Process a single script"""
        mgmt_id = script['management_id']
        
        self.log_progress(f"Processing {mgmt_id}: {script['title'][:50]}...")
        
        # Extract data from URL
        df, status = self.extract_spreadsheet_data(script['script_url'])
        
        if df is not None:
            # Extract dialogue
            dialogue_entries = self.extract_dialogue_from_dataframe(df, mgmt_id)
            
            if dialogue_entries:
                # Save to database
                inserted = self.save_dialogue_to_database(dialogue_entries)
                if inserted > 0:
                    self.log_progress(f"   ✅ Success: {inserted} dialogue entries extracted")
                    self.success_count += 1
                    self.total_dialogue_extracted += inserted
                    return True
                else:
                    self.log_progress(f"   ⚠️ No data saved")
                    self.fail_count += 1
                    return False
            else:
                self.log_progress(f"   ⚠️ No dialogue found")
                self.fail_count += 1
                return False
        else:
            self.log_progress(f"   ❌ Failed: {status}")
            self.fail_count += 1
            return False
    
    def execute_complete_extraction(self):
        """Execute complete extraction of all missing dialogue data"""
        self.log_progress("="*80)
        self.log_progress("STARTING COMPLETE EXTRACTION OF ALL MISSING DIALOGUE DATA")
        self.log_progress("="*80)
        
        # Get all missing scripts
        missing_scripts = self.get_all_missing_scripts()
        
        if not missing_scripts:
            self.log_progress("No scripts with missing dialogue found")
            return
        
        self.log_progress(f"Starting extraction for {len(missing_scripts)} scripts...")
        
        # Process all scripts
        for i, script in enumerate(missing_scripts):
            script_num = i + 1
            
            self.log_progress(f"\n[{script_num}/{len(missing_scripts)}] Processing {script['management_id']}")
            
            # Process script
            success = self.process_script(script)
            
            # Progress reporting every 25 scripts
            if script_num % 25 == 0:
                completion_rate = self.success_count / script_num * 100 if script_num > 0 else 0
                self.log_progress(f"\n📊 Progress Report ({script_num}/{len(missing_scripts)}):")
                self.log_progress(f"   Success: {self.success_count}")
                self.log_progress(f"   Failed: {self.fail_count}")
                self.log_progress(f"   Success Rate: {completion_rate:.1f}%")
                self.log_progress(f"   Dialogue Extracted: {self.total_dialogue_extracted} lines")
            
            # Rate limiting
            delay = random.uniform(0.8, 2.0)
            time.sleep(delay)
        
        # Final summary
        self.log_progress("\n" + "="*80)
        self.log_progress("COMPLETE EXTRACTION FINISHED")
        self.log_progress("="*80)
        self.log_progress(f"📊 FINAL RESULTS:")
        self.log_progress(f"   Scripts processed: {len(missing_scripts)}")
        self.log_progress(f"   Successful extractions: {self.success_count}")
        self.log_progress(f"   Failed extractions: {self.fail_count}")
        self.log_progress(f"   Success rate: {self.success_count/len(missing_scripts)*100:.1f}%")
        self.log_progress(f"   Total dialogue lines extracted: {self.total_dialogue_extracted}")
        self.log_progress("="*80)

def main():
    """Main execution function"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found at: {db_path}")
        return
    
    executor = CompleteExtractionExecutor(db_path)
    executor.execute_complete_extraction()
    
    print("\n🎯 Complete extraction finished. Check complete_extraction_log.txt for detailed results.")

if __name__ == "__main__":
    main()
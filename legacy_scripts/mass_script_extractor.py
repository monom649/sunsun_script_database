#!/usr/bin/env python3
"""
Mass Script Extractor

This script performs large-scale extraction of dialogue data from all missing scripts
identified in the comprehensive audit.
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

class MassScriptExtractor:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/extraction_log.txt"
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
    
    def get_scripts_missing_dialogue(self):
        """Get all scripts that have URLs but no dialogue data"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
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
            self.log_progress(f"❌ Error getting scripts: {str(e)}")
            return []
    
    def extract_spreadsheet_id_and_gid(self, url):
        """Extract spreadsheet ID and GID from Google Sheets URL"""
        try:
            sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
            sheet_match = re.search(sheet_pattern, url)
            
            gid_pattern = r'[#&]gid=([0-9]+)'
            gid_match = re.search(gid_pattern, url)
            
            if sheet_match:
                spreadsheet_id = sheet_match.group(1)
                gid = gid_match.group(1) if gid_match else '0'
                return spreadsheet_id, gid
            
        except Exception as e:
            pass
        
        return None, None
    
    def fetch_script_content(self, url):
        """Fetch script content from Google Spreadsheet URL"""
        try:
            spreadsheet_id, gid = self.extract_spreadsheet_id_and_gid(url)
            if not spreadsheet_id:
                return None, "Invalid URL format"
            
            csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
            
            response = requests.get(csv_url, timeout=15)
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
    
    def extract_dialogue_data(self, df, management_id):
        """Extract dialogue data from script DataFrame"""
        dialogue_entries = []
        
        if df is None or len(df) == 0:
            return dialogue_entries
        
        try:
            # Look for dialogue patterns in all columns
            for index, row in df.iterrows():
                try:
                    character_name = ""
                    dialogue_text = ""
                    voice_instruction = ""
                    
                    # Search through all columns for data
                    for col in df.columns:
                        cell_value = str(row.get(col, ""))
                        if cell_value and cell_value != "nan":
                            # Check for character names (common patterns)
                            if any(char in cell_value for char in ['サンサン', 'くもりん', 'ツクモ', 'ノイズ', 'ママ', 'パパ']):
                                if not character_name and len(cell_value) < 20:  # Short text likely to be character name
                                    character_name = cell_value.strip()
                            
                            # Check for dialogue patterns
                            if any(pattern in cell_value for pattern in ['「', '」', 'です', 'だよ', 'ます', '！', '？']):
                                if len(cell_value) > 5 and not dialogue_text:  # Longer text likely to be dialogue
                                    dialogue_text = cell_value.strip()
                            
                            # Check for voice instructions
                            if any(pattern in cell_value for pattern in ['元気', '悲しく', '驚き', '笑い', '怒り']):
                                if len(cell_value) < 30 and not voice_instruction:
                                    voice_instruction = cell_value.strip()
                    
                    # If we found some meaningful content, add it
                    if dialogue_text or character_name:
                        dialogue_entries.append({
                            'management_id': management_id,
                            'row_number': index + 1,
                            'character_name': character_name,
                            'dialogue_text': dialogue_text,
                            'voice_instruction': voice_instruction
                        })
                
                except Exception as row_error:
                    continue
        
        except Exception as e:
            pass
        
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
                            entry['character_name'],
                            entry['dialogue_text'],
                            entry['voice_instruction']
                        ))
                        inserted_count += 1
                
                except sqlite3.Error:
                    continue
            
            conn.commit()
            conn.close()
            
            return inserted_count
            
        except Exception as e:
            return 0
    
    def process_scripts_batch(self, scripts, batch_size=50):
        """Process scripts in batches to avoid overwhelming the system"""
        total_scripts = len(scripts)
        
        for i in range(0, total_scripts, batch_size):
            batch = scripts[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_scripts + batch_size - 1) // batch_size
            
            self.log_progress(f"\n{'='*60}")
            self.log_progress(f"PROCESSING BATCH {batch_num}/{total_batches} ({len(batch)} scripts)")
            self.log_progress(f"{'='*60}")
            
            batch_success = 0
            batch_fail = 0
            batch_dialogue = 0
            
            for j, script in enumerate(batch):
                script_num = i + j + 1
                
                self.log_progress(f"[{script_num}/{total_scripts}] Processing {script['management_id']}: {script['title'][:40]}...")
                
                # Fetch script content
                df, status = self.fetch_script_content(script['script_url'])
                
                if df is not None:
                    # Extract dialogue data
                    dialogue_entries = self.extract_dialogue_data(df, script['management_id'])
                    
                    if dialogue_entries:
                        # Save to database
                        inserted = self.save_dialogue_to_database(dialogue_entries)
                        if inserted > 0:
                            self.log_progress(f"   ✅ Success: {inserted} dialogue entries extracted")
                            batch_success += 1
                            batch_dialogue += inserted
                        else:
                            self.log_progress(f"   ⚠️ No data saved")
                            batch_fail += 1
                    else:
                        self.log_progress(f"   ⚠️ No dialogue found")
                        batch_fail += 1
                else:
                    self.log_progress(f"   ❌ Failed: {status}")
                    batch_fail += 1
                
                # Rate limiting - random delay between 0.5-2 seconds
                delay = random.uniform(0.5, 2.0)
                time.sleep(delay)
            
            self.success_count += batch_success
            self.fail_count += batch_fail
            self.total_dialogue_extracted += batch_dialogue
            
            self.log_progress(f"\nBatch {batch_num} Results:")
            self.log_progress(f"   Success: {batch_success}/{len(batch)} ({batch_success/len(batch)*100:.1f}%)")
            self.log_progress(f"   Failed: {batch_fail}/{len(batch)}")
            self.log_progress(f"   Dialogue extracted: {batch_dialogue} lines")
            self.log_progress(f"\nCumulative Results:")
            self.log_progress(f"   Total Success: {self.success_count}")
            self.log_progress(f"   Total Failed: {self.fail_count}")
            self.log_progress(f"   Total Dialogue: {self.total_dialogue_extracted} lines")
            
            # Longer break between batches
            if i + batch_size < total_scripts:
                self.log_progress(f"Waiting 5 seconds before next batch...")
                time.sleep(5)
    
    def run_mass_extraction(self):
        """Run mass extraction of all missing dialogue data"""
        self.log_progress("="*80)
        self.log_progress("STARTING MASS SCRIPT EXTRACTION")
        self.log_progress("="*80)
        
        # Get all scripts missing dialogue
        scripts = self.get_scripts_missing_dialogue()
        
        if not scripts:
            self.log_progress("No scripts with missing dialogue found")
            return
        
        self.log_progress(f"Starting extraction for {len(scripts)} scripts...")
        self.log_progress(f"Estimated time: {len(scripts) * 2 / 60:.1f} minutes")
        
        # Process in batches
        self.process_scripts_batch(scripts, batch_size=25)  # Smaller batches for reliability
        
        # Final summary
        self.log_progress("\n" + "="*80)
        self.log_progress("MASS EXTRACTION COMPLETE")
        self.log_progress("="*80)
        self.log_progress(f"📊 FINAL RESULTS:")
        self.log_progress(f"   Scripts processed: {self.success_count + self.fail_count}")
        self.log_progress(f"   Successful extractions: {self.success_count}")
        self.log_progress(f"   Failed extractions: {self.fail_count}")
        self.log_progress(f"   Success rate: {self.success_count/(self.success_count + self.fail_count)*100:.1f}%")
        self.log_progress(f"   Total dialogue lines extracted: {self.total_dialogue_extracted}")
        self.log_progress("="*80)

def main():
    """Main extraction function"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found at: {db_path}")
        return
    
    extractor = MassScriptExtractor(db_path)
    extractor.run_mass_extraction()
    
    print("\n🎯 Mass extraction completed. Check extraction_log.txt for detailed results.")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Script Content Extractor

This script fetches script content from Google Spreadsheet URLs and extracts
dialogue, voice instructions, and filming instructions to populate the database.
"""

import requests
import sqlite3
import pandas as pd
import io
import re
from datetime import datetime
import time
import os

class ScriptContentExtractor:
    def __init__(self, db_path):
        self.db_path = db_path
        
    def extract_spreadsheet_id_and_gid(self, url):
        """Extract spreadsheet ID and GID from Google Sheets URL"""
        try:
            # Pattern for spreadsheet ID
            sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
            sheet_match = re.search(sheet_pattern, url)
            
            # Pattern for GID
            gid_pattern = r'[#&]gid=([0-9]+)'
            gid_match = re.search(gid_pattern, url)
            
            if sheet_match:
                spreadsheet_id = sheet_match.group(1)
                gid = gid_match.group(1) if gid_match else '0'
                return spreadsheet_id, gid
            
        except Exception as e:
            print(f"Error parsing URL {url}: {str(e)}")
        
        return None, None
    
    def fetch_script_content(self, url):
        """Fetch script content from Google Spreadsheet URL"""
        try:
            spreadsheet_id, gid = self.extract_spreadsheet_id_and_gid(url)
            if not spreadsheet_id:
                return None
            
            # Construct CSV export URL
            csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
            
            print(f"Fetching script content from: {csv_url}")
            
            # Fetch the CSV data
            response = requests.get(csv_url, timeout=30)
            response.raise_for_status()
            
            # Parse CSV data
            csv_data = response.content.decode('utf-8')
            df = pd.read_csv(io.StringIO(csv_data))
            
            return df
            
        except Exception as e:
            print(f"Error fetching script content from {url}: {str(e)}")
            return None
    
    def extract_dialogue_data(self, df, management_id):
        """Extract dialogue data from script DataFrame"""
        dialogue_entries = []
        
        if df is None or len(df) == 0:
            return dialogue_entries
        
        try:
            # Common column patterns for script data
            character_col = None
            dialogue_col = None
            voice_instruction_col = None
            filming_instruction_col = None
            
            # Search for relevant columns
            for col in df.columns:
                col_str = str(col).lower()
                if 'キャラ' in col_str or 'character' in col_str:
                    character_col = col
                elif 'セリフ' in col_str or 'dialogue' in col_str or 'せりふ' in col_str:
                    dialogue_col = col
                elif '音声' in col_str or '声' in col_str or 'voice' in col_str:
                    voice_instruction_col = col
                elif '撮影' in col_str or 'filming' in col_str or 'camera' in col_str:
                    filming_instruction_col = col
            
            # If no specific columns found, try to find data by content pattern
            if not dialogue_col:
                for col in df.columns:
                    # Look for columns with dialogue-like content
                    sample_data = df[col].dropna().astype(str)
                    if len(sample_data) > 0:
                        # Check if column contains dialogue patterns
                        dialogue_patterns = ['「', '」', 'サンサン', 'くもりん', 'こんにちは', 'です']
                        if any(pattern in ' '.join(sample_data.head(10).values) for pattern in dialogue_patterns):
                            dialogue_col = col
                            break
            
            # Extract data row by row
            for index, row in df.iterrows():
                try:
                    character_name = ""
                    dialogue_text = ""
                    voice_instruction = ""
                    filming_instruction = ""
                    
                    # Extract character name
                    if character_col and not pd.isna(row.get(character_col)):
                        character_name = str(row[character_col]).strip()
                    
                    # Extract dialogue
                    if dialogue_col and not pd.isna(row.get(dialogue_col)):
                        dialogue_text = str(row[dialogue_col]).strip()
                    
                    # Extract voice instruction
                    if voice_instruction_col and not pd.isna(row.get(voice_instruction_col)):
                        voice_instruction = str(row[voice_instruction_col]).strip()
                    
                    # Extract filming instruction
                    if filming_instruction_col and not pd.isna(row.get(filming_instruction_col)):
                        filming_instruction = str(row[filming_instruction_col]).strip()
                    
                    # If no specific columns, try to extract from any column with dialogue patterns
                    if not dialogue_text:
                        for col in df.columns:
                            cell_value = str(row.get(col, ""))
                            if cell_value and cell_value != "nan":
                                # Check for dialogue patterns
                                if any(pattern in cell_value for pattern in ['「', '」', 'サンサン', 'くもりん']):
                                    dialogue_text = cell_value.strip()
                                    break
                    
                    # Only add if we have some meaningful content
                    if dialogue_text or character_name:
                        dialogue_entries.append({
                            'management_id': management_id,
                            'row_number': index + 1,
                            'character_name': character_name,
                            'dialogue_text': dialogue_text,
                            'voice_instruction': voice_instruction,
                            'filming_instruction': filming_instruction
                        })
                
                except Exception as row_error:
                    print(f"Error processing row {index} for {management_id}: {str(row_error)}")
                    continue
        
        except Exception as e:
            print(f"Error extracting dialogue data for {management_id}: {str(e)}")
        
        return dialogue_entries
    
    def save_dialogue_to_database(self, dialogue_entries):
        """Save dialogue entries to database"""
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
                        
                        # Insert dialogue entry
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
                
                except sqlite3.Error as e:
                    print(f"Error inserting dialogue for {entry['management_id']}: {str(e)}")
            
            conn.commit()
            conn.close()
            
            return inserted_count
            
        except Exception as e:
            print(f"Error saving dialogue to database: {str(e)}")
            return 0
    
    def process_all_scripts(self, limit=None):
        """Process all scripts with URLs to extract content"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get scripts with URLs that don't have dialogue data yet
            query = """
                SELECT s.id, s.management_id, s.title, s.script_url
                FROM scripts s
                LEFT JOIN character_dialogue cd ON s.id = cd.script_id
                WHERE s.script_url IS NOT NULL 
                  AND s.script_url != ''
                  AND cd.script_id IS NULL
                ORDER BY s.management_id
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            cursor.execute(query)
            scripts_to_process = cursor.fetchall()
            conn.close()
            
            print(f"Found {len(scripts_to_process)} scripts to process")
            
            total_processed = 0
            total_dialogue_entries = 0
            
            for script_id, management_id, title, script_url in scripts_to_process:
                print(f"\nProcessing {management_id}: {title}")
                
                # Fetch script content
                df = self.fetch_script_content(script_url)
                
                if df is not None:
                    # Extract dialogue data
                    dialogue_entries = self.extract_dialogue_data(df, management_id)
                    
                    if dialogue_entries:
                        # Save to database
                        inserted = self.save_dialogue_to_database(dialogue_entries)
                        total_dialogue_entries += inserted
                        print(f"  Extracted {len(dialogue_entries)} entries, inserted {inserted}")
                    else:
                        print(f"  No dialogue data found")
                else:
                    print(f"  Failed to fetch content")
                
                total_processed += 1
                
                # Add delay to avoid rate limiting
                time.sleep(1)
            
            print(f"\nProcessing complete!")
            print(f"Scripts processed: {total_processed}")
            print(f"Total dialogue entries added: {total_dialogue_entries}")
            
            return total_processed, total_dialogue_entries
            
        except Exception as e:
            print(f"Error processing scripts: {str(e)}")
            return 0, 0
    
    def process_2025_q1_scripts(self):
        """Process specifically 2025 Q1 (Jan-Apr) scripts"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get 2025 Q1 scripts with URLs that don't have dialogue data yet
            query = """
                SELECT s.id, s.management_id, s.title, s.script_url
                FROM scripts s
                LEFT JOIN character_dialogue cd ON s.id = cd.script_id
                WHERE s.script_url IS NOT NULL 
                  AND s.script_url != ''
                  AND cd.script_id IS NULL
                  AND (s.broadcast_date LIKE '25/01/%' 
                       OR s.broadcast_date LIKE '25/02/%'
                       OR s.broadcast_date LIKE '25/03/%' 
                       OR s.broadcast_date LIKE '25/04/%')
                ORDER BY s.broadcast_date, s.management_id
            """
            
            cursor.execute(query)
            scripts_to_process = cursor.fetchall()
            conn.close()
            
            print(f"Found {len(scripts_to_process)} 2025 Q1 scripts to process")
            
            total_processed = 0
            total_dialogue_entries = 0
            
            for script_id, management_id, title, script_url in scripts_to_process:
                print(f"\nProcessing {management_id}: {title}")
                
                # Fetch script content
                df = self.fetch_script_content(script_url)
                
                if df is not None:
                    # Extract dialogue data
                    dialogue_entries = self.extract_dialogue_data(df, management_id)
                    
                    if dialogue_entries:
                        # Save to database
                        inserted = self.save_dialogue_to_database(dialogue_entries)
                        total_dialogue_entries += inserted
                        print(f"  Extracted {len(dialogue_entries)} entries, inserted {inserted}")
                    else:
                        print(f"  No dialogue data found")
                else:
                    print(f"  Failed to fetch content")
                
                total_processed += 1
                
                # Add delay to avoid rate limiting
                time.sleep(1)
            
            print(f"\n2025 Q1 processing complete!")
            print(f"Scripts processed: {total_processed}")
            print(f"Total dialogue entries added: {total_dialogue_entries}")
            
            return total_processed, total_dialogue_entries
            
        except Exception as e:
            print(f"Error processing 2025 Q1 scripts: {str(e)}")
            return 0, 0

def main():
    """Main execution function"""
    
    # Path to the database
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    if not os.path.exists(db_path):
        print(f"Database not found at: {db_path}")
        return
    
    extractor = ScriptContentExtractor(db_path)
    
    print("=== Script Content Extraction Process ===")
    
    # Process 2025 Q1 scripts specifically
    total_processed, total_dialogue = extractor.process_2025_q1_scripts()  # Process 2025 Q1 specifically
    
    print(f"\nExtraction complete!")
    print(f"Processed {total_processed} scripts")
    print(f"Added {total_dialogue} dialogue entries")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Data Cleaner

This script cleans and organizes the character_dialogue table by:
1. Removing invalid entries (headers, empty data)
2. Standardizing character names
3. Cleaning dialogue text
4. Organizing voice instructions
"""

import sqlite3
import re
import os

class DataCleaner:
    def __init__(self, db_path):
        self.db_path = db_path
        
    def analyze_current_data(self):
        """Analyze current data quality"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            print("=== Current Data Analysis ===")
            
            # Total rows
            cursor.execute("SELECT COUNT(*) FROM character_dialogue")
            total = cursor.fetchone()[0]
            print(f"Total dialogue rows: {total}")
            
            # Character analysis
            cursor.execute("""
                SELECT character_name, COUNT(*) as count
                FROM character_dialogue
                WHERE character_name IS NOT NULL AND character_name != ''
                GROUP BY character_name
                ORDER BY count DESC
                LIMIT 10
            """)
            
            print("\nTop characters:")
            valid_characters = []
            for char, count in cursor.fetchall():
                print(f"  {char}: {count} lines")
                if char in ['サンサン', 'くもりん', 'ツクモ', 'ノイズ']:
                    valid_characters.append(char)
            
            # Empty data analysis
            cursor.execute("SELECT COUNT(*) FROM character_dialogue WHERE character_name IS NULL OR character_name = ''")
            empty_chars = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM character_dialogue WHERE dialogue_text LIKE '%タイトル%' OR dialogue_text LIKE '%使用おもちゃ%' OR dialogue_text LIKE '%キャラクター%'")
            header_like = cursor.fetchone()[0]
            
            print(f"\nData quality issues:")
            print(f"  Empty character names: {empty_chars}")
            print(f"  Header-like entries: {header_like}")
            
            conn.close()
            return total, empty_chars, header_like
            
        except Exception as e:
            print(f"Error analyzing data: {str(e)}")
            return 0, 0, 0
    
    def identify_invalid_entries(self):
        """Identify entries that should be removed"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Patterns that indicate invalid entries
            invalid_patterns = [
                '%タイトル%',
                '%使用おもちゃ%',
                '%キャラクター%',
                '%シーン%',
                '%道具%',
                '%SE%',
                '%BGM%',
                '%テロップ%',
                '%カット%',
                '%編集%'
            ]
            
            invalid_ids = set()
            
            # Find entries with header-like content
            for pattern in invalid_patterns:
                cursor.execute("""
                    SELECT id FROM character_dialogue 
                    WHERE dialogue_text LIKE ?
                """, (pattern,))
                
                for row in cursor.fetchall():
                    invalid_ids.add(row[0])
            
            # Find entries with only numbers as character names
            cursor.execute("""
                SELECT id FROM character_dialogue 
                WHERE character_name REGEXP '^[0-9]+$'
            """)
            
            for row in cursor.fetchall():
                invalid_ids.add(row[0])
            
            # Find entries with very short dialogue that looks like headers
            cursor.execute("""
                SELECT id FROM character_dialogue 
                WHERE LENGTH(dialogue_text) < 10 
                AND (dialogue_text LIKE '%...' OR dialogue_text LIKE '%…')
            """)
            
            for row in cursor.fetchall():
                invalid_ids.add(row[0])
            
            conn.close()
            return list(invalid_ids)
            
        except Exception as e:
            print(f"Error identifying invalid entries: {str(e)}")
            return []
    
    def clean_character_names(self):
        """Standardize character names"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            print("Cleaning character names...")
            
            # Character name mappings
            name_mappings = {
                'サンサン': ['サンサン', 'sun sun', 'SunSun', 'sunsun'],
                'くもりん': ['くもりん', 'kumorin', 'Kumorin', 'クモリン'],
                'ツクモ': ['ツクモ', 'tsukumo', 'Tsukumo', 'つくも'],
                'ノイズ': ['ノイズ', 'noise', 'Noise', 'のいず'],
                'ママ': ['ママ', 'mama', 'Mama', 'お母さん', 'おかあさん'],
                'パパ': ['パパ', 'papa', 'Papa', 'お父さん', 'おとうさん']
            }
            
            updated_count = 0
            
            for standard_name, variations in name_mappings.items():
                for variation in variations:
                    cursor.execute("""
                        UPDATE character_dialogue 
                        SET character_name = ? 
                        WHERE character_name LIKE ?
                    """, (standard_name, variation))
                    
                    updated_count += cursor.rowcount
            
            conn.commit()
            conn.close()
            
            print(f"Updated {updated_count} character names")
            return updated_count
            
        except Exception as e:
            print(f"Error cleaning character names: {str(e)}")
            return 0
    
    def clean_dialogue_text(self):
        """Clean dialogue text"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            print("Cleaning dialogue text...")
            
            # Remove common prefixes/suffixes
            cursor.execute("""
                UPDATE character_dialogue 
                SET dialogue_text = TRIM(dialogue_text)
                WHERE dialogue_text != TRIM(dialogue_text)
            """)
            
            cleaned_count = cursor.rowcount
            
            # Remove entries that are clearly not dialogue
            cursor.execute("""
                DELETE FROM character_dialogue 
                WHERE dialogue_text IN ('', 'タイトル', 'キャラクター', 'シーン', 'SE', 'BGM')
                OR dialogue_text LIKE '%...%'
                AND LENGTH(dialogue_text) < 15
            """)
            
            deleted_count = cursor.rowcount
            
            conn.commit()
            conn.close()
            
            print(f"Cleaned {cleaned_count} dialogue entries")
            print(f"Deleted {deleted_count} invalid entries")
            
            return cleaned_count, deleted_count
            
        except Exception as e:
            print(f"Error cleaning dialogue text: {str(e)}")
            return 0, 0
    
    def remove_invalid_entries(self, invalid_ids):
        """Remove invalid entries from database"""
        if not invalid_ids:
            print("No invalid entries to remove")
            return 0
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            print(f"Removing {len(invalid_ids)} invalid entries...")
            
            # Remove invalid entries in batches
            batch_size = 1000
            removed_count = 0
            
            for i in range(0, len(invalid_ids), batch_size):
                batch = invalid_ids[i:i + batch_size]
                placeholders = ','.join(['?' for _ in batch])
                
                cursor.execute(f"""
                    DELETE FROM character_dialogue 
                    WHERE id IN ({placeholders})
                """, batch)
                
                removed_count += cursor.rowcount
            
            conn.commit()
            conn.close()
            
            print(f"Removed {removed_count} invalid entries")
            return removed_count
            
        except Exception as e:
            print(f"Error removing invalid entries: {str(e)}")
            return 0
    
    def organize_voice_instructions(self):
        """Clean and standardize voice instructions"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            print("Organizing voice instructions...")
            
            # Common voice instruction mappings
            voice_mappings = {
                '元気': ['元気', 'げんき', '明るく', '明るい'],
                '悲しい': ['悲しい', '悲しく', 'かなしい', '泣き', '泣く'],
                '驚き': ['驚き', '驚く', 'びっくり', '驚いて'],
                '怒り': ['怒り', '怒る', '怒って', 'いかり'],
                '笑い': ['笑い', '笑う', '笑って', 'わらい'],
                '優しく': ['優しく', '優しい', 'やさしく', 'やさしい']
            }
            
            updated_count = 0
            
            for standard_voice, variations in voice_mappings.items():
                for variation in variations:
                    cursor.execute("""
                        UPDATE character_dialogue 
                        SET voice_instruction = ? 
                        WHERE voice_instruction LIKE ?
                    """, (standard_voice, f'%{variation}%'))
                    
                    updated_count += cursor.rowcount
            
            conn.commit()
            conn.close()
            
            print(f"Standardized {updated_count} voice instructions")
            return updated_count
            
        except Exception as e:
            print(f"Error organizing voice instructions: {str(e)}")
            return 0
    
    def create_clean_summary(self):
        """Create summary of cleaned data"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            print("\n=== Cleaned Data Summary ===")
            
            # Total rows after cleaning
            cursor.execute("SELECT COUNT(*) FROM character_dialogue")
            total = cursor.fetchone()[0]
            print(f"Total dialogue rows after cleaning: {total}")
            
            # Character distribution
            cursor.execute("""
                SELECT character_name, COUNT(*) as count
                FROM character_dialogue
                WHERE character_name IS NOT NULL AND character_name != ''
                GROUP BY character_name
                ORDER BY count DESC
                LIMIT 10
            """)
            
            print("\nCharacter distribution:")
            for char, count in cursor.fetchall():
                print(f"  {char}: {count} lines")
            
            # Voice instruction distribution
            cursor.execute("""
                SELECT voice_instruction, COUNT(*) as count
                FROM character_dialogue
                WHERE voice_instruction IS NOT NULL AND voice_instruction != ''
                GROUP BY voice_instruction
                ORDER BY count DESC
                LIMIT 10
            """)
            
            print("\nVoice instruction distribution:")
            for voice, count in cursor.fetchall():
                print(f"  {voice}: {count} lines")
            
            # Sample cleaned data
            print("\nSample cleaned data:")
            cursor.execute("""
                SELECT s.management_id, cd.character_name, cd.dialogue_text, cd.voice_instruction
                FROM character_dialogue cd
                JOIN scripts s ON cd.script_id = s.id
                WHERE cd.character_name IN ('サンサン', 'くもりん', 'ツクモ', 'ノイズ')
                LIMIT 5
            """)
            
            for mgmt_id, character, dialogue, voice in cursor.fetchall():
                voice_display = voice if voice else '(なし)'
                print(f"  {mgmt_id}: {character} - \"{dialogue[:40]}...\" [{voice_display}]")
            
            conn.close()
            
        except Exception as e:
            print(f"Error creating summary: {str(e)}")
    
    def clean_all_data(self):
        """Execute complete data cleaning process"""
        print("=== Data Cleaning Process ===")
        
        # 1. Analyze current data
        self.analyze_current_data()
        
        # 2. Identify invalid entries
        print("\n--- Identifying Invalid Entries ---")
        invalid_ids = self.identify_invalid_entries()
        print(f"Found {len(invalid_ids)} invalid entries")
        
        # 3. Remove invalid entries
        print("\n--- Removing Invalid Entries ---")
        self.remove_invalid_entries(invalid_ids)
        
        # 4. Clean character names
        print("\n--- Cleaning Character Names ---")
        self.clean_character_names()
        
        # 5. Clean dialogue text
        print("\n--- Cleaning Dialogue Text ---")
        self.clean_dialogue_text()
        
        # 6. Organize voice instructions
        print("\n--- Organizing Voice Instructions ---")
        self.organize_voice_instructions()
        
        # 7. Create summary
        self.create_clean_summary()

def main():
    """Main execution function"""
    
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    if not os.path.exists(db_path):
        print(f"Database not found at: {db_path}")
        return
    
    cleaner = DataCleaner(db_path)
    cleaner.clean_all_data()
    
    print("\n=== Data Cleaning Complete ===")

if __name__ == "__main__":
    main()
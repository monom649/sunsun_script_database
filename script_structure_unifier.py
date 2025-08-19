#!/usr/bin/env python3
"""
Script Structure Unifier

This script unifies the script structure to:
- |キャラクター|セリフ| - Character names with their dialogue
- |撮影・音声指示| - All other content (filming/audio instructions)

Rules:
- Character column's right cell = always dialogue
- Everything else = filming/audio instructions
"""

import sqlite3
import re
from datetime import datetime

class ScriptStructureUnifier:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/structure_unify_log.txt"
        self.character_names = ['サンサン', 'くもりん', 'ツクモ', 'ノイズ', 'ママ', 'パパ', 'BB']
        self.processed_count = 0
        self.unified_count = 0
        
    def log_progress(self, message):
        """Log unification progress"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry + "\n")
        
        print(log_entry)
    
    def is_character_name(self, text):
        """Check if text is a character name"""
        if not text or not isinstance(text, str):
            return False
        
        text = text.strip()
        
        # Check exact match with known characters
        if text in self.character_names:
            return True
        
        # Check if text contains character name
        for char in self.character_names:
            if char in text and len(text) <= len(char) + 5:  # Allow some variation
                return True
        
        return False
    
    def is_dialogue_text(self, text):
        """Check if text is dialogue (not instruction)"""
        if not text or not isinstance(text, str):
            return False
        
        text = text.strip()
        
        # Dialogue patterns
        dialogue_indicators = [
            'だよ', 'です', 'ます', '！', '？', 'だね', 'かな', 'でしょ',
            '「', '」', 'わー', 'うー', 'えー', 'おー', 'あー'
        ]
        
        # Instruction patterns (NOT dialogue)
        instruction_indicators = [
            'カット', 'SE', 'BGM', '撮影', '編集', '指示', 'アップ', 'ズーム',
            '見切れ', '置く', '使用', '様子', '※', '【', '】', 'countifs',
            'TRUE', 'FALSE', '空欄', '下さい', 'ところは', 'みんなが',
            '広げて', 'トミカ', 'プラレール', 'ぬいぐるみ'
        ]
        
        # Check for instruction patterns first (higher priority)
        if any(indicator in text for indicator in instruction_indicators):
            return False
        
        # Check for dialogue patterns
        if any(indicator in text for indicator in dialogue_indicators):
            return True
        
        # If it's very short and doesn't contain instructions, likely dialogue
        if len(text) <= 20 and not any(indicator in text for indicator in instruction_indicators):
            return True
        
        return False
    
    def categorize_content(self, character_name, dialogue_text, voice_instruction):
        """Categorize content into character dialogue or instructions"""
        
        # Initialize results
        final_character = ""
        final_dialogue = ""
        final_instructions = []
        
        # Process character_name field
        if character_name and character_name.strip():
            if self.is_character_name(character_name):
                final_character = character_name.strip()
            else:
                # Not a character name, treat as instruction
                final_instructions.append(character_name.strip())
        
        # Process dialogue_text field
        if dialogue_text and dialogue_text.strip():
            text = dialogue_text.strip()
            
            # If we have a character and this looks like dialogue
            if final_character and self.is_dialogue_text(text):
                final_dialogue = text
            # If no character but this is clearly dialogue, extract character
            elif not final_character and self.is_dialogue_text(text):
                # Try to find character name in the text
                for char in self.character_names:
                    if char in text:
                        final_character = char
                        # Remove character name from dialogue
                        final_dialogue = text.replace(char, "").strip()
                        if final_dialogue.startswith("|") or final_dialogue.startswith("：") or final_dialogue.startswith(":"):
                            final_dialogue = final_dialogue[1:].strip()
                        break
                else:
                    # No character found, treat as instruction
                    final_instructions.append(text)
            else:
                # Not dialogue, treat as instruction
                final_instructions.append(text)
        
        # Process voice_instruction field
        if voice_instruction and voice_instruction.strip():
            final_instructions.append(voice_instruction.strip())
        
        return final_character, final_dialogue, " | ".join(final_instructions) if final_instructions else ""
    
    def analyze_current_structure(self):
        """Analyze current data structure"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            self.log_progress("=== ANALYZING CURRENT STRUCTURE ===")
            
            # Total entries
            cursor.execute("SELECT COUNT(*) FROM character_dialogue")
            total_entries = cursor.fetchone()[0]
            
            # Entries with character names
            cursor.execute("SELECT COUNT(*) FROM character_dialogue WHERE character_name IN ('サンサン', 'くもりん', 'ツクモ', 'ノイズ', 'ママ', 'パパ', 'BB')")
            proper_characters = cursor.fetchone()[0]
            
            # Sample problematic entries
            cursor.execute("""
                SELECT character_name, dialogue_text, voice_instruction 
                FROM character_dialogue 
                WHERE character_name NOT IN ('サンサン', 'くもりん', 'ツクモ', 'ノイズ', 'ママ', 'パパ', 'BB')
                LIMIT 10
            """)
            
            self.log_progress(f"Total entries: {total_entries:,}")
            self.log_progress(f"Proper character entries: {proper_characters:,}")
            self.log_progress(f"Entries needing restructure: {total_entries - proper_characters:,}")
            
            self.log_progress("\nSample problematic entries:")
            for row in cursor.fetchall():
                char, dialogue, voice = row
                self.log_progress(f"  CHAR: '{char}' | DIALOGUE: '{dialogue[:50]}...' | VOICE: '{voice[:30] if voice else 'None'}...'")
            
            conn.close()
            
        except Exception as e:
            self.log_progress(f"❌ Error analyzing structure: {str(e)}")
    
    def unify_structure(self):
        """Unify the script structure"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            self.log_progress("=== STARTING STRUCTURE UNIFICATION ===")
            
            # Get all entries
            cursor.execute("SELECT id, character_name, dialogue_text, voice_instruction FROM character_dialogue")
            all_entries = cursor.fetchall()
            
            self.log_progress(f"Processing {len(all_entries):,} entries...")
            
            # Create new unified table structure
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS character_dialogue_unified (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    script_id INTEGER REFERENCES scripts(id),
                    row_number INTEGER,
                    character_name TEXT,
                    dialogue_text TEXT,
                    filming_audio_instructions TEXT,
                    original_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            for entry_id, char_name, dialogue_text, voice_instruction in all_entries:
                self.processed_count += 1
                
                # Categorize content
                unified_char, unified_dialogue, unified_instructions = self.categorize_content(
                    char_name, dialogue_text, voice_instruction
                )
                
                # Get additional info for this entry
                cursor.execute("SELECT script_id, row_number FROM character_dialogue WHERE id = ?", (entry_id,))
                script_id, row_number = cursor.fetchone()
                
                # Insert unified data
                if unified_char and unified_dialogue:
                    # Character dialogue entry
                    cursor.execute("""
                        INSERT INTO character_dialogue_unified 
                        (script_id, row_number, character_name, dialogue_text, filming_audio_instructions, original_id)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (script_id, row_number, unified_char, unified_dialogue, unified_instructions, entry_id))
                    self.unified_count += 1
                elif unified_instructions:
                    # Instruction-only entry
                    cursor.execute("""
                        INSERT INTO character_dialogue_unified 
                        (script_id, row_number, character_name, dialogue_text, filming_audio_instructions, original_id)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (script_id, row_number, "", "", unified_instructions, entry_id))
                
                # Progress reporting
                if self.processed_count % 1000 == 0:
                    self.log_progress(f"   Processed: {self.processed_count:,} entries, Unified: {self.unified_count:,}")
            
            conn.commit()
            conn.close()
            
            self.log_progress(f"✅ Structure unification complete!")
            self.log_progress(f"   Total processed: {self.processed_count:,}")
            self.log_progress(f"   Unified entries: {self.unified_count:,}")
            
        except Exception as e:
            self.log_progress(f"❌ Error during unification: {str(e)}")
    
    def verify_unified_structure(self):
        """Verify the unified structure"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            self.log_progress("=== VERIFYING UNIFIED STRUCTURE ===")
            
            # Check unified table
            cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified")
            total_unified = cursor.fetchone()[0]
            
            # Character dialogue entries
            cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE character_name != '' AND dialogue_text != ''")
            character_entries = cursor.fetchone()[0]
            
            # Instruction entries
            cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE filming_audio_instructions != ''")
            instruction_entries = cursor.fetchone()[0]
            
            self.log_progress(f"Total unified entries: {total_unified:,}")
            self.log_progress(f"Character dialogue entries: {character_entries:,}")
            self.log_progress(f"Instruction entries: {instruction_entries:,}")
            
            # Sample unified entries
            self.log_progress("\nSample character dialogue entries:")
            cursor.execute("""
                SELECT character_name, dialogue_text, filming_audio_instructions 
                FROM character_dialogue_unified 
                WHERE character_name != '' AND dialogue_text != '' 
                LIMIT 5
            """)
            for row in cursor.fetchall():
                char, dialogue, instructions = row
                self.log_progress(f"  {char}: '{dialogue}' | Instructions: '{instructions[:50]}...' ")
            
            self.log_progress("\nSample instruction entries:")
            cursor.execute("""
                SELECT filming_audio_instructions 
                FROM character_dialogue_unified 
                WHERE character_name = '' AND dialogue_text = '' AND filming_audio_instructions != ''
                LIMIT 5
            """)
            for row in cursor.fetchall():
                instructions = row[0]
                self.log_progress(f"  Instructions: '{instructions[:100]}...'")
            
            conn.close()
            
        except Exception as e:
            self.log_progress(f"❌ Error verifying structure: {str(e)}")
    
    def execute_unification(self):
        """Execute complete structure unification"""
        self.log_progress("=" * 80)
        self.log_progress("SCRIPT STRUCTURE UNIFICATION")
        self.log_progress("=" * 80)
        
        # Step 1: Analyze current structure
        self.analyze_current_structure()
        
        # Step 2: Unify structure
        self.unify_structure()
        
        # Step 3: Verify results
        self.verify_unified_structure()
        
        self.log_progress("=" * 80)
        self.log_progress("STRUCTURE UNIFICATION COMPLETE")
        self.log_progress("=" * 80)

def main():
    """Main execution function"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    unifier = ScriptStructureUnifier(db_path)
    unifier.execute_unification()
    
    print("\n🎯 Structure unification completed. Check structure_unify_log.txt for detailed results.")

if __name__ == "__main__":
    main()
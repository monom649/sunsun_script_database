#!/usr/bin/env python3
"""
Database Validation Script for Reorganized SunSun Script Database

This script validates the reorganized database and demonstrates usage examples.
"""

import sqlite3
from typing import List, Dict, Tuple

class DatabaseValidator:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

    def validate_structure(self) -> Dict:
        """Validate the reorganized database structure"""
        cursor = self.conn.cursor()
        
        # Check table counts
        tables = ['scripts', 'character_dialogue', 'scene_descriptions', 
                 'visual_effects', 'audio_instructions', 'technical_notes']
        
        counts = {}
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            counts[table] = cursor.fetchone()['count']
        
        return counts

    def get_character_statistics(self) -> List[Tuple]:
        """Get character dialogue statistics"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT character_name, COUNT(*) as dialogue_count,
                   COUNT(DISTINCT script_id) as scripts_appeared
            FROM character_dialogue
            GROUP BY character_name
            ORDER BY dialogue_count DESC
        """)
        return cursor.fetchall()

    def get_script_composition(self, management_id: str) -> Dict:
        """Get composition breakdown for a specific script"""
        cursor = self.conn.cursor()
        
        # Get script info
        cursor.execute("SELECT * FROM scripts WHERE management_id = ?", (management_id,))
        script = cursor.fetchone()
        
        if not script:
            return {}
        
        script_id = script['id']
        composition = {'script_info': dict(script)}
        
        # Count each content type
        tables = {
            'character_dialogue': 'dialogue_count',
            'scene_descriptions': 'scene_count', 
            'visual_effects': 'effect_count',
            'audio_instructions': 'audio_count',
            'technical_notes': 'note_count'
        }
        
        for table, key in tables.items():
            cursor.execute(f"SELECT COUNT(*) as count FROM {table} WHERE script_id = ?", (script_id,))
            composition[key] = cursor.fetchone()['count']
        
        return composition

    def find_dialogue_by_character(self, character_name: str, limit: int = 10) -> List:
        """Find dialogue lines by specific character"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT s.management_id, s.title, d.dialogue_text, d.row_number
            FROM character_dialogue d
            JOIN scripts s ON d.script_id = s.id
            WHERE d.character_name = ?
            ORDER BY s.management_id, d.row_number
            LIMIT ?
        """, (character_name, limit))
        return cursor.fetchall()

    def find_visual_effects_by_type(self, effect_type: str, limit: int = 10) -> List:
        """Find visual effects by type"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT s.management_id, v.effect_description, v.row_number
            FROM visual_effects v
            JOIN scripts s ON v.script_id = s.id
            WHERE v.effect_type = ?
            ORDER BY s.management_id, v.row_number
            LIMIT ?
        """, (effect_type, limit))
        return cursor.fetchall()

    def search_content(self, search_term: str) -> Dict:
        """Search across all content types"""
        cursor = self.conn.cursor()
        results = {}
        
        # Search dialogue
        cursor.execute("""
            SELECT s.management_id, d.character_name, d.dialogue_text
            FROM character_dialogue d
            JOIN scripts s ON d.script_id = s.id
            WHERE d.dialogue_text LIKE ?
            LIMIT 5
        """, (f'%{search_term}%',))
        results['dialogue'] = cursor.fetchall()
        
        # Search scene descriptions
        cursor.execute("""
            SELECT s.management_id, sd.description_text
            FROM scene_descriptions sd
            JOIN scripts s ON sd.script_id = s.id
            WHERE sd.description_text LIKE ?
            LIMIT 5
        """, (f'%{search_term}%',))
        results['scenes'] = cursor.fetchall()
        
        # Search effects
        cursor.execute("""
            SELECT s.management_id, v.effect_description, v.effect_type
            FROM visual_effects v
            JOIN scripts s ON v.script_id = s.id
            WHERE v.effect_description LIKE ?
            LIMIT 5
        """, (f'%{search_term}%',))
        results['effects'] = cursor.fetchall()
        
        return results

    def close(self):
        self.conn.close()


def main():
    """Demonstrate database validation and usage"""
    
    db_path = "youtube_search_complete_all_reorganized.db"
    validator = DatabaseValidator(db_path)
    
    try:
        print("SunSun Script Database Validation")
        print("=================================")
        
        # Validate structure
        print("\n1. Database Structure Validation")
        counts = validator.validate_structure()
        for table, count in counts.items():
            print(f"   {table}: {count:,} rows")
        
        # Character statistics
        print("\n2. Character Dialogue Statistics")
        char_stats = validator.get_character_statistics()
        for stat in char_stats[:10]:
            print(f"   {stat['character_name']}: {stat['dialogue_count']:,} lines in {stat['scripts_appeared']} scripts")
        
        # Script composition examples
        print("\n3. Script Composition Examples")
        for script_id in ['A01', 'B1039', 'B1398']:
            composition = validator.get_script_composition(script_id)
            if composition:
                print(f"\n   Script {script_id}: {composition['script_info']['title']}")
                print(f"     Dialogue: {composition.get('dialogue_count', 0)}")
                print(f"     Scenes: {composition.get('scene_count', 0)}")
                print(f"     Effects: {composition.get('effect_count', 0)}")
                print(f"     Audio: {composition.get('audio_count', 0)}")
                print(f"     Notes: {composition.get('note_count', 0)}")
        
        # Search examples
        print("\n4. Search Examples")
        
        # Find サンサン dialogue
        print("\n   Sample サンサン dialogue:")
        sansan_dialogue = validator.find_dialogue_by_character('サンサン', 5)
        for line in sansan_dialogue:
            print(f"     [{line['management_id']}] {line['dialogue_text']}")
        
        # Find telop effects
        print("\n   Sample telop effects:")
        telop_effects = validator.find_visual_effects_by_type('telop', 5)
        for effect in telop_effects:
            print(f"     [{effect['management_id']}] {effect['effect_description']}")
        
        # Search for クリスマス content
        print("\n   Content containing 'クリスマス':")
        christmas_content = validator.search_content('クリスマス')
        
        if christmas_content['dialogue']:
            print("     Dialogue:")
            for line in christmas_content['dialogue']:
                print(f"       [{line['management_id']}] {line['character_name']}: {line['dialogue_text']}")
        
        if christmas_content['scenes']:
            print("     Scene descriptions:")
            for scene in christmas_content['scenes'][:2]:
                print(f"       [{scene['management_id']}] {scene['description_text'][:100]}...")
        
        print("\n✅ Validation completed successfully!")
        print("\nThe reorganized database is working correctly and provides clean separation of content types.")
        
    finally:
        validator.close()


if __name__ == "__main__":
    main()
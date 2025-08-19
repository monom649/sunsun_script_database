#!/usr/bin/env python3
"""
Database Reorganizer for SunSun Script Database

This script analyzes and reorganizes the YouTube script database to properly separate:
1. Actual character dialogue
2. Situational descriptions/scene instructions
3. Technical instructions (filming, voice, editing)
4. Visual effects and audio instructions

Author: Claude Code Assistant
Date: 2025-08-19
"""

import sqlite3
import re
import json
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime

@dataclass
class ContentClassification:
    """Classification result for database content"""
    content_type: str  # 'dialogue', 'scene_description', 'tech_instruction', 'visual_effect', 'audio_instruction'
    confidence: float  # 0.0 to 1.0
    reasoning: str
    suggested_column: str

class DatabaseReorganizer:
    """Main class for reorganizing the script database"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        
        # Classification patterns
        self.dialogue_patterns = [
            r'[！？。〜♪]+$',  # Ends with punctuation typical of dialogue
            r'^[「『].*[」』]$',  # Quoted text
            r'[あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほまみむめもやゆよらりるれろわをん]+[！？。〜♪]',  # Japanese with emotional punctuation
        ]
        
        self.scene_description_patterns = [
            r'^[^「『]*[のをに][いるある]',  # Descriptive sentences
            r'カメラ|撮影|アップ|全体|登場|の様子',  # Camera/filming terms
            r'^.*[がは].*[するている]',  # Action descriptions
            r'ペットボトル|プール|全体像|手をあげる',  # Scene elements
        ]
        
        self.tech_instruction_patterns = [
            r'BGM|SE|音楽|効果音|audiostock',  # Audio instructions
            r'テロップ|CG|エフェクト|フェード|アイリス',  # Visual effects
            r'撮影|カット|編集|※|★',  # Technical notes
            r'https?://|\.mp3|\.wav|gigafile',  # File references
        ]
        
        self.visual_effect_patterns = [
            r'テロップ|エフェクト|CG|稲光|電撃|アニメーション',
            r'スロー|モノクロ|フェード|ズーム|パン',
            r'光|キラキラ|ぼわん|ドカーン',
        ]
        
        self.audio_instruction_patterns = [
            r'BGM|SE|効果音|音楽|サウンド',
            r'ちゃんちゃん|シャキーン|ドンッ|キキーッ',
            r'audiostock|gigafile|\.mp3|\.wav',
        ]

    def analyze_content_patterns(self) -> Dict:
        """Analyze existing content patterns in the database"""
        cursor = self.conn.cursor()
        
        analysis = {
            'total_rows': 0,
            'column_distribution': {},
            'character_distribution': {},
            'content_samples': {},
            'classification_results': {}
        }
        
        # Get total rows
        cursor.execute("SELECT COUNT(*) FROM script_lines")
        analysis['total_rows'] = cursor.fetchone()[0]
        
        # Column distribution
        cursor.execute("""
            SELECT dialogue_column, COUNT(*) as count 
            FROM script_lines 
            GROUP BY dialogue_column 
            ORDER BY count DESC
        """)
        analysis['column_distribution'] = dict(cursor.fetchall())
        
        # Character distribution
        cursor.execute("""
            SELECT character_name, COUNT(*) as count 
            FROM script_lines 
            WHERE character_name IS NOT NULL AND character_name != ''
            GROUP BY character_name 
            ORDER BY count DESC
            LIMIT 20
        """)
        analysis['character_distribution'] = dict(cursor.fetchall())
        
        # Sample content by column
        for column in ['E', 'F', 'G', 'H', 'I', 'J']:
            cursor.execute("""
                SELECT character_name, dialogue 
                FROM script_lines 
                WHERE dialogue_column = ? 
                LIMIT 10
            """, (column,))
            analysis['content_samples'][column] = cursor.fetchall()
        
        return analysis

    def classify_content(self, character_name: str, dialogue: str, column: str) -> ContentClassification:
        """Classify content based on patterns and context"""
        
        if not dialogue or dialogue.strip() == '':
            return ContentClassification('empty', 1.0, 'Empty content', 'ignore')
        
        dialogue = dialogue.strip()
        
        # Check for actual character dialogue
        if character_name and character_name in ['サンサン', 'くもりん', 'ツクモ', 'ノイズ', 'プリル']:
            # Strong indicators of actual dialogue
            if any(re.search(pattern, dialogue) for pattern in self.dialogue_patterns):
                if not any(re.search(pattern, dialogue) for pattern in self.tech_instruction_patterns):
                    return ContentClassification('dialogue', 0.9, 'Character with dialogue patterns', 'character_dialogue')
        
        # Check for technical instructions
        if any(re.search(pattern, dialogue) for pattern in self.tech_instruction_patterns):
            if any(re.search(pattern, dialogue) for pattern in self.audio_instruction_patterns):
                return ContentClassification('audio_instruction', 0.8, 'Audio/music instruction', 'audio_instructions')
            elif any(re.search(pattern, dialogue) for pattern in self.visual_effect_patterns):
                return ContentClassification('visual_effect', 0.8, 'Visual effect instruction', 'visual_effects')
            else:
                return ContentClassification('tech_instruction', 0.7, 'Technical instruction', 'technical_notes')
        
        # Check for scene descriptions
        if any(re.search(pattern, dialogue) for pattern in self.scene_description_patterns):
            return ContentClassification('scene_description', 0.7, 'Scene/situational description', 'scene_descriptions')
        
        # Default classification based on character presence
        if character_name and character_name != '':
            return ContentClassification('dialogue', 0.5, 'Has character name', 'character_dialogue')
        else:
            return ContentClassification('scene_description', 0.5, 'No character, likely description', 'scene_descriptions')

    def create_optimized_schema(self) -> str:
        """Create SQL for optimized database schema"""
        
        schema_sql = """
        -- Optimized Schema for SunSun Script Database
        
        -- Main scripts table (metadata)
        CREATE TABLE IF NOT EXISTS scripts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            management_id TEXT UNIQUE NOT NULL,
            title TEXT,
            broadcast_date TEXT,
            script_url TEXT,
            source_sheet TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Character dialogue table (actual spoken lines)
        CREATE TABLE IF NOT EXISTS character_dialogue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            script_id INTEGER REFERENCES scripts(id),
            row_number INTEGER,
            character_name TEXT NOT NULL,
            dialogue_text TEXT NOT NULL,
            voice_instruction TEXT,
            original_column TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Scene descriptions table (situational/environmental descriptions)
        CREATE TABLE IF NOT EXISTS scene_descriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            script_id INTEGER REFERENCES scripts(id),
            row_number INTEGER,
            description_text TEXT NOT NULL,
            filming_instruction TEXT,
            original_column TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Visual effects table (CG, animations, visual elements)
        CREATE TABLE IF NOT EXISTS visual_effects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            script_id INTEGER REFERENCES scripts(id),
            row_number INTEGER,
            effect_description TEXT NOT NULL,
            effect_type TEXT, -- 'telop', 'cg', 'animation', 'transition', etc.
            timing_note TEXT,
            original_column TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Audio instructions table (BGM, SE, music)
        CREATE TABLE IF NOT EXISTS audio_instructions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            script_id INTEGER REFERENCES scripts(id),
            row_number INTEGER,
            audio_description TEXT NOT NULL,
            audio_type TEXT, -- 'bgm', 'se', 'voice_note', etc.
            file_reference TEXT,
            timing_note TEXT,
            original_column TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Technical notes table (general production notes)
        CREATE TABLE IF NOT EXISTS technical_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            script_id INTEGER REFERENCES scripts(id),
            row_number INTEGER,
            note_text TEXT NOT NULL,
            note_type TEXT, -- 'filming', 'editing', 'general', etc.
            original_column TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Indexes for performance
        CREATE INDEX IF NOT EXISTS idx_character_dialogue_script ON character_dialogue(script_id);
        CREATE INDEX IF NOT EXISTS idx_character_dialogue_character ON character_dialogue(character_name);
        CREATE INDEX IF NOT EXISTS idx_scene_descriptions_script ON scene_descriptions(script_id);
        CREATE INDEX IF NOT EXISTS idx_visual_effects_script ON visual_effects(script_id);
        CREATE INDEX IF NOT EXISTS idx_audio_instructions_script ON audio_instructions(script_id);
        CREATE INDEX IF NOT EXISTS idx_technical_notes_script ON technical_notes(script_id);
        CREATE INDEX IF NOT EXISTS idx_scripts_management_id ON scripts(management_id);
        """
        
        return schema_sql

    def reorganize_database(self, output_path: str = None) -> Dict:
        """Reorganize the database into the new optimized structure"""
        
        if output_path is None:
            output_path = self.db_path.replace('.db', '_reorganized.db')
        
        # Create new database with optimized schema
        new_conn = sqlite3.connect(output_path)
        new_conn.executescript(self.create_optimized_schema())
        
        # Get all unique scripts
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT DISTINCT management_id, title, broadcast_date, script_url, source_sheet
            FROM script_lines
            ORDER BY management_id
        """)
        
        scripts = cursor.fetchall()
        script_id_map = {}
        
        # Insert scripts
        for script in scripts:
            new_cursor = new_conn.cursor()
            new_cursor.execute("""
                INSERT OR IGNORE INTO scripts (management_id, title, broadcast_date, script_url, source_sheet)
                VALUES (?, ?, ?, ?, ?)
            """, script)
            if new_cursor.lastrowid:
                script_id_map[script[0]] = new_cursor.lastrowid
            else:
                # Get existing ID
                new_cursor.execute("SELECT id FROM scripts WHERE management_id = ?", (script[0],))
                script_id_map[script[0]] = new_cursor.fetchone()[0]
        
        # Process all script lines
        cursor.execute("SELECT * FROM script_lines ORDER BY management_id, row_number")
        
        stats = {
            'total_processed': 0,
            'character_dialogue': 0,
            'scene_descriptions': 0,
            'visual_effects': 0,
            'audio_instructions': 0,
            'technical_notes': 0,
            'empty_ignored': 0
        }
        
        for row in cursor:
            stats['total_processed'] += 1
            
            script_id = script_id_map[row['management_id']]
            classification = self.classify_content(
                row['character_name'], 
                row['dialogue'], 
                row['dialogue_column']
            )
            
            new_cursor = new_conn.cursor()
            
            if classification.content_type == 'dialogue':
                new_cursor.execute("""
                    INSERT INTO character_dialogue 
                    (script_id, row_number, character_name, dialogue_text, voice_instruction, original_column)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (script_id, row['row_number'], row['character_name'], 
                     row['dialogue'], row['voice_instruction'], row['dialogue_column']))
                stats['character_dialogue'] += 1
                
            elif classification.content_type == 'scene_description':
                new_cursor.execute("""
                    INSERT INTO scene_descriptions 
                    (script_id, row_number, description_text, filming_instruction, original_column)
                    VALUES (?, ?, ?, ?, ?)
                """, (script_id, row['row_number'], row['dialogue'], 
                     row['filming_instruction'], row['dialogue_column']))
                stats['scene_descriptions'] += 1
                
            elif classification.content_type == 'visual_effect':
                effect_type = self._detect_effect_type(row['dialogue'])
                new_cursor.execute("""
                    INSERT INTO visual_effects 
                    (script_id, row_number, effect_description, effect_type, original_column)
                    VALUES (?, ?, ?, ?, ?)
                """, (script_id, row['row_number'], row['dialogue'], 
                     effect_type, row['dialogue_column']))
                stats['visual_effects'] += 1
                
            elif classification.content_type == 'audio_instruction':
                audio_type = self._detect_audio_type(row['dialogue'])
                file_ref = self._extract_file_reference(row['dialogue'])
                new_cursor.execute("""
                    INSERT INTO audio_instructions 
                    (script_id, row_number, audio_description, audio_type, file_reference, original_column)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (script_id, row['row_number'], row['dialogue'], 
                     audio_type, file_ref, row['dialogue_column']))
                stats['audio_instructions'] += 1
                
            elif classification.content_type == 'tech_instruction':
                note_type = 'general'
                if row['filming_instruction']:
                    note_type = 'filming'
                elif row['editing_instruction']:
                    note_type = 'editing'
                    
                new_cursor.execute("""
                    INSERT INTO technical_notes 
                    (script_id, row_number, note_text, note_type, original_column)
                    VALUES (?, ?, ?, ?, ?)
                """, (script_id, row['row_number'], row['dialogue'], 
                     note_type, row['dialogue_column']))
                stats['technical_notes'] += 1
                
            else:  # empty
                stats['empty_ignored'] += 1
            
            if stats['total_processed'] % 10000 == 0:
                print(f"Processed {stats['total_processed']} rows...")
                new_conn.commit()
        
        new_conn.commit()
        new_conn.close()
        
        return stats

    def _detect_effect_type(self, text: str) -> str:
        """Detect the type of visual effect"""
        if 'テロップ' in text:
            return 'telop'
        elif 'エフェクト' in text:
            return 'effect'
        elif 'アニメ' in text:
            return 'animation'
        elif 'フェード' in text or 'アイリス' in text:
            return 'transition'
        else:
            return 'general'

    def _detect_audio_type(self, text: str) -> str:
        """Detect the type of audio instruction"""
        if 'BGM' in text or '音楽' in text:
            return 'bgm'
        elif 'SE' in text or '効果音' in text:
            return 'se'
        elif 'ちゃんちゃん' in text or 'シャキーン' in text:
            return 'se'
        else:
            return 'general'

    def _extract_file_reference(self, text: str) -> Optional[str]:
        """Extract file references from text"""
        url_match = re.search(r'https?://[^\s]+', text)
        if url_match:
            return url_match.group()
        
        file_match = re.search(r'audiostock_\d+', text)
        if file_match:
            return file_match.group()
        
        return None

    def generate_report(self, analysis: Dict, stats: Dict) -> str:
        """Generate a detailed analysis report"""
        
        report = f"""
# SunSun Script Database Analysis and Reorganization Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Original Database Analysis

### Overview
- **Total rows**: {analysis['total_rows']:,}
- **Unique scripts**: {len([k for k in analysis['column_distribution'].keys() if k])}

### Column Distribution
"""
        for column, count in analysis['column_distribution'].items():
            percentage = (count / analysis['total_rows']) * 100
            report += f"- **Column {column}**: {count:,} rows ({percentage:.1f}%)\n"
        
        report += f"""

### Character Distribution (Top Characters)
"""
        for character, count in list(analysis['character_distribution'].items())[:10]:
            report += f"- **{character}**: {count:,} lines\n"
        
        report += f"""

### Content Analysis Findings

Based on pattern analysis, the original 'dialogue' column contains:

1. **Actual Character Dialogue**: Spoken lines by characters (サンサン, くもりん, etc.)
   - Located primarily in columns E, F, D, C
   - Identifiable by punctuation patterns (！？〜♪)
   - Associated with character names

2. **Scene Descriptions**: Environmental and situational descriptions
   - Camera angles, character positions, scene setup
   - Often in columns G, H for B-series scripts
   - No character names, descriptive language

3. **Technical Instructions**: Production notes
   - Filming directions, editing notes
   - Audio references (BGM, SE, file links)
   - Visual effects descriptions

4. **Mixed Content Issues**:
   - Column E contains both dialogue AND descriptions
   - Different script formats (A01 vs B1xxx) use different layouts
   - Instructions mixed with actual dialogue content

## Reorganization Results

### Statistics After Reorganization
- **Total processed**: {stats['total_processed']:,} rows
- **Character dialogue**: {stats['character_dialogue']:,} rows ({(stats['character_dialogue']/stats['total_processed']*100):.1f}%)
- **Scene descriptions**: {stats['scene_descriptions']:,} rows ({(stats['scene_descriptions']/stats['total_processed']*100):.1f}%)
- **Visual effects**: {stats['visual_effects']:,} rows ({(stats['visual_effects']/stats['total_processed']*100):.1f}%)
- **Audio instructions**: {stats['audio_instructions']:,} rows ({(stats['audio_instructions']/stats['total_processed']*100):.1f}%)
- **Technical notes**: {stats['technical_notes']:,} rows ({(stats['technical_notes']/stats['total_processed']*100):.1f}%)
- **Empty/ignored**: {stats['empty_ignored']:,} rows ({(stats['empty_ignored']/stats['total_processed']*100):.1f}%)

### New Database Structure

The reorganized database separates content into specialized tables:

1. **scripts**: Metadata for each script (management_id, title, dates)
2. **character_dialogue**: Pure character spoken lines
3. **scene_descriptions**: Environmental and situational descriptions
4. **visual_effects**: CG, animations, visual elements
5. **audio_instructions**: BGM, SE, music notes
6. **technical_notes**: Production and filming notes

### Benefits of New Structure

1. **Clean Separation**: No more mixed content types
2. **Optimized Queries**: Specialized indexes for each content type
3. **Better Organization**: Clear categorization enables targeted searches
4. **Maintenance**: Easier to update and maintain specific content types
5. **Analytics**: Better insights into script composition and patterns

### Recommendations

1. **Validation**: Review sample entries in each new table
2. **Testing**: Run queries against new structure to verify accuracy
3. **Migration**: Update applications to use new table structure
4. **Monitoring**: Track query performance improvements
5. **Documentation**: Update documentation for new schema

## Usage Examples

### Find all dialogue by character:
```sql
SELECT d.dialogue_text, s.title, s.management_id
FROM character_dialogue d
JOIN scripts s ON d.script_id = s.id
WHERE d.character_name = 'サンサン'
ORDER BY s.management_id, d.row_number;
```

### Find all visual effects for a script:
```sql
SELECT v.effect_description, v.effect_type
FROM visual_effects v
JOIN scripts s ON v.script_id = s.id
WHERE s.management_id = 'B1039'
ORDER BY v.row_number;
```

### Get complete script content in order:
```sql
-- Complex query to reconstruct full script
-- (Implementation would require UNION of all content tables)
```
"""
        
        return report

    def close(self):
        """Close database connection"""
        self.conn.close()


def main():
    """Main execution function"""
    
    db_path = "youtube_search_complete_all.db"
    
    print("SunSun Script Database Reorganizer")
    print("==================================")
    print(f"Analyzing database: {db_path}")
    
    # Initialize reorganizer
    reorganizer = DatabaseReorganizer(db_path)
    
    try:
        # Analyze current structure
        print("\n1. Analyzing current database structure...")
        analysis = reorganizer.analyze_content_patterns()
        
        # Reorganize database
        print("\n2. Reorganizing database structure...")
        stats = reorganizer.reorganize_database()
        
        # Generate report
        print("\n3. Generating analysis report...")
        report = reorganizer.generate_report(analysis, stats)
        
        # Save report
        with open("database_analysis_report.md", "w", encoding="utf-8") as f:
            f.write(report)
        
        print(f"\n✅ Reorganization complete!")
        print(f"📊 Original database: {db_path}")
        print(f"📊 Reorganized database: youtube_search_complete_all_reorganized.db")
        print(f"📋 Report saved to: database_analysis_report.md")
        
        print(f"\n📈 Summary:")
        for key, value in stats.items():
            if key != 'total_processed':
                percentage = (value / stats['total_processed']) * 100
                print(f"   {key.replace('_', ' ').title()}: {value:,} ({percentage:.1f}%)")
    
    finally:
        reorganizer.close()


if __name__ == "__main__":
    main()
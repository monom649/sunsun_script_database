#!/usr/bin/env python3
"""
Dialogue Content Analyzer

This script analyzes the actual content of dialogue data to check
if the content is meaningful or just placeholder/duplicate data.
"""

import sqlite3
import os
from collections import Counter
import re

class DialogueContentAnalyzer:
    def __init__(self, db_path):
        self.db_path = db_path
        
    def analyze_dialogue_content(self):
        """Analyze the actual content of dialogue data"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            print("=== セリフ内容分析 ===")
            
            # 1. 総統計
            cursor.execute("SELECT COUNT(*) FROM character_dialogue")
            total_lines = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM character_dialogue WHERE dialogue_text IS NOT NULL AND dialogue_text != ''")
            non_empty_lines = cursor.fetchone()[0]
            
            print(f"総セリフ行数: {total_lines:,}")
            print(f"空でないセリフ: {non_empty_lines:,}")
            print(f"空のセリフ率: {(total_lines - non_empty_lines) / total_lines * 100:.1f}%")
            
            # 2. セリフの長さ分析
            cursor.execute("SELECT LENGTH(dialogue_text) FROM character_dialogue WHERE dialogue_text IS NOT NULL AND dialogue_text != ''")
            lengths = [row[0] for row in cursor.fetchall()]
            
            if lengths:
                avg_length = sum(lengths) / len(lengths)
                print(f"\nセリフの平均文字数: {avg_length:.1f}文字")
                
                length_ranges = {
                    '1-5文字': len([l for l in lengths if 1 <= l <= 5]),
                    '6-20文字': len([l for l in lengths if 6 <= l <= 20]),
                    '21-50文字': len([l for l in lengths if 21 <= l <= 50]),
                    '51文字以上': len([l for l in lengths if l > 50])
                }
                
                print("文字数分布:")
                for range_name, count in length_ranges.items():
                    print(f"  {range_name}: {count:,}行 ({count/len(lengths)*100:.1f}%)")
            
            # 3. 重複セリフの分析
            cursor.execute("SELECT dialogue_text, COUNT(*) as count FROM character_dialogue WHERE dialogue_text IS NOT NULL AND dialogue_text != '' GROUP BY dialogue_text HAVING count > 1 ORDER BY count DESC LIMIT 20")
            
            print(f"\n重複の多いセリフ TOP20:")
            total_duplicates = 0
            for dialogue, count in cursor.fetchall():
                print(f"  \"{dialogue[:60]}...\" - {count:,}回")
                total_duplicates += count
            
            print(f"重複セリフの総数: {total_duplicates:,}行")
            
            # 4. 特殊なパターンの分析
            patterns = {
                'テンプレート系': ['タイトル', 'キャラクター', 'シーン', '使用おもちゃ', '道具など'],
                '記号のみ': ['...', '…', '？？？', '！！！'],
                '指示系': ['カット', '編集', 'SE', 'BGM', '効果音'],
                '空白・記号': ['　', '■', '●', '○', '□']
            }
            
            print(f"\n特殊パターンの分析:")
            for pattern_name, keywords in patterns.items():
                count = 0
                for keyword in keywords:
                    cursor.execute("SELECT COUNT(*) FROM character_dialogue WHERE dialogue_text LIKE ?", (f'%{keyword}%',))
                    count += cursor.fetchone()[0]
                print(f"  {pattern_name}: {count:,}行")
            
            # 5. 実際のセリフサンプル（ランダム）
            print(f"\n実際のセリフサンプル（ランダム20件）:")
            cursor.execute("""
                SELECT s.management_id, cd.character_name, cd.dialogue_text, cd.voice_instruction
                FROM character_dialogue cd
                JOIN scripts s ON cd.script_id = s.id
                WHERE cd.dialogue_text IS NOT NULL 
                  AND cd.dialogue_text != ''
                  AND cd.character_name IN ('サンサン', 'くもりん', 'ツクモ', 'ノイズ')
                ORDER BY RANDOM()
                LIMIT 20
            """)
            
            for mgmt_id, character, dialogue, voice in cursor.fetchall():
                voice_display = f"[{voice}]" if voice else ""
                dialogue_display = dialogue[:80] + "..." if len(dialogue) > 80 else dialogue
                print(f"  {mgmt_id}: {character} - \"{dialogue_display}\" {voice_display}")
            
            # 6. 意味のあるセリフの割合分析
            cursor.execute("""
                SELECT COUNT(*) FROM character_dialogue 
                WHERE dialogue_text IS NOT NULL 
                  AND dialogue_text != ''
                  AND LENGTH(dialogue_text) >= 3
                  AND dialogue_text NOT LIKE '%タイトル%'
                  AND dialogue_text NOT LIKE '%キャラクター%'
                  AND dialogue_text NOT LIKE '%シーン%'
                  AND dialogue_text NOT LIKE '%使用おもちゃ%'
                  AND dialogue_text NOT LIKE '%...%'
                  AND dialogue_text NOT LIKE '%…%'
            """)
            meaningful_count = cursor.fetchone()[0]
            
            print(f"\n意味のあるセリフの推定:")
            print(f"  意味のあるセリフ: {meaningful_count:,}行")
            print(f"  意味のあるセリフ率: {meaningful_count/total_lines*100:.1f}%")
            
            # 7. キャラクター別の実セリフ分析
            print(f"\n主要キャラクター別セリフ品質:")
            for character in ['サンサン', 'くもりん', 'ツクモ', 'ノイズ']:
                cursor.execute("SELECT COUNT(*) FROM character_dialogue WHERE character_name = ?", (character,))
                total_char = cursor.fetchone()[0]
                
                cursor.execute("""
                    SELECT COUNT(*) FROM character_dialogue 
                    WHERE character_name = ?
                      AND dialogue_text IS NOT NULL 
                      AND dialogue_text != ''
                      AND LENGTH(dialogue_text) >= 5
                      AND dialogue_text NOT LIKE '%タイトル%'
                      AND dialogue_text NOT LIKE '%キャラクター%'
                      AND dialogue_text NOT LIKE '%...%'
                """, (character,))
                meaningful_char = cursor.fetchone()[0]
                
                quality_rate = meaningful_char / total_char * 100 if total_char > 0 else 0
                print(f"  {character}: {meaningful_char:,}/{total_char:,}行 ({quality_rate:.1f}%が有効)")
            
            # 8. 最新データの品質チェック（2025年データ）
            print(f"\n2025年データの品質チェック:")
            cursor.execute("""
                SELECT COUNT(*) FROM character_dialogue cd
                JOIN scripts s ON cd.script_id = s.id
                WHERE s.broadcast_date LIKE '25/%'
                  AND cd.dialogue_text IS NOT NULL 
                  AND cd.dialogue_text != ''
            """)
            total_2025 = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT COUNT(*) FROM character_dialogue cd
                JOIN scripts s ON cd.script_id = s.id
                WHERE s.broadcast_date LIKE '25/%'
                  AND cd.dialogue_text IS NOT NULL 
                  AND cd.dialogue_text != ''
                  AND LENGTH(cd.dialogue_text) >= 5
                  AND cd.character_name IN ('サンサン', 'くもりん', 'ツクモ', 'ノイズ')
            """)
            quality_2025 = cursor.fetchone()[0]
            
            if total_2025 > 0:
                print(f"  2025年総セリフ: {total_2025:,}行")
                print(f"  2025年有効セリフ: {quality_2025:,}行 ({quality_2025/total_2025*100:.1f}%)")
            else:
                print(f"  2025年セリフデータなし")
            
            conn.close()
            
        except Exception as e:
            print(f"❌ Error analyzing dialogue content: {str(e)}")
    
    def check_data_patterns(self):
        """Check for specific data quality patterns"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            print("\n=== データパターン詳細調査 ===")
            
            # よくあるダミーデータのパターンをチェック
            dummy_patterns = [
                "タイトル",
                "使用おもちゃ、\n道具など",
                "キャラクター",
                "シーン",
                "SE",
                "BGM",
                "カット",
                "編集指示"
            ]
            
            print("ダミー/ヘッダーデータのパターン:")
            total_dummy = 0
            for pattern in dummy_patterns:
                cursor.execute("SELECT COUNT(*) FROM character_dialogue WHERE dialogue_text = ?", (pattern,))
                count = cursor.fetchone()[0]
                if count > 0:
                    print(f"  \"{pattern}\": {count:,}行")
                    total_dummy += count
            
            print(f"ダミーデータ総計: {total_dummy:,}行")
            
            # 実際のセリフっぽいデータのサンプル
            print("\n実際のセリフっぽいデータの例:")
            cursor.execute("""
                SELECT DISTINCT dialogue_text FROM character_dialogue 
                WHERE dialogue_text IS NOT NULL 
                  AND dialogue_text != ''
                  AND LENGTH(dialogue_text) BETWEEN 10 AND 100
                  AND (dialogue_text LIKE '%だよ%' 
                       OR dialogue_text LIKE '%です%'
                       OR dialogue_text LIKE '%！%'
                       OR dialogue_text LIKE '%？%')
                  AND dialogue_text NOT LIKE '%タイトル%'
                  AND dialogue_text NOT LIKE '%キャラクター%'
                LIMIT 15
            """)
            
            for row in cursor.fetchall():
                dialogue = row[0]
                print(f"  \"{dialogue}\"")
            
            conn.close()
            
        except Exception as e:
            print(f"❌ Error checking data patterns: {str(e)}")

def main():
    """Main analysis function"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found at: {db_path}")
        return
    
    analyzer = DialogueContentAnalyzer(db_path)
    analyzer.analyze_dialogue_content()
    analyzer.check_data_patterns()

if __name__ == "__main__":
    main()
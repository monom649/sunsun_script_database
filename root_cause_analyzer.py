#!/usr/bin/env python3
"""
Root Cause Analyzer

This script analyzes why character-dialogue adjacency logic fails
and causes instruction text to be misclassified as dialogue.
"""

import sqlite3
import requests
import pandas as pd
import io
import re
from datetime import datetime

class RootCauseAnalyzer:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/root_cause_analysis_log.txt"
        
    def log_message(self, message: str):
        """Log messages with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())
    
    def analyze_problematic_entries(self, limit=20):
        """Analyze specific problematic entries to understand the cause"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get problematic entries with clear instruction markers
            cursor.execute('''
                SELECT cdu.character_name, cdu.dialogue_text, cdu.filming_audio_instructions, 
                       cdu.row_number, s.management_id, s.title, s.script_url
                FROM character_dialogue_unified cdu
                JOIN scripts s ON cdu.script_id = s.id
                WHERE LENGTH(cdu.character_name) > 0 AND LENGTH(cdu.dialogue_text) > 0
                AND (cdu.dialogue_text LIKE '%・%' OR cdu.dialogue_text LIKE '%（%' 
                     OR cdu.dialogue_text LIKE '%作り方%' OR cdu.dialogue_text LIKE '%演出%'
                     OR cdu.dialogue_text LIKE '%シーン%' OR cdu.dialogue_text LIKE '%アニメ%')
                ORDER BY LENGTH(cdu.dialogue_text) DESC
                LIMIT ?
            ''', (limit,))
            
            problematic_entries = cursor.fetchall()
            conn.close()
            
            self.log_message("=" * 80)
            self.log_message("問題エントリの詳細分析")
            self.log_message("=" * 80)
            
            for i, (char_name, dialogue, instructions, row_num, script_id, title, script_url) in enumerate(problematic_entries, 1):
                self.log_message(f"\n{i}. スクリプト: {script_id} - 行{row_num}")
                self.log_message(f"   タイトル: {title[:50]}...")
                self.log_message(f"   キャラクター: {char_name}")
                self.log_message(f"   セリフ: {dialogue[:200]}...")
                self.log_message(f"   指示: {instructions[:100]}...")
                
                # Analyze the original spreadsheet structure for this entry
                if script_url and i <= 5:  # Only analyze first 5 to avoid too many requests
                    self.analyze_original_structure(script_url, script_id, row_num, char_name, dialogue)
            
            return problematic_entries
            
        except Exception as e:
            self.log_message(f"❌ Analysis error: {str(e)}")
            return []
    
    def analyze_original_structure(self, script_url, script_id, row_num, char_name, dialogue):
        """Analyze the original spreadsheet structure to understand extraction logic"""
        if not script_url or 'docs.google.com' not in script_url:
            return
        
        try:
            # Extract spreadsheet ID and GID
            sheet_pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
            gid_pattern = r'[#&]gid=([0-9]+)'
            
            sheet_match = re.search(sheet_pattern, script_url)
            gid_match = re.search(gid_pattern, script_url)
            
            if not sheet_match:
                return
            
            spreadsheet_id = sheet_match.group(1)
            gid = gid_match.group(1) if gid_match else '0'
            
            # Build CSV export URL
            csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
            
            # Fetch CSV data
            response = requests.get(csv_url, timeout=10)
            if response.status_code != 200:
                self.log_message(f"   ⚠️  HTTP {response.status_code} for {script_id}")
                return
            
            # Parse CSV
            csv_data = response.content.decode('utf-8', errors='ignore')
            df = pd.read_csv(io.StringIO(csv_data))
            
            self.log_message(f"\\n   📊 {script_id} 元スプレッドシート分析:")
            self.log_message(f"   総行数: {len(df)}")
            self.log_message(f"   総列数: {len(df.columns)}")
            
            # Show column headers
            self.log_message(f"   列名: {list(df.columns)[:10]}")
            
            # Analyze the specific row
            if row_num < len(df):
                target_row = df.iloc[row_num]
                self.log_message(f"\\n   行{row_num}の全データ:")
                
                for col_idx, (col_name, value) in enumerate(target_row.items()):
                    if pd.notna(value) and str(value).strip():
                        value_str = str(value).strip()
                        self.log_message(f"     列{col_idx}({col_name}): {value_str[:100]}...")
                        
                        # Check if this matches our extracted character or dialogue
                        if char_name in value_str:
                            self.log_message(f"       → キャラクター名マッチ")
                        if value_str in dialogue:
                            self.log_message(f"       → セリフ内容マッチ")
                
                # Analyze adjacency logic failure
                self.log_message(f"\\n   隣接ロジック分析:")
                for col_idx, (col_name, value) in enumerate(target_row.items()):
                    if pd.notna(value) and char_name in str(value):
                        self.log_message(f"     キャラクター発見: 列{col_idx}")
                        
                        # Check adjacent columns
                        for offset in [1, 2, 3, -1]:
                            adj_idx = col_idx + offset
                            if 0 <= adj_idx < len(target_row):
                                adj_col_name = list(df.columns)[adj_idx]
                                adj_value = target_row.iloc[adj_idx]
                                if pd.notna(adj_value):
                                    adj_str = str(adj_value).strip()
                                    self.log_message(f"       隣接列{adj_idx}(+{offset}): {adj_str[:80]}...")
                                    if adj_str in dialogue:
                                        self.log_message(f"         → これが誤って「セリフ」として抽出された")
            
        except Exception as e:
            self.log_message(f"   ❌ 構造分析エラー: {str(e)}")
    
    def analyze_extraction_patterns(self):
        """Analyze patterns in the extraction logic that cause misclassification"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            self.log_message("\\n" + "=" * 80)
            self.log_message("抽出ロジックパターン分析")
            self.log_message("=" * 80)
            
            # Pattern 1: Instructions with bullet points
            cursor.execute('''
                SELECT COUNT(*) FROM character_dialogue_unified 
                WHERE LENGTH(character_name) > 0 AND dialogue_text LIKE '%・%'
            ''')
            bullet_count = cursor.fetchone()[0]
            self.log_message(f"1. 箇条書き指示文（・含む）: {bullet_count}件")
            
            # Pattern 2: Instructions with parentheses
            cursor.execute('''
                SELECT COUNT(*) FROM character_dialogue_unified 
                WHERE LENGTH(character_name) > 0 AND (dialogue_text LIKE '%（%' OR dialogue_text LIKE '%）%')
            ''')
            paren_count = cursor.fetchone()[0]
            self.log_message(f"2. 括弧付き指示文: {paren_count}件")
            
            # Pattern 3: Instructions with arrows
            cursor.execute('''
                SELECT COUNT(*) FROM character_dialogue_unified 
                WHERE LENGTH(character_name) > 0 AND dialogue_text LIKE '%→%'
            ''')
            arrow_count = cursor.fetchone()[0]
            self.log_message(f"3. 矢印付き指示文: {arrow_count}件")
            
            # Pattern 4: Production instructions
            cursor.execute('''
                SELECT COUNT(*) FROM character_dialogue_unified 
                WHERE LENGTH(character_name) > 0 AND (
                    dialogue_text LIKE '%作り方%' OR dialogue_text LIKE '%演出%'
                    OR dialogue_text LIKE '%撮影%' OR dialogue_text LIKE '%編集%'
                )
            ''')
            production_count = cursor.fetchone()[0]
            self.log_message(f"4. 制作指示文: {production_count}件")
            
            # Pattern 5: Character names as "characters"
            cursor.execute('''
                SELECT character_name, COUNT(*) as count
                FROM character_dialogue_unified 
                WHERE LENGTH(character_name) > 0 AND LENGTH(dialogue_text) > 0
                AND character_name NOT IN ('サンサン', 'くもりん', 'ツクモ', 'ノイズ', 'ママ', 'パパ', 'BB')
                GROUP BY character_name
                ORDER BY count DESC
                LIMIT 10
            ''')
            non_character_names = cursor.fetchall()
            
            self.log_message(f"\\n5. 非キャラクター名が「キャラクター」として分類:")
            for name, count in non_character_names:
                self.log_message(f"   {name}: {count}件")
            
            # Analyze specific examples
            cursor.execute('''
                SELECT character_name, dialogue_text 
                FROM character_dialogue_unified 
                WHERE character_name = 'SE' AND LENGTH(dialogue_text) > 50
                LIMIT 5
            ''')
            se_examples = cursor.fetchall()
            
            self.log_message(f"\\n6. 'SE'として分類された例:")
            for char, dialogue in se_examples:
                self.log_message(f"   {char}: {dialogue[:100]}...")
            
            conn.close()
            
        except Exception as e:
            self.log_message(f"❌ Pattern analysis error: {str(e)}")
    
    def analyze_adjacency_failures(self):
        """Analyze why adjacency logic fails"""
        self.log_message("\\n" + "=" * 80)
        self.log_message("隣接ロジック失敗原因分析")
        self.log_message("=" * 80)
        
        self.log_message("推定される失敗原因:")
        self.log_message("1. 複数行にまたがるセル結合")
        self.log_message("   - キャラクター名が複数行分のセルと結合されている")
        self.log_message("   - 隣接する全ての行が「セリフ」として抽出される")
        
        self.log_message("\\n2. スプレッドシート構造の不統一")
        self.log_message("   - キャラクター列と指示列の位置が一定でない")
        self.log_message("   - 隣接ロジックが指示文の列を誤ってセリフ列と判定")
        
        self.log_message("\\n3. 内容ベース判定の不備")
        self.log_message("   - 「・」「（）」「→」等の指示マーカーを無視")
        self.log_message("   - 長文の制作指示もセリフとして抽出")
        
        self.log_message("\\n4. キャラクター名の誤認識")
        self.log_message("   - 'SE'、'TRUE'、'FALSE'等をキャラクター名と判定")
        self.log_message("   - これらに隣接するテキストを全てセリフとして抽出")
        
        self.log_message("\\n5. 抽出戦略の過度な許容性")
        self.log_message("   - 「とりあえず何でも抽出」の方針")
        self.log_message("   - 指示文とセリフの区別をしない抽出ロジック")
    
    def run_root_cause_analysis(self):
        """Run complete root cause analysis"""
        self.log_message("=" * 80)
        self.log_message("セリフ・指示文混在問題の根本原因分析")
        self.log_message("=" * 80)
        
        # 1. Analyze problematic entries
        problematic_entries = self.analyze_problematic_entries(10)
        
        # 2. Analyze extraction patterns
        self.analyze_extraction_patterns()
        
        # 3. Analyze adjacency logic failures
        self.analyze_adjacency_failures()
        
        # 4. Summary
        self.log_message("\\n" + "=" * 80)
        self.log_message("📋 根本原因まとめ")
        self.log_message("=" * 80)
        self.log_message("❌ 主要問題:")
        self.log_message("1. 隣接ロジックだけでは不十分")
        self.log_message("   - スプレッドシートの複雑な構造を考慮していない")
        self.log_message("   - セル結合、不規則な配置に対応できない")
        
        self.log_message("\\n2. 内容ベース判定の欠如")
        self.log_message("   - 指示文マーカー（・、（）、→）を無視")
        self.log_message("   - セリフらしさの判定基準が甘い")
        
        self.log_message("\\n3. キャラクター名認識の問題")
        self.log_message("   - SE、TRUE、FALSE等を誤認識")
        self.log_message("   - 実在しないキャラクター名も受け入れ")
        
        self.log_message("\\n✅ 必要な対策:")
        self.log_message("1. 厳格な内容ベース判定の実装")
        self.log_message("2. 指示文マーカーの除外ロジック強化")
        self.log_message("3. キャラクター名の厳格な検証")
        self.log_message("4. セリフらしさの判定基準強化")
        self.log_message("=" * 80)

def main():
    """Main execution function"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    analyzer = RootCauseAnalyzer(db_path)
    analyzer.run_root_cause_analysis()

if __name__ == "__main__":
    main()
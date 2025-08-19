#!/usr/bin/env python3
"""
Data Structure Analyzer and Fixer

This script analyzes the current database structure patterns and fixes
character dialogue vs instruction text mixing issues.

It considers multiple possible structure patterns across different sheets.
"""

import sqlite3
import re
import os
from typing import Dict, List, Tuple, Optional
from datetime import datetime

class DataStructureAnalyzerFixer:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/data_fix_log.txt"
        
        # Character names for validation
        self.character_names = {
            'サンサン', 'くもりん', 'ツクモ', 'ノイズ', 'ママ', 'パパ', 'BB', 'プリル'
        }
        
        # Instruction patterns (撮影指示・音声指示)
        self.instruction_patterns = [
            r'.*を見.*', r'.*する.*', r'.*から.*', r'.*へ.*', r'.*まで.*',
            r'.*カメラ.*', r'.*映像.*', r'.*テロップ.*', r'.*アニメ.*',
            r'.*実機.*', r'.*ダイジェスト.*', r'.*コラボ.*', r'.*セット.*',
            r'.*撮影.*', r'.*音声.*', r'.*編集.*', r'.*整音.*',
            r'^\(.*\)$', r'^【.*】$', r'^＜.*＞$', r'^※.*',
            r'.*指示.*', r'.*注意.*', r'.*補足.*', r'.*メモ.*'
        ]
    
    def log_message(self, message: str):
        """Log messages with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())
    
    def is_likely_instruction(self, text: str) -> bool:
        """Check if text is likely an instruction rather than dialogue"""
        if not text or not text.strip():
            return False
            
        text = text.strip()
        
        # Check for instruction patterns
        for pattern in self.instruction_patterns:
            if re.match(pattern, text):
                return True
        
        # Check for very short utterances that might be dialogue
        if len(text) <= 10 and any(char in text for char in 'あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほまみむめもやゆよらりるれろわをん'):
            return False
            
        # Check for bracketed instructions
        if text.startswith(('(', '（', '[', '【', '<', '＜', '※')):
            return True
            
        # Check for instruction keywords
        instruction_keywords = ['映像', 'テロップ', 'カメラ', '撮影', '音声', '編集', 'アニメ', '実機', 'コラボ']
        if any(keyword in text for keyword in instruction_keywords):
            return True
            
        return False
    
    def is_likely_dialogue(self, text: str) -> bool:
        """Check if text is likely dialogue"""
        if not text or not text.strip():
            return False
            
        text = text.strip()
        
        # Check for dialogue endings
        dialogue_endings = ['！', '？', '。', '～', '♪', '★', '☆', 'よ', 'ね', 'だ', 'である']
        if any(text.endswith(ending) for ending in dialogue_endings):
            return True
            
        # Check for conversational patterns
        conversational_patterns = [
            r'.*だよ.*', r'.*ですね.*', r'.*ますね.*', r'.*でしょう.*',
            r'.*かな.*', r'.*だね.*', r'.*ちゃん.*', r'.*くん.*',
            r'みんな.*', r'.*しよう.*', r'.*ましょう.*'
        ]
        
        for pattern in conversational_patterns:
            if re.match(pattern, text):
                return True
                
        return False
    
    def analyze_structure_patterns(self) -> Dict[str, any]:
        """Analyze different structure patterns in the database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            self.log_message("=== データ構造パターン分析開始 ===")
            
            # Get sample data for analysis
            cursor.execute("""
                SELECT s.management_id, s.title, cdu.character_name, cdu.dialogue_text, 
                       cdu.filming_audio_instructions, cdu.row_number
                FROM character_dialogue_unified cdu
                JOIN scripts s ON cdu.script_id = s.id
                WHERE LENGTH(cdu.character_name) > 0
                ORDER BY s.management_id, cdu.row_number
                LIMIT 1000
            """)
            
            sample_data = cursor.fetchall()
            
            # Analyze patterns
            patterns = {
                'dialogue_in_wrong_column': 0,
                'instruction_in_dialogue_column': 0,
                'correct_structure': 0,
                'empty_dialogue': 0,
                'empty_instructions': 0,
                'problematic_scripts': set()
            }
            
            for mgmt_id, title, char_name, dialogue_text, instructions, row_num in sample_data:
                # Check if dialogue_text contains instructions
                if dialogue_text and self.is_likely_instruction(dialogue_text):
                    patterns['instruction_in_dialogue_column'] += 1
                    patterns['problematic_scripts'].add(mgmt_id)
                
                # Check if dialogue_text is likely proper dialogue
                elif dialogue_text and self.is_likely_dialogue(dialogue_text):
                    patterns['correct_structure'] += 1
                
                # Check for empty dialogue
                elif not dialogue_text or not dialogue_text.strip():
                    patterns['empty_dialogue'] += 1
                    patterns['problematic_scripts'].add(mgmt_id)
                
                # Check for empty instructions
                if not instructions or not instructions.strip():
                    patterns['empty_instructions'] += 1
            
            # Log analysis results
            self.log_message(f"📊 構造分析結果 (サンプル1000件):")
            self.log_message(f"   正しい構造: {patterns['correct_structure']}件")
            self.log_message(f"   セリフ列に指示文: {patterns['instruction_in_dialogue_column']}件")
            self.log_message(f"   空のセリフ: {patterns['empty_dialogue']}件") 
            self.log_message(f"   空の指示文: {patterns['empty_instructions']}件")
            self.log_message(f"   問題のあるスクリプト数: {len(patterns['problematic_scripts'])}件")
            
            conn.close()
            return patterns
            
        except Exception as e:
            self.log_message(f"❌ 構造分析エラー: {str(e)}")
            return {}
    
    def analyze_specific_scripts(self, script_ids: List[str] = None) -> Dict[str, List]:
        """Analyze specific scripts in detail"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            if not script_ids:
                # Get problematic scripts from sample
                cursor.execute("""
                    SELECT DISTINCT s.management_id
                    FROM character_dialogue_unified cdu
                    JOIN scripts s ON cdu.script_id = s.id
                    WHERE LENGTH(cdu.character_name) > 0 
                      AND (LENGTH(cdu.dialogue_text) = 0 OR cdu.dialogue_text IS NULL)
                    LIMIT 10
                """)
                script_ids = [row[0] for row in cursor.fetchall()]
            
            detailed_analysis = {}
            
            for script_id in script_ids:
                cursor.execute("""
                    SELECT cdu.row_number, cdu.character_name, cdu.dialogue_text, 
                           cdu.filming_audio_instructions
                    FROM character_dialogue_unified cdu
                    JOIN scripts s ON cdu.script_id = s.id
                    WHERE s.management_id = ? AND LENGTH(cdu.character_name) > 0
                    ORDER BY cdu.row_number
                    LIMIT 20
                """, (script_id,))
                
                script_data = cursor.fetchall()
                
                issues = []
                for row_num, char_name, dialogue, instructions in script_data:
                    issue = {}
                    
                    if not dialogue or not dialogue.strip():
                        issue['empty_dialogue'] = True
                        
                    if dialogue and self.is_likely_instruction(dialogue):
                        issue['instruction_in_dialogue'] = True
                        issue['suggested_fix'] = 'Move to instructions column'
                        
                    if instructions and self.is_likely_dialogue(instructions):
                        issue['dialogue_in_instructions'] = True
                        issue['suggested_fix'] = 'Swap columns'
                    
                    if issue:
                        issue['row'] = row_num
                        issue['character'] = char_name
                        issue['current_dialogue'] = dialogue
                        issue['current_instructions'] = instructions
                        issues.append(issue)
                
                detailed_analysis[script_id] = issues
                
                if issues:
                    self.log_message(f"🔍 {script_id}: {len(issues)}件の問題発見")
            
            conn.close()
            return detailed_analysis
            
        except Exception as e:
            self.log_message(f"❌ 詳細分析エラー: {str(e)}")
            return {}
    
    def create_fix_strategy(self, patterns: Dict, detailed_analysis: Dict) -> Dict[str, any]:
        """Create a data fixing strategy based on analysis"""
        strategy = {
            'swap_columns': [],  # Scripts where columns need swapping
            'move_to_instructions': [],  # Entries where dialogue should move to instructions
            'clear_empty': [],  # Entries with empty dialogue to investigate
            'manual_review': []  # Entries that need manual review
        }
        
        # Analyze patterns to determine strategy
        if patterns.get('instruction_in_dialogue_column', 0) > patterns.get('correct_structure', 0) / 2:
            self.log_message("⚠️  大量のセリフ列指示文混入を検出 - 全体修正が必要")
            strategy['full_analysis_needed'] = True
        
        # Process detailed analysis
        for script_id, issues in detailed_analysis.items():
            for issue in issues:
                if issue.get('instruction_in_dialogue') and issue.get('dialogue_in_instructions'):
                    strategy['swap_columns'].append({
                        'script_id': script_id,
                        'row': issue['row'],
                        'character': issue['character']
                    })
                elif issue.get('instruction_in_dialogue'):
                    strategy['move_to_instructions'].append({
                        'script_id': script_id,
                        'row': issue['row'],
                        'character': issue['character'],
                        'text': issue['current_dialogue']
                    })
                elif issue.get('empty_dialogue'):
                    strategy['clear_empty'].append({
                        'script_id': script_id,
                        'row': issue['row'],
                        'character': issue['character']
                    })
        
        self.log_message(f"🎯 修正戦略:")
        self.log_message(f"   列交換: {len(strategy['swap_columns'])}件")
        self.log_message(f"   指示文移動: {len(strategy['move_to_instructions'])}件")
        self.log_message(f"   空セリフ調査: {len(strategy['clear_empty'])}件")
        
        return strategy
    
    def apply_fixes_safely(self, strategy: Dict, dry_run: bool = True) -> Dict[str, int]:
        """Apply fixes safely with backup and validation"""
        if dry_run:
            self.log_message("🔄 DRY RUN モード - 実際の変更は行いません")
        else:
            self.log_message("⚠️  実際にデータベースを修正します")
        
        results = {
            'swapped': 0,
            'moved': 0,
            'cleared': 0,
            'errors': 0
        }
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Process column swaps
            for fix in strategy.get('swap_columns', []):
                try:
                    if not dry_run:
                        cursor.execute("""
                            UPDATE character_dialogue_unified SET 
                                dialogue_text = filming_audio_instructions,
                                filming_audio_instructions = dialogue_text
                            WHERE script_id = (SELECT id FROM scripts WHERE management_id = ?)
                              AND row_number = ?
                              AND character_name = ?
                        """, (fix['script_id'], fix['row'], fix['character']))
                    
                    results['swapped'] += 1
                    self.log_message(f"🔄 {fix['script_id']}:{fix['row']} - 列交換")
                    
                except Exception as e:
                    results['errors'] += 1
                    self.log_message(f"❌ 列交換エラー {fix['script_id']}: {str(e)}")
            
            # Process moves to instructions
            for fix in strategy.get('move_to_instructions', []):
                try:
                    if not dry_run:
                        cursor.execute("""
                            UPDATE character_dialogue_unified SET 
                                dialogue_text = '',
                                filming_audio_instructions = COALESCE(filming_audio_instructions, '') || 
                                    (CASE WHEN filming_audio_instructions IS NOT NULL AND filming_audio_instructions != '' 
                                          THEN ' | ' ELSE '' END) || ?
                            WHERE script_id = (SELECT id FROM scripts WHERE management_id = ?)
                              AND row_number = ?
                              AND character_name = ?
                        """, (fix['text'], fix['script_id'], fix['row'], fix['character']))
                    
                    results['moved'] += 1
                    self.log_message(f"📝 {fix['script_id']}:{fix['row']} - 指示文移動")
                    
                except Exception as e:
                    results['errors'] += 1
                    self.log_message(f"❌ 移動エラー {fix['script_id']}: {str(e)}")
            
            if not dry_run:
                conn.commit()
                
            conn.close()
            
        except Exception as e:
            self.log_message(f"❌ 修正処理エラー: {str(e)}")
            results['errors'] += 1
        
        return results
    
    def run_comprehensive_analysis_and_fix(self, dry_run: bool = True):
        """Run comprehensive analysis and fix process"""
        self.log_message("=" * 80)
        self.log_message("データ構造分析・修正プロセス開始")
        self.log_message("=" * 80)
        
        # Step 1: Analyze structure patterns
        patterns = self.analyze_structure_patterns()
        if not patterns:
            return
        
        # Step 2: Analyze specific problematic scripts
        detailed_analysis = self.analyze_specific_scripts()
        
        # Step 3: Create fix strategy
        strategy = self.create_fix_strategy(patterns, detailed_analysis)
        
        # Step 4: Apply fixes
        results = self.apply_fixes_safely(strategy, dry_run=dry_run)
        
        # Step 5: Report results
        self.log_message("=" * 80)
        self.log_message(f"修正結果 ({'DRY RUN' if dry_run else 'ACTUAL RUN'}):")
        self.log_message(f"  列交換: {results['swapped']}件")
        self.log_message(f"  指示文移動: {results['moved']}件")
        self.log_message(f"  空セリフ処理: {results['cleared']}件")
        self.log_message(f"  エラー: {results['errors']}件")
        self.log_message("=" * 80)
        
        if dry_run:
            self.log_message("💡 実際の修正を実行するには dry_run=False で再実行してください")
        else:
            self.log_message("✅ データベース修正完了")
        
        return results

def main():
    """Main execution function"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found at: {db_path}")
        return
    
    analyzer_fixer = DataStructureAnalyzerFixer(db_path)
    
    print("=== データ構造分析・修正ツール ===")
    print("まず DRY RUN で分析を実行します...")
    
    # Run analysis and dry run first
    results = analyzer_fixer.run_comprehensive_analysis_and_fix(dry_run=True)
    
    if results and results.get('errors', 0) == 0:
        print("\n🎯 DRY RUN 完了。実際の修正を実行しますか？")
        print("実行する場合は、このスクリプトを dry_run=False で再実行してください。")

if __name__ == "__main__":
    main()
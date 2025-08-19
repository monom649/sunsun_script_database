#!/usr/bin/env python3
"""
Comprehensive Database Validation

This script validates the entire database to ensure all dialogue/instruction
fixes have been properly applied across ALL scripts.
"""

import sqlite3
import re
from datetime import datetime

class ComprehensiveValidator:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/validation_log.txt"
        
        # Instruction patterns that should NOT be in dialogue_text
        self.instruction_patterns = [
            r'.*を見.*', r'.*する.*', r'.*から.*', r'.*へ.*', r'.*に.*',
            r'.*カメラ.*', r'.*映像.*', r'.*テロップ.*', r'.*アニメ.*',
            r'.*撮影.*', r'.*音声.*', r'.*編集.*',
            r'.*タッチ.*', r'.*近づく.*', r'.*見渡す.*', r'.*対峙.*'
        ]
        
        # Dialogue patterns that SHOULD be in dialogue_text
        self.dialogue_patterns = [
            r'.*！$', r'.*？$', r'.*。$', r'.*～$', r'.*♪$', r'.*★$',
            r'.*だよ.*', r'.*ですね.*', r'.*だね.*', r'.*かな.*', r'.*よー.*',
            r'みんな.*', r'.*ちゃん.*', r'.*くん.*'
        ]
    
    def log_message(self, message: str):
        """Log messages with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())
    
    def is_instruction_like(self, text: str) -> bool:
        """Check if text looks like instruction"""
        if not text:
            return False
        
        for pattern in self.instruction_patterns:
            if re.match(pattern, text):
                return True
        return False
    
    def is_dialogue_like(self, text: str) -> bool:
        """Check if text looks like dialogue"""
        if not text:
            return False
        
        for pattern in self.dialogue_patterns:
            if re.match(pattern, text):
                return True
        return False
    
    def validate_entire_database(self):
        """Validate the entire database structure"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            self.log_message("=== 全データベース構造検証開始 ===")
            
            # Get ALL dialogue entries
            cursor.execute("""
                SELECT s.management_id, s.title, cdu.character_name, cdu.dialogue_text, 
                       cdu.filming_audio_instructions, cdu.row_number, cdu.id
                FROM character_dialogue_unified cdu
                JOIN scripts s ON cdu.script_id = s.id
                WHERE LENGTH(cdu.character_name) > 0 
                ORDER BY s.management_id, cdu.row_number
            """)
            
            all_entries = cursor.fetchall()
            
            validation_results = {
                'total_entries': len(all_entries),
                'correct_dialogue_structure': 0,
                'instruction_in_dialogue': 0,
                'empty_dialogue': 0,
                'correct_instruction_structure': 0,
                'dialogue_in_instruction': 0,
                'empty_instruction': 0,
                'problematic_scripts': set(),
                'sample_issues': []
            }
            
            self.log_message(f"📊 全{len(all_entries):,}件のエントリを検証中...")
            
            for i, (mgmt_id, title, char_name, dialogue, instructions, row_num, entry_id) in enumerate(all_entries):
                # Check dialogue column
                if dialogue and dialogue.strip():
                    if self.is_instruction_like(dialogue):
                        validation_results['instruction_in_dialogue'] += 1
                        validation_results['problematic_scripts'].add(mgmt_id)
                        
                        # Add to sample issues (first 10)
                        if len(validation_results['sample_issues']) < 10:
                            validation_results['sample_issues'].append({
                                'type': 'instruction_in_dialogue',
                                'script': mgmt_id,
                                'character': char_name,
                                'dialogue': dialogue[:100],
                                'instructions': instructions[:100] if instructions else ''
                            })
                    else:
                        validation_results['correct_dialogue_structure'] += 1
                else:
                    validation_results['empty_dialogue'] += 1
                    validation_results['problematic_scripts'].add(mgmt_id)
                
                # Check instruction column
                if instructions and instructions.strip():
                    if self.is_dialogue_like(instructions):
                        validation_results['dialogue_in_instruction'] += 1
                        validation_results['problematic_scripts'].add(mgmt_id)
                        
                        # Add to sample issues (first 10)
                        if len(validation_results['sample_issues']) < 10:
                            validation_results['sample_issues'].append({
                                'type': 'dialogue_in_instruction', 
                                'script': mgmt_id,
                                'character': char_name,
                                'dialogue': dialogue[:100] if dialogue else '',
                                'instructions': instructions[:100]
                            })
                    else:
                        validation_results['correct_instruction_structure'] += 1
                else:
                    validation_results['empty_instruction'] += 1
                
                # Progress update
                if i % 10000 == 0 and i > 0:
                    self.log_message(f"進行状況: {i:,}/{len(all_entries):,} ({i/len(all_entries)*100:.1f}%)")
            
            conn.close()
            
            # Calculate percentages
            total = validation_results['total_entries']
            correct_pct = validation_results['correct_dialogue_structure'] / total * 100 if total > 0 else 0
            issue_pct = validation_results['instruction_in_dialogue'] / total * 100 if total > 0 else 0
            
            # Report results
            self.log_message("=" * 80)
            self.log_message("📊 全データベース検証結果:")
            self.log_message(f"  総エントリ数: {total:,}件")
            self.log_message(f"  正しいセリフ構造: {validation_results['correct_dialogue_structure']:,}件 ({correct_pct:.1f}%)")
            self.log_message(f"  セリフ欄に指示文: {validation_results['instruction_in_dialogue']:,}件 ({issue_pct:.1f}%)")
            self.log_message(f"  空のセリフ: {validation_results['empty_dialogue']:,}件")
            self.log_message(f"  正しい指示構造: {validation_results['correct_instruction_structure']:,}件")
            self.log_message(f"  指示欄にセリフ: {validation_results['dialogue_in_instruction']:,}件")
            self.log_message(f"  空の指示: {validation_results['empty_instruction']:,}件")
            self.log_message(f"  問題のあるスクリプト数: {len(validation_results['problematic_scripts'])}件")
            
            # Show sample issues
            if validation_results['sample_issues']:
                self.log_message("\n🔍 問題の例:")
                for issue in validation_results['sample_issues']:
                    self.log_message(f"  {issue['type']} - {issue['script']} - {issue['character']}")
                    self.log_message(f"    dialogue: '{issue['dialogue']}'")
                    self.log_message(f"    instruction: '{issue['instructions']}'")
            
            self.log_message("=" * 80)
            
            # Overall assessment
            if issue_pct < 5:
                self.log_message("✅ 優秀: 95%以上が正しい構造です")
            elif issue_pct < 10:
                self.log_message("⚠️  注意: 一部修正が必要です")
            else:
                self.log_message("❌ 問題: 大幅な修正が必要です")
            
            return validation_results
            
        except Exception as e:
            self.log_message(f"❌ 検証エラー: {str(e)}")
            return {}
    
    def test_sample_searches(self):
        """Test sample searches to verify functionality"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            self.log_message("\n=== サンプル検索テスト ===")
            
            test_keywords = ['トミカ', 'サンサン', 'くもりん', 'ゲーム', '遊び']
            
            for keyword in test_keywords:
                # Test dialogue search
                cursor.execute("""
                    SELECT COUNT(*) FROM character_dialogue_unified cdu
                    JOIN scripts s ON cdu.script_id = s.id
                    WHERE LENGTH(cdu.character_name) > 0 AND LENGTH(cdu.dialogue_text) > 0 
                      AND (cdu.dialogue_text LIKE ? OR cdu.character_name LIKE ? OR s.title LIKE ?)
                """, (f'%{keyword}%', f'%{keyword}%', f'%{keyword}%'))
                
                dialogue_count = cursor.fetchone()[0]
                
                # Get sample results
                cursor.execute("""
                    SELECT s.title, cdu.character_name, cdu.dialogue_text 
                    FROM character_dialogue_unified cdu
                    JOIN scripts s ON cdu.script_id = s.id
                    WHERE LENGTH(cdu.character_name) > 0 AND LENGTH(cdu.dialogue_text) > 0 
                      AND (cdu.dialogue_text LIKE ? OR cdu.character_name LIKE ? OR s.title LIKE ?)
                    LIMIT 3
                """, (f'%{keyword}%', f'%{keyword}%', f'%{keyword}%'))
                
                samples = cursor.fetchall()
                
                self.log_message(f"🔍 '{keyword}' セリフ検索: {dialogue_count}件")
                
                for title, char, dialogue in samples:
                    self.log_message(f"  例: {char}: {dialogue[:50]}... ({title[:30]}...)")
            
            conn.close()
            
        except Exception as e:
            self.log_message(f"❌ 検索テストエラー: {str(e)}")

def main():
    """Main validation function"""
    db_path = "/tmp/updated_dropbox_db.db"
    
    validator = ComprehensiveValidator(db_path)
    
    print("=== 全データベース包括検証 ===")
    
    # Run comprehensive validation
    results = validator.validate_entire_database()
    
    # Run sample searches
    validator.test_sample_searches()
    
    print(f"\n検証完了。詳細は {validator.log_file} をご確認ください。")

if __name__ == "__main__":
    main()
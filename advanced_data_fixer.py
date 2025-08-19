#!/usr/bin/env python3
"""
Advanced Data Fixer

This script performs more comprehensive data fixing for remaining issues
after the initial column swap.
"""

import sqlite3
import re
from datetime import datetime

class AdvancedDataFixer:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/advanced_fix_log.txt"
        
        # More comprehensive instruction patterns
        self.instruction_patterns = [
            # Action instructions
            r'.*を見.*', r'.*する.*', r'.*から.*', r'.*へ.*', r'.*に.*', r'.*まで.*',
            r'.*登場.*', r'.*出現.*', r'.*消える.*', r'.*移動.*', r'.*歩く.*',
            r'.*踊り.*', r'.*踊る.*', r'.*ダンス.*', r'.*振り.*',
            
            # Technical instructions
            r'.*テロップ.*', r'.*アニメ.*', r'.*映像.*', r'.*カメラ.*',
            r'.*撮影.*', r'.*音声.*', r'.*編集.*', r'.*カット.*',
            r'.*ズーム.*', r'.*パン.*', r'.*フェード.*',
            
            # Position/direction
            r'.*左.*', r'.*右.*', r'.*中央.*', r'.*上.*', r'.*下.*',
            r'.*横.*', r'.*前.*', r'.*後.*', r'.*奥.*', r'.*手前.*',
            r'.*端.*', r'.*角.*', r'.*隅.*',
            
            # Description patterns
            r'^.*は.*$', r'^.*が.*される.*$', r'^.*の.*$',
            r'.*コスプレ.*', r'.*ケーキ.*', r'.*バナナ.*',
            
            # Multi-line instructions
            r'.*\n.*', r'.*改行.*'
        ]
        
        # Clear dialogue patterns
        self.clear_dialogue_patterns = [
            r'.*！$', r'.*？$', r'.*。$', r'.*～$', r'.*♪$', r'.*★$', r'.*☆$',
            r'.*だよ.*', r'.*ですね.*', r'.*だね.*', r'.*かな.*', r'.*よー.*', r'.*なー.*',
            r'みんな.*', r'.*ちゃん.*', r'.*くん.*', r'.*さん.*',
            r'はーい.*', r'やった.*', r'すごい.*', r'がんばれ.*', r'おめでとう.*',
            r'えー.*', r'うわー.*', r'きゃー.*', r'わー.*', r'おー.*'
        ]
    
    def log_message(self, message: str):
        """Log messages with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        print(log_entry.strip())
    
    def is_definitely_instruction(self, text: str) -> bool:
        """Check if text is definitely an instruction"""
        if not text or not text.strip():
            return False
        
        text = text.strip()
        
        # Check for definitive instruction patterns
        definitive_patterns = [
            r'.*テロップ.*', r'.*アニメ.*', r'.*カメラ.*', r'.*映像.*',
            r'.*撮影.*', r'.*編集.*', r'.*カット.*',
            r'.*登場.*', r'.*出現.*', r'.*コスプレ.*',
            r'.*を見上げる.*', r'.*を取る.*', r'.*から取る.*',
            r'^.*は揺れてる$', r'^.*に踊りながら.*登場$',
            r'.*端っこ表示.*', r'^テロップ.*', r'.*実はちょっと.*'
        ]
        
        for pattern in definitive_patterns:
            if re.match(pattern, text):
                return True
        
        # Check for multi-line content (usually instructions)
        if '\n' in text:
            return True
            
        return False
    
    def is_definitely_dialogue(self, text: str) -> bool:
        """Check if text is definitely dialogue"""
        if not text or not text.strip():
            return False
        
        text = text.strip()
        
        # Clear dialogue patterns
        for pattern in self.clear_dialogue_patterns:
            if re.match(pattern, text):
                return True
                
        # Single character exclamations/questions
        if len(text) <= 5 and any(ending in text for ending in ['！', '？', '。', '～']):
            return True
            
        return False
    
    def find_remaining_issues(self):
        """Find entries that still need fixing"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            self.log_message("=== 残り問題の特定 ===")
            
            # Find definite instruction text in dialogue column
            cursor.execute("""
                SELECT s.management_id, s.title, cdu.id, cdu.character_name, 
                       cdu.dialogue_text, cdu.filming_audio_instructions, cdu.row_number
                FROM character_dialogue_unified cdu
                JOIN scripts s ON cdu.script_id = s.id
                WHERE LENGTH(cdu.character_name) > 0 AND LENGTH(cdu.dialogue_text) > 0
            """)
            
            all_entries = cursor.fetchall()
            issues = []
            
            for mgmt_id, title, entry_id, char_name, dialogue, instructions, row_num in all_entries:
                if self.is_definitely_instruction(dialogue):
                    issue_type = 'clear_instruction_in_dialogue'
                    
                    # Determine fix action
                    if instructions and self.is_definitely_dialogue(instructions):
                        action = 'swap_columns'
                    elif not instructions or not instructions.strip():
                        action = 'move_to_instructions'
                    else:
                        action = 'manual_review'
                    
                    issues.append({
                        'id': entry_id,
                        'script': mgmt_id,
                        'character': char_name,
                        'row': row_num,
                        'dialogue': dialogue,
                        'instructions': instructions or '',
                        'issue_type': issue_type,
                        'action': action
                    })
            
            conn.close()
            
            # Group by action type
            action_counts = {}
            for issue in issues:
                action = issue['action']
                action_counts[action] = action_counts.get(action, 0) + 1
            
            self.log_message(f"📊 残り問題分析:")
            self.log_message(f"  総問題数: {len(issues)}件")
            for action, count in action_counts.items():
                self.log_message(f"  {action}: {count}件")
            
            return issues
            
        except Exception as e:
            self.log_message(f"❌ 問題特定エラー: {str(e)}")
            return []
    
    def apply_advanced_fixes(self, issues, dry_run=True):
        """Apply advanced fixes"""
        if dry_run:
            self.log_message("🔄 ADVANCED DRY RUN モード")
        else:
            self.log_message("⚠️  実際に高度修正を実行します")
        
        results = {
            'swapped': 0,
            'moved': 0,
            'manual': 0,
            'errors': 0
        }
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for i, issue in enumerate(issues):
                try:
                    if issue['action'] == 'swap_columns':
                        if not dry_run:
                            cursor.execute("""
                                UPDATE character_dialogue_unified 
                                SET dialogue_text = ?, filming_audio_instructions = ?
                                WHERE id = ?
                            """, (issue['instructions'], issue['dialogue'], issue['id']))
                        results['swapped'] += 1
                        
                    elif issue['action'] == 'move_to_instructions':
                        if not dry_run:
                            cursor.execute("""
                                UPDATE character_dialogue_unified 
                                SET dialogue_text = '', filming_audio_instructions = ?
                                WHERE id = ?
                            """, (issue['dialogue'], issue['id']))
                        results['moved'] += 1
                        
                    else:
                        results['manual'] += 1
                    
                    # Log progress
                    if i < 10 or i % 1000 == 0:
                        self.log_message(f"🔧 {issue['script']}:{issue['row']} - {issue['action']}")
                        
                except Exception as e:
                    results['errors'] += 1
                    self.log_message(f"❌ 修正エラー {issue['script']}: {str(e)}")
            
            if not dry_run:
                conn.commit()
                self.log_message("💾 高度修正をコミットしました")
            
            conn.close()
            
        except Exception as e:
            self.log_message(f"❌ 高度修正エラー: {str(e)}")
            results['errors'] += 1
        
        return results
    
    def run_advanced_fixing(self, dry_run=True):
        """Run advanced fixing process"""
        self.log_message("=" * 80)
        self.log_message("高度データ修正プロセス開始")
        self.log_message("=" * 80)
        
        # Find remaining issues
        issues = self.find_remaining_issues()
        
        if not issues:
            self.log_message("✅ 修正が必要な問題は見つかりませんでした")
            return
        
        # Apply fixes
        results = self.apply_advanced_fixes(issues, dry_run=dry_run)
        
        # Report results
        self.log_message("=" * 80)
        self.log_message(f"高度修正結果 ({'DRY RUN' if dry_run else 'ACTUAL RUN'}):")
        self.log_message(f"  列交換: {results['swapped']}件")
        self.log_message(f"  指示文移動: {results['moved']}件") 
        self.log_message(f"  手動確認必要: {results['manual']}件")
        self.log_message(f"  エラー: {results['errors']}件")
        self.log_message("=" * 80)
        
        if dry_run and (results['swapped'] + results['moved']) > 0:
            self.log_message("💡 実際の修正を実行するには dry_run=False で再実行してください")
        elif not dry_run:
            self.log_message("✅ 高度修正完了！")
        
        return results

def main():
    """Main execution function"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    fixer = AdvancedDataFixer(db_path)
    
    print("=== 高度データ修正ツール ===")
    print("DRY RUN で分析を実行します...")
    
    # Run dry run first
    results = fixer.run_advanced_fixing(dry_run=True)
    
    if results and (results.get('swapped', 0) + results.get('moved', 0)) > 0:
        total_fixes = results.get('swapped', 0) + results.get('moved', 0)
        print(f"\n🎯 {total_fixes}件の追加修正が可能です。")
        
        # Automatically proceed with actual fixes
        print("実際の修正を実行します...")
        actual_results = fixer.run_advanced_fixing(dry_run=False)
        
        if actual_results:
            total_actual = actual_results.get('swapped', 0) + actual_results.get('moved', 0)
            print(f"\n✅ {total_actual}件の追加修正が完了しました！")

if __name__ == "__main__":
    main()
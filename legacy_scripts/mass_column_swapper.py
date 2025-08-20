#!/usr/bin/env python3
"""
Mass Column Swapper

This script performs mass column swapping for the dialogue and instruction columns
when they are reversed in the database.
"""

import sqlite3
import os
import re
from datetime import datetime

class MassColumnSwapper:
    def __init__(self, db_path):
        self.db_path = db_path
        self.log_file = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/mass_swap_log.txt"
        
        # Instruction patterns
        self.instruction_patterns = [
            r'.*を見.*', r'.*する.*', r'.*から.*', r'.*へ.*', r'.*に.*',
            r'.*カメラ.*', r'.*映像.*', r'.*テロップ.*', r'.*アニメ.*',
            r'.*撮影.*', r'.*音声.*', r'.*編集.*',
            r'.*タッチ.*', r'.*近づく.*', r'.*見渡す.*', r'.*対峙.*'
        ]
        
        # Dialogue patterns
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
    
    def analyze_swap_candidates(self) -> list:
        """Find entries that need column swapping"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Find entries where dialogue_text looks like instruction 
            # AND filming_audio_instructions looks like dialogue
            cursor.execute("""
                SELECT id, script_id, character_name, dialogue_text, filming_audio_instructions, row_number
                FROM character_dialogue_unified
                WHERE LENGTH(character_name) > 0 
                  AND LENGTH(dialogue_text) > 0
                  AND LENGTH(filming_audio_instructions) > 0
            """)
            
            all_entries = cursor.fetchall()
            swap_candidates = []
            
            for entry_id, script_id, char_name, dialogue, instructions, row_num in all_entries:
                dialogue_is_instruction = self.is_instruction_like(dialogue)
                instructions_is_dialogue = self.is_dialogue_like(instructions)
                
                if dialogue_is_instruction and instructions_is_dialogue:
                    # Get script info for logging
                    cursor.execute("SELECT management_id, title FROM scripts WHERE id = ?", (script_id,))
                    script_info = cursor.fetchone()
                    
                    swap_candidates.append({
                        'id': entry_id,
                        'script_id': script_id,
                        'management_id': script_info[0] if script_info else 'Unknown',
                        'title': script_info[1] if script_info else 'Unknown',
                        'character': char_name,
                        'row_number': row_num,
                        'current_dialogue': dialogue,
                        'current_instructions': instructions
                    })
            
            conn.close()
            
            self.log_message(f"🔍 列交換候補: {len(swap_candidates)}件発見")
            return swap_candidates
            
        except Exception as e:
            self.log_message(f"❌ 分析エラー: {str(e)}")
            return []
    
    def perform_mass_swap(self, candidates: list, dry_run: bool = True) -> dict:
        """Perform mass column swapping"""
        results = {
            'success': 0,
            'errors': 0,
            'skipped': 0
        }
        
        if dry_run:
            self.log_message("🔄 DRY RUN モード - 実際の変更は行いません")
        else:
            self.log_message("⚠️  実際に列交換を実行します")
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for i, candidate in enumerate(candidates):
                try:
                    if not dry_run:
                        cursor.execute("""
                            UPDATE character_dialogue_unified 
                            SET dialogue_text = ?, filming_audio_instructions = ?
                            WHERE id = ?
                        """, (
                            candidate['current_instructions'],  # Move instructions to dialogue
                            candidate['current_dialogue'],     # Move dialogue to instructions  
                            candidate['id']
                        ))
                    
                    results['success'] += 1
                    
                    if i < 10 or i % 100 == 0:  # Log first 10 and every 100th
                        self.log_message(
                            f"🔄 {candidate['management_id']}:{candidate['row_number']} "
                            f"{candidate['character']} - 列交換"
                        )
                    
                except Exception as e:
                    results['errors'] += 1
                    self.log_message(
                        f"❌ エラー {candidate['management_id']}: {str(e)}"
                    )
            
            if not dry_run:
                conn.commit()
                self.log_message("💾 データベース変更をコミットしました")
                
            conn.close()
            
        except Exception as e:
            self.log_message(f"❌ 大量交換エラー: {str(e)}")
            results['errors'] += 1
        
        return results
    
    def run_mass_swap_process(self, dry_run: bool = True):
        """Run the complete mass swap process"""
        self.log_message("=" * 80)
        self.log_message("大量列交換プロセス開始")
        self.log_message("=" * 80)
        
        # Step 1: Analyze candidates
        candidates = self.analyze_swap_candidates()
        
        if not candidates:
            self.log_message("✅ 交換が必要な列は見つかりませんでした")
            return
        
        # Show sample of what will be swapped
        self.log_message("\n📋 交換予定のサンプル (最初の5件):")
        for i, candidate in enumerate(candidates[:5]):
            self.log_message(
                f"  {candidate['management_id']} - {candidate['character']}\n"
                f"    現在のdialogue_text: '{candidate['current_dialogue'][:50]}...'\n"
                f"    現在のinstructions: '{candidate['current_instructions'][:50]}...'\n"
                f"    → これらを交換します\n"
            )
        
        # Step 2: Perform swap
        results = self.perform_mass_swap(candidates, dry_run=dry_run)
        
        # Step 3: Report results
        self.log_message("=" * 80)
        self.log_message(f"大量列交換結果 ({'DRY RUN' if dry_run else 'ACTUAL RUN'}):")
        self.log_message(f"  成功: {results['success']}件")
        self.log_message(f"  エラー: {results['errors']}件")
        self.log_message(f"  スキップ: {results['skipped']}件")
        self.log_message("=" * 80)
        
        if dry_run and results['success'] > 0:
            self.log_message("💡 実際の交換を実行するには dry_run=False で再実行してください")
        elif not dry_run and results['success'] > 0:
            self.log_message("✅ 大量列交換完了！")
        
        return results

def main():
    """Main execution function"""
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found at: {db_path}")
        return
    
    swapper = MassColumnSwapper(db_path)
    
    print("=== 大量列交換ツール ===")
    print("DRY RUN で分析を実行します...")
    
    # Run dry run first
    results = swapper.run_mass_swap_process(dry_run=True)
    
    if results and results.get('success', 0) > 0 and results.get('errors', 0) == 0:
        print(f"\n🎯 {results['success']}件の列交換が可能です。")
        
        # Automatically proceed with actual swap
        print("実際の交換を実行します...")
        actual_results = swapper.run_mass_swap_process(dry_run=False)
        
        if actual_results and actual_results.get('success', 0) > 0:
            print(f"\n✅ {actual_results['success']}件の列交換が完了しました！")

if __name__ == "__main__":
    main()
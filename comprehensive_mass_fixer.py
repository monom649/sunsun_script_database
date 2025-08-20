#!/usr/bin/env python3
"""
Comprehensive Mass Fixer

This script performs comprehensive mass fixing for all problematic dialogue data
by running multiple fixing approaches in sequence.
"""

import sqlite3
import subprocess
import time
from datetime import datetime

def log_message(message: str, log_file: str = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/comprehensive_fix_log.txt"):
    """Log messages with timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}\n"
    
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(log_entry)
    
    print(log_entry.strip())

def get_database_stats(db_path: str) -> dict:
    """Get current database statistics"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Total entries
        cursor.execute('SELECT COUNT(*) FROM character_dialogue_unified')
        total = cursor.fetchone()[0]
        
        # Empty dialogue entries
        cursor.execute('SELECT COUNT(*) FROM character_dialogue_unified WHERE LENGTH(character_name) > 0 AND (dialogue_text IS NULL OR dialogue_text = "")')
        empty_dialogue = cursor.fetchone()[0]
        
        # Problematic scripts
        cursor.execute('''
            SELECT COUNT(DISTINCT s.management_id)
            FROM scripts s
            JOIN character_dialogue_unified cdu ON s.id = cdu.script_id
            WHERE LENGTH(cdu.character_name) > 0 AND (cdu.dialogue_text IS NULL OR cdu.dialogue_text = "")
        ''')
        problematic_scripts = cursor.fetchone()[0]
        
        # Tomica dialogue count
        cursor.execute('SELECT COUNT(*) FROM character_dialogue_unified WHERE dialogue_text LIKE "%トミカ%"')
        tomica_count = cursor.fetchone()[0]
        
        # Correct dialogue structure
        cursor.execute('SELECT COUNT(*) FROM character_dialogue_unified WHERE LENGTH(character_name) > 0 AND LENGTH(dialogue_text) > 0')
        correct_dialogues = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'total': total,
            'empty_dialogue': empty_dialogue,
            'problematic_scripts': problematic_scripts,
            'tomica_count': tomica_count,
            'correct_dialogues': correct_dialogues
        }
        
    except Exception as e:
        log_message(f"❌ Database stats error: {str(e)}")
        return {}

def run_mass_dialogue_fixer(max_batches: int = 10, batch_size: int = 100):
    """Run mass dialogue fixer in multiple batches"""
    
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    log_message("=" * 80)
    log_message("包括的大量修正プロセス開始")
    log_message("=" * 80)
    
    # Initial stats
    initial_stats = get_database_stats(db_path)
    log_message(f"初期状態:")
    log_message(f"  総エントリ数: {initial_stats.get('total', 0):,}件")
    log_message(f"  空のセリフエントリ: {initial_stats.get('empty_dialogue', 0):,}件")
    log_message(f"  問題のあるスクリプト数: {initial_stats.get('problematic_scripts', 0)}件")
    log_message(f"  正しいセリフ構造: {initial_stats.get('correct_dialogues', 0):,}件")
    
    total_fixed = 0
    
    # Run multiple batches of fixing
    for batch in range(max_batches):
        log_message(f"\\n🔧 バッチ {batch + 1}/{max_batches} 実行中...")
        
        try:
            # Run missing dialogue fixer
            from missing_dialogue_fixer import MissingDialogueFixer
            
            fixer = MissingDialogueFixer(db_path)
            batch_fixed = fixer.run_mass_fix(max_scripts=batch_size)
            total_fixed += batch_fixed
            
            log_message(f"✅ バッチ {batch + 1} 完了: {batch_fixed}件修正")
            
            # Check current stats
            current_stats = get_database_stats(db_path)
            remaining_problems = current_stats.get('problematic_scripts', 0)
            
            log_message(f"  残り問題スクリプト: {remaining_problems}件")
            
            if remaining_problems == 0:
                log_message("🎉 全ての問題スクリプトが修正されました！")
                break
            elif batch_fixed == 0:
                log_message("⚠️  これ以上修正できるスクリプトがありません")
                break
                
            # Rate limiting between batches
            if batch < max_batches - 1:
                time.sleep(2)
                
        except Exception as e:
            log_message(f"❌ バッチ {batch + 1} エラー: {str(e)}")
            continue
    
    # Final stats
    final_stats = get_database_stats(db_path)
    
    log_message("=" * 80)
    log_message("包括的修正結果:")
    log_message(f"  修正したスクリプト総数: {total_fixed}件")
    log_message(f"  最終状態:")
    log_message(f"    総エントリ数: {final_stats.get('total', 0):,}件 (変化: +{final_stats.get('total', 0) - initial_stats.get('total', 0):,})")
    log_message(f"    空のセリフエントリ: {final_stats.get('empty_dialogue', 0):,}件 (修正: -{initial_stats.get('empty_dialogue', 0) - final_stats.get('empty_dialogue', 0):,})")
    log_message(f"    問題のあるスクリプト数: {final_stats.get('problematic_scripts', 0)}件 (修正: -{initial_stats.get('problematic_scripts', 0) - final_stats.get('problematic_scripts', 0)})")
    log_message(f"    正しいセリフ構造: {final_stats.get('correct_dialogues', 0):,}件 (増加: +{final_stats.get('correct_dialogues', 0) - initial_stats.get('correct_dialogues', 0):,})")
    log_message(f"    トミカセリフ数: {final_stats.get('tomica_count', 0)}件")
    log_message("=" * 80)
    
    if final_stats.get('problematic_scripts', 0) == 0:
        log_message("🎉 全ての問題が解決されました！")
        return True
    else:
        log_message(f"⚠️  {final_stats.get('problematic_scripts', 0)}件のスクリプトがまだ修正されていません")
        return False

def main():
    """Main execution function"""
    success = run_mass_dialogue_fixer(max_batches=15, batch_size=100)
    
    if success:
        print("\\n✅ 包括的修正が完全に完了しました！")
    else:
        print("\\n⚠️  一部のスクリプトがまだ修正されていません")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
誤った指示フラグを一旦リセットして再解析準備
"""

import sqlite3

def revert_instruction_flags():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🔄 誤った指示フラグの一旦リセット開始")
    print("=" * 80)
    
    # 現在の状況確認
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE is_instruction = 1")
    current_instruction_count = cursor.fetchone()[0]
    
    print(f"📊 リセット前の指示データ: {current_instruction_count}件")
    
    # 明らかに正しい指示データのみ残す
    print("✅ 確実な指示データのみ保持...")
    
    cursor.execute("""
        UPDATE character_dialogue_unified 
        SET is_instruction = 0
    """)
    
    # 確実に指示と判断できるもののみフラグ設定
    certain_instructions = [
        '[撮影指示]', '[話者不明]', 'アイキャッチ', 'CM', '5秒CM',
        'BGM', '効果音', 'ゲーム音声', '[定型フレーズ]'
    ]
    
    total_flagged = 0
    for char_pattern in certain_instructions:
        cursor.execute("""
            UPDATE character_dialogue_unified 
            SET is_instruction = 1 
            WHERE character_name = ?
        """, (char_pattern,))
        
        flagged = cursor.rowcount
        total_flagged += flagged
        print(f"  ✅ {char_pattern}: {flagged}件")
    
    # 最終確認
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE is_instruction = 1")
    final_instruction_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE is_instruction = 0")
    final_dialogue_count = cursor.fetchone()[0]
    
    print()
    print("=" * 80)
    print(f"📊 リセット結果:")
    print(f"  確実な指示データ: {final_instruction_count}件")
    print(f"  セリフ・疑わしいデータ: {final_dialogue_count}件")
    print(f"  リセットにより復元: {current_instruction_count - final_instruction_count}件")
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ 指示フラグのリセット完了")
    print(f"💡 スプレッドシート構造の再解析が必要です")
    
    return final_instruction_count, final_dialogue_count

if __name__ == "__main__":
    revert_instruction_flags()
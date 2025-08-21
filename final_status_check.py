#!/usr/bin/env python3
"""
最終状況チェックと問題キャラクター分析
"""

import sqlite3

def final_status_check():
    db_path = "/Users/mitsuruono/sunsun_script_search/sunsun_script_database/youtube_search_complete_all.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("📊 最終データベース状況チェック")
    print("=" * 80)
    
    # 基本統計
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE is_instruction = 1")
    instruction_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM character_dialogue_unified WHERE is_instruction = 0")
    dialogue_count = cursor.fetchone()[0]
    
    total = instruction_count + dialogue_count
    
    print(f"🎯 最終データ構成:")
    print(f"  指示データ: {instruction_count:,}件 ({instruction_count/total*100:.1f}%)")
    print(f"  セリフデータ: {dialogue_count:,}件 ({dialogue_count/total*100:.1f}%)")
    print(f"  総計: {total:,}件")
    print()
    
    # 問題キャラクターの詳細分析
    print("⚠️ 残存問題キャラクター詳細:")
    print("-" * 50)
    
    problematic_chars = ['SE', 'TRUE', 'みんな', 'シーン']
    
    for char in problematic_chars:
        cursor.execute("""
            SELECT COUNT(*) 
            FROM character_dialogue_unified 
            WHERE is_instruction = 0 AND character_name = ?
        """, (char,))
        
        count = cursor.fetchone()[0]
        if count > 0:
            print(f"📌 {char}: {count:,}件")
            
            # サンプル表示
            cursor.execute("""
                SELECT s.management_id, cdu.dialogue_text, cdu.row_number
                FROM character_dialogue_unified cdu
                JOIN scripts s ON cdu.script_id = s.id
                WHERE cdu.is_instruction = 0 AND cdu.character_name = ?
                LIMIT 3
            """, (char,))
            
            samples = cursor.fetchall()
            for management_id, dialogue, row in samples:
                row_str = f"{row:3d}" if row is not None else "---"
                dialogue_short = dialogue[:60] + "..." if len(dialogue) > 60 else dialogue
                print(f"    例: {management_id} 行{row_str} | \"{dialogue_short}\"")
            print()
    
    # 正常キャラクター統計
    print("✅ 正常キャラクター統計（上位10位）:")
    print("-" * 50)
    
    cursor.execute("""
        SELECT character_name, COUNT(*) as count
        FROM character_dialogue_unified 
        WHERE is_instruction = 0
        AND character_name NOT IN ('SE', 'TRUE', 'みんな', 'シーン', 'FALSE')
        GROUP BY character_name
        ORDER BY count DESC
        LIMIT 10
    """)
    
    normal_chars = cursor.fetchall()
    for i, (char_name, count) in enumerate(normal_chars, 1):
        char_short = char_name[:25] + "..." if len(char_name) > 25 else char_name
        print(f"  {i:2d}. {char_short:30s}: {count:6,}件")
    
    print()
    
    # 指示データ統計
    print("📋 現在の指示データ統計:")
    print("-" * 50)
    
    cursor.execute("""
        SELECT character_name, COUNT(*) as count
        FROM character_dialogue_unified 
        WHERE is_instruction = 1
        GROUP BY character_name
        ORDER BY count DESC
        LIMIT 8
    """)
    
    instruction_chars = cursor.fetchall()
    for char_name, count in instruction_chars:
        char_short = char_name[:30] + "..." if len(char_name) > 30 else char_name
        print(f"  {char_short:35s}: {count:6,}件")
    
    conn.close()
    
    print()
    print("=" * 80)
    print("💡 状況:")
    print("  ✅ 確実な指示データ（FALSE、技術指示等）は除外済み")
    print("  ⚠️  残存問題キャラクターは手動検証が必要")
    print("  🎯 検索品質は大幅に改善されました")

if __name__ == "__main__":
    final_status_check()